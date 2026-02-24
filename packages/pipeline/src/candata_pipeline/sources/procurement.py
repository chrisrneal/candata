"""
sources/procurement.py — Federal proactive disclosure procurement source.

Pulls awarded contracts and open tenders from:
  1. open.canada.ca proactive disclosure CSV (contracts) via CKAN API
  2. CanadaBuys API (active/recent tenders)

Contract CSV dataset (CKAN):
  GET https://open.canada.ca/data/api/3/action/package_show?id=d8f85d91-7dec-4fd1-8f59-35571b88e4d1

Contract CSV columns (actual proactive disclosure format):
  reference_number, procurement_id, vendor_name, vendor_postal_code,
  buyer_name, contract_date, economic_object_code, description_en,
  contract_period_start, delivery_date, original_value, final_value,
  comments_en, additional_comments_en, amendment_value, agreement_type_code

CanadaBuys tender API:
  https://canadabuys.canada.ca/en/tender-opportunities/api/v1/notices

Usage:
    source = ProcurementSource()
    contracts_df = await source.extract(dataset="contracts")
    tenders_df   = await source.extract(dataset="tenders")
"""

from __future__ import annotations

import io
import json
import re
from typing import Any, Literal

import httpx
import polars as pl
import structlog

from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

Dataset = Literal["contracts", "tenders"]

# CKAN dataset ID for proactive disclosure of contracts
_CKAN_DATASET_ID = "d8f85d91-7dec-4fd1-8f59-35571b88e4d1"
_CKAN_API_URL = "https://open.canada.ca/data/api/3/action/package_show"

# Fallback direct download URL if CKAN API fails
_PROACTIVE_CSV_URL = (
    "https://open.canada.ca/data/en/datastore/dump/d8f85d91-7dec-4fd1-8055-483b77225d8b"
    "?bom=True&format=csv"
)

_CANADABUYS_TENDERS_URL = (
    "https://canadabuys.canada.ca/en/tender-opportunities/api/v1/notices"
)

# Department name normalization for common misspellings / abbreviations
_DEPT_NORMALIZE: dict[str, str] = {
    "national defence": "National Defence",
    "national defense": "National Defence",
    "dnd": "National Defence",
    "public works and government services canada": "Public Services and Procurement Canada",
    "public works": "Public Services and Procurement Canada",
    "pwgsc": "Public Services and Procurement Canada",
    "pspc": "Public Services and Procurement Canada",
    "health canada": "Health Canada",
    "hc": "Health Canada",
    "transport canada": "Transport Canada",
    "tc": "Transport Canada",
    "rcmp": "Royal Canadian Mounted Police",
    "cra": "Canada Revenue Agency",
    "canada revenue agency": "Canada Revenue Agency",
    "ircc": "Immigration, Refugees and Citizenship Canada",
    "employment and social development canada": "Employment and Social Development Canada",
    "esdc": "Employment and Social Development Canada",
    "global affairs canada": "Global Affairs Canada",
    "gac": "Global Affairs Canada",
    "department of foreign affairs and international trade": "Global Affairs Canada",
    "dfait": "Global Affairs Canada",
    "treasury board of canada secretariat": "Treasury Board of Canada Secretariat",
    "tbs": "Treasury Board of Canada Secretariat",
    "fisheries and oceans canada": "Fisheries and Oceans Canada",
    "dfo": "Fisheries and Oceans Canada",
    "environment and climate change canada": "Environment and Climate Change Canada",
    "eccc": "Environment and Climate Change Canada",
    "natural resources canada": "Natural Resources Canada",
    "nrcan": "Natural Resources Canada",
    "innovation, science and economic development canada": "Innovation, Science and Economic Development Canada",
    "ised": "Innovation, Science and Economic Development Canada",
    "public safety canada": "Public Safety Canada",
    "psc": "Public Safety Canada",
    "indigenous services canada": "Indigenous Services Canada",
    "isc": "Indigenous Services Canada",
    "crown-indigenous relations and northern affairs canada": "Crown-Indigenous Relations and Northern Affairs Canada",
    "cirnac": "Crown-Indigenous Relations and Northern Affairs Canada",
}

# Economic object code → category mapping (top-level groupings)
_ECON_OBJ_CATEGORY: dict[str, str] = {
    "0": "Personnel",
    "1": "Transportation and Communications",
    "2": "Information",
    "3": "Professional and Special Services",
    "4": "Rentals",
    "5": "Repair and Maintenance",
    "6": "Utilities, Materials and Supplies",
    "7": "Acquisition of Land, Buildings and Works",
    "8": "Acquisition of Machinery and Equipment",
    "9": "Transfer Payments",
    "12": "Other Subsidies and Payments",
}


def normalize_vendor(name: str | None) -> str | None:
    """Normalize vendor name: trim whitespace, title case."""
    if not name or not name.strip():
        return None
    cleaned = re.sub(r"\s+", " ", name.strip())
    return cleaned.title()


def normalize_department(name: str | None) -> str | None:
    """Normalize department name using known variants."""
    if not name:
        return None
    stripped = name.strip()
    return _DEPT_NORMALIZE.get(stripped.lower(), stripped)


def categorize_economic_object(code: str | None) -> str | None:
    """Map an economic object code to a category label."""
    if not code or not code.strip():
        return None
    prefix = code.strip()[:1]
    if prefix == "1" and len(code.strip()) >= 2 and code.strip()[:2] == "12":
        return _ECON_OBJ_CATEGORY.get("12")
    return _ECON_OBJ_CATEGORY.get(prefix)


class ProcurementSource(BaseSource):
    """Pulls federal procurement data from open.canada.ca and CanadaBuys."""

    name = "CanadaBuys"

    def __init__(self, timeout: float = 120.0) -> None:
        super().__init__()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # HTTP — CKAN API
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _fetch_ckan_metadata(self) -> dict[str, Any]:
        """Fetch dataset metadata from the CKAN API."""
        self._log.info("ckan_fetch", dataset_id=_CKAN_DATASET_ID)
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            r = await client.get(
                _CKAN_API_URL, params={"id": _CKAN_DATASET_ID}
            )
            r.raise_for_status()
            payload = r.json()
            if not payload.get("success"):
                raise httpx.HTTPError(f"CKAN API error: {payload}")
            return payload["result"]

    def _extract_csv_urls(self, ckan_result: dict[str, Any]) -> list[str]:
        """Extract CSV resource URLs from CKAN package metadata."""
        urls: list[str] = []
        for resource in ckan_result.get("resources", []):
            fmt = (resource.get("format") or "").upper()
            url = resource.get("url", "")
            if fmt == "CSV" and url:
                urls.append(url)
        return urls

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_csv(self, url: str) -> bytes:
        """Download a single CSV file."""
        self._log.info("csv_download", url=url[:120])
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.content

    @with_retry(max_attempts=3, base_delay=1.0, retry_on=(httpx.HTTPError,))
    async def _fetch_tenders_page(
        self, page: int = 1, per_page: int = 100
    ) -> dict[str, Any]:
        """Fetch a page of active tenders from the CanadaBuys API."""
        params = {
            "status": "active",
            "page": page,
            "per_page": per_page,
            "format": "json",
        }
        self._log.debug("tenders_fetch", page=page)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.get(_CANADABUYS_TENDERS_URL, params=params)
            r.raise_for_status()
            return r.json()

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        dataset: Dataset = "contracts",
        max_tenders: int = 500,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download procurement data.

        For contracts: queries CKAN API for CSV resource URLs, downloads and
        concatenates all CSVs. Falls back to direct URL if CKAN fails.

        For tenders: paginates the CanadaBuys API.
        """
        if dataset == "contracts":
            return await self._extract_contracts()
        return await self._extract_tenders(max_tenders)

    async def _extract_contracts(self) -> pl.DataFrame:
        """Download contract CSVs via CKAN API discovery."""
        try:
            metadata = await self._fetch_ckan_metadata()
            csv_urls = self._extract_csv_urls(metadata)
        except Exception as exc:
            self._log.warning(
                "ckan_fallback",
                error=str(exc),
                msg="Falling back to direct CSV URL",
            )
            csv_urls = [_PROACTIVE_CSV_URL]

        if not csv_urls:
            csv_urls = [_PROACTIVE_CSV_URL]

        dfs: list[pl.DataFrame] = []
        for url in csv_urls:
            try:
                raw_bytes = await self._download_csv(url)
                # Handle UTF-8 BOM
                if raw_bytes.startswith(b"\xef\xbb\xbf"):
                    raw_bytes = raw_bytes[3:]
                df = pl.read_csv(
                    io.BytesIO(raw_bytes),
                    infer_schema_length=0,
                    truncate_ragged_lines=True,
                    encoding="utf8-lossy",
                )
                if not df.is_empty():
                    dfs.append(df)
                    self._log.info("csv_loaded", url=url[:80], rows=len(df))
            except Exception as exc:
                self._log.warning("csv_failed", url=url[:80], error=str(exc))

        if not dfs:
            return pl.DataFrame()
        return pl.concat(dfs, how="diagonal_relaxed")

    async def _extract_tenders(self, max_tenders: int) -> pl.DataFrame:
        """Paginate the CanadaBuys tender API."""
        all_notices: list[dict[str, Any]] = []
        page = 1
        while len(all_notices) < max_tenders:
            try:
                payload = await self._fetch_tenders_page(page)
            except Exception as exc:
                self._log.warning(
                    "tenders_page_failed", page=page, error=str(exc)
                )
                break
            notices = payload.get("data", payload.get("notices", []))
            if not notices:
                break
            all_notices.extend(notices)
            if len(notices) < 100:
                break
            page += 1

        if not all_notices:
            return pl.DataFrame()
        return pl.from_dicts(all_notices)

    def transform(
        self, raw: pl.DataFrame, *, dataset: Dataset = "contracts"
    ) -> pl.DataFrame:
        """
        Normalize procurement data to contracts or tenders schema.

        Output for contracts:
            contract_number, vendor_name, department, category,
            description, contract_value, original_value, amendment_value,
            start_date, end_date, award_date, source_url, raw_data

        Output for tenders:
            tender_number, title, department, category, region,
            closing_date, status, estimated_value, source_url
        """
        if raw.is_empty():
            return raw

        df = self._normalize_columns(raw)

        if dataset == "contracts":
            return self._transform_contracts(df)
        return self._transform_tenders(df)

    def _transform_contracts(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map proactive disclosure CSV columns to contracts table schema."""

        def pick(candidates: list[str]) -> str | None:
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        col_ref = pick(["reference_number", "contract_number", "procurement_id"])
        col_vendor = pick(["vendor_name", "vendor", "supplier_name"])
        col_dept = pick(["buyer_name", "department", "organization", "owner_org"])
        col_desc = pick(["description_en", "description", "desc"])
        col_value = pick(["final_value", "contract_value", "value"])
        col_orig = pick(["original_value"])
        col_amend_val = pick(["amendment_value"])
        col_date = pick(["contract_date", "award_date", "date"])
        col_start = pick(["contract_period_start", "start_date"])
        col_end = pick(["delivery_date", "end_date"])
        col_econ = pick(["economic_object_code"])

        # Serialize each row as JSON string for raw_data
        raw_json = [json.dumps(row, default=str) for row in df.to_dicts()]

        exprs: list[pl.Expr] = []
        col_mapping: dict[str, str] = {}

        if col_ref:
            exprs.append(pl.col(col_ref).alias("contract_number"))
            col_mapping["contract_number"] = col_ref
        if col_vendor:
            exprs.append(
                pl.col(col_vendor)
                .map_elements(normalize_vendor, return_dtype=pl.String)
                .alias("vendor_name")
            )
            col_mapping["vendor_name"] = col_vendor
        if col_dept:
            exprs.append(
                pl.col(col_dept)
                .map_elements(normalize_department, return_dtype=pl.String)
                .alias("department")
            )
            col_mapping["department"] = col_dept
        if col_desc:
            exprs.append(pl.col(col_desc).alias("description"))
            col_mapping["description"] = col_desc
        if col_value:
            exprs.append(
                pl.col(col_value).cast(pl.Float64, strict=False).alias("contract_value")
            )
            col_mapping["contract_value"] = col_value
        if col_orig:
            exprs.append(
                pl.col(col_orig).cast(pl.Float64, strict=False).alias("original_value")
            )
        if col_amend_val:
            exprs.append(
                pl.col(col_amend_val)
                .cast(pl.Float64, strict=False)
                .alias("amendment_value")
            )
        if col_date:
            exprs.append(
                pl.col(col_date).str.to_date(strict=False).alias("award_date")
            )
            col_mapping["award_date"] = col_date
        if col_start:
            exprs.append(
                pl.col(col_start).str.to_date(strict=False).alias("start_date")
            )
        if col_end:
            exprs.append(
                pl.col(col_end).str.to_date(strict=False).alias("end_date")
            )
        if col_econ:
            exprs.append(
                pl.col(col_econ)
                .map_elements(categorize_economic_object, return_dtype=pl.String)
                .alias("category")
            )

        if not exprs:
            return df

        result = df.with_columns(exprs)

        # Select only the output columns
        output_cols = [e.meta.output_name() for e in exprs]
        result = result.select([c for c in output_cols if c in result.columns])

        # Add raw_data
        result = result.with_columns(
            pl.Series("raw_data", raw_json, dtype=pl.String)
        )

        return result

    def _transform_tenders(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map CanadaBuys API fields to tenders table schema."""
        col_map = {
            "tender_number": ["reference_number", "tender_number", "notice_id"],
            "title": ["title", "title_en", "subject"],
            "department": ["department", "buyer_name", "organization"],
            "closing_date": ["closing_date", "close_date", "closing"],
            "status": ["status", "notice_status"],
            "estimated_value": [
                "estimated_value",
                "budget",
                "contract_value",
            ],
            "category": ["category", "commodity", "gsin"],
            "region": ["region", "delivery_region"],
            "source_url": ["url", "source_url", "link"],
        }

        def pick(candidates: list[str]) -> str | None:
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        exprs: list[pl.Expr] = []
        for out_col, candidates in col_map.items():
            src = pick(candidates)
            if src:
                if out_col == "closing_date":
                    exprs.append(
                        pl.col(src).str.to_date(strict=False).alias(out_col)
                    )
                elif out_col == "estimated_value":
                    exprs.append(
                        pl.col(src).cast(pl.Float64, strict=False).alias(out_col)
                    )
                elif out_col == "department":
                    exprs.append(
                        pl.col(src)
                        .map_elements(normalize_department, return_dtype=pl.String)
                        .alias(out_col)
                    )
                else:
                    exprs.append(pl.col(src).alias(out_col))

        if not exprs:
            return df

        result = df.with_columns(exprs)
        output_cols = [e.meta.output_name() for e in exprs]
        return result.select([c for c in output_cols if c in result.columns])

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "ckan_dataset_id": _CKAN_DATASET_ID,
            "tenders_url": _CANADABUYS_TENDERS_URL,
            "description": "Federal proactive disclosure contracts and CanadaBuys tenders",
        }
