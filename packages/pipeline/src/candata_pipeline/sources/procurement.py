"""
sources/procurement.py — Federal proactive disclosure procurement source.

Pulls awarded contracts and open tenders from:
  1. open.canada.ca proactive disclosure CSV (contracts)
  2. CanadaBuys API (active/recent tenders)

Contract CSV dataset:
  https://open.canada.ca/data/en/dataset/d8f85d91-7dec-4fd1-8055-483b77225d8b

Contract CSV columns (actual proactive disclosure format):
  reference_number, procurement_id, vendor_name, vendor_postal_code,
  buyer_name, contract_date, economic_object_code, description_en,
  contract_period_start, delivery_date, original_value, final_value,
  comments_en, additional_comments_en, amendment_value, agreement_type_code

CanadaBuys tender API:
  https://canadabuys.canada.ca/en/tender-opportunities/api/v1/notices

Usage:
    source = ProcurementSource()
    contracts_df = await source.run(dataset="contracts", year=2023)
    tenders_df   = await source.run(dataset="tenders", status="active")
"""

from __future__ import annotations

import io
from datetime import date
from typing import Any, Literal

import httpx
import polars as pl
import structlog

from candata_shared.config import settings
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

Dataset = Literal["contracts", "tenders"]

# open.canada.ca resource IDs for proactive disclosure contracts
# The actual IDs may change — use the CKAN API to discover current ones
_CONTRACT_RESOURCE_IDS: dict[str, str] = {
    # Sample resource IDs — each department publishes separately.
    # This list covers a subset; the full pipeline would paginate all depts.
    "all": "d8f85d91-7dec-4fd1-8055-483b77225d8b",  # master aggregated file
}

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
}


def normalize_department(name: str | None) -> str | None:
    if not name:
        return None
    return _DEPT_NORMALIZE.get(name.strip().lower(), name.strip())


class ProcurementSource(BaseSource):
    """Pulls federal procurement data from open.canada.ca and CanadaBuys."""

    name = "CanadaBuys"

    _PROACTIVE_URL = (
        "https://open.canada.ca/data/en/datastore/dump/d8f85d91-7dec-4fd1-8055-483b77225d8b"
        "?bom=True&format=csv"
    )
    _CANADABUYS_TENDERS_URL = "https://canadabuys.canada.ca/en/tender-opportunities/api/v1/notices"

    def __init__(self, timeout: float = 120.0) -> None:
        super().__init__()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_contracts_csv(self) -> bytes:
        """Download the proactive disclosure contract CSV dump."""
        self._log.info("contracts_download", url=self._PROACTIVE_URL)
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            r = await client.get(self._PROACTIVE_URL)
            r.raise_for_status()
            return r.content

    @with_retry(max_attempts=3, base_delay=1.0, retry_on=(httpx.HTTPError,))
    async def _fetch_tenders_page(self, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        """Fetch a page of active tenders from the CanadaBuys API."""
        params = {
            "status": "active",
            "page": page,
            "per_page": per_page,
            "format": "json",
        }
        self._log.debug("tenders_fetch", page=page)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            r = await client.get(self._CANADABUYS_TENDERS_URL, params=params)
            r.raise_for_status()
            return r.json()

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        dataset: Dataset = "contracts",
        year: int | None = None,
        status: str = "active",
        max_tenders: int = 500,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download procurement data.

        Args:
            dataset:     "contracts" or "tenders".
            year:        Filter contracts to this award year (None = all).
            status:      Tender status filter ("active", "closed").
            max_tenders: Maximum tender rows to fetch.

        Returns:
            Raw polars DataFrame.
        """
        if dataset == "contracts":
            raw_bytes = await self._download_contracts_csv()
            # Handle UTF-8 BOM
            if raw_bytes.startswith(b"\xef\xbb\xbf"):
                raw_bytes = raw_bytes[3:]
            df = pl.read_csv(
                io.BytesIO(raw_bytes),
                infer_schema_length=0,
                truncate_ragged_lines=True,
                encoding="utf8-lossy",
            )
            return df

        # Tenders — paginate the API
        all_notices: list[dict[str, Any]] = []
        page = 1
        while len(all_notices) < max_tenders:
            try:
                payload = await self._fetch_tenders_page(page)
            except Exception as exc:
                self._log.warning("tenders_page_failed", page=page, error=str(exc))
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

    def transform(self, raw: pl.DataFrame, *, dataset: Dataset = "contracts") -> pl.DataFrame:
        """
        Normalize procurement data to contracts or tenders schema.

        Output for contracts:
            contract_number, vendor_name, department, category,
            description, contract_value, start_date, end_date,
            award_date, amendment_number, source_url, raw_data

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
        result = pl.DataFrame()

        def pick(candidates: list[str]) -> str | None:
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        col_vendor = pick(["vendor_name", "vendor", "supplier_name"])
        col_dept = pick(["buyer_name", "department", "organization", "owner_org"])
        col_desc = pick(["description_en", "description", "desc"])
        col_value = pick(["final_value", "contract_value", "original_value", "value"])
        col_date = pick(["contract_date", "award_date", "date"])
        col_start = pick(["contract_period_start", "start_date"])
        col_end = pick(["delivery_date", "end_date"])
        col_ref = pick(["reference_number", "contract_number", "procurement_id"])
        col_amend = pick(["amendment_value", "amendment_number"])

        exprs: list[pl.Expr] = []

        if col_ref:
            exprs.append(pl.col(col_ref).alias("contract_number"))
        if col_vendor:
            exprs.append(pl.col(col_vendor).alias("vendor_name"))
        if col_dept:
            exprs.append(
                pl.col(col_dept)
                .map_elements(normalize_department, return_dtype=pl.String)
                .alias("department")
            )
        if col_desc:
            exprs.append(pl.col(col_desc).alias("description"))
        if col_value:
            exprs.append(pl.col(col_value).cast(pl.Float64, strict=False).alias("contract_value"))
        if col_date:
            exprs.append(pl.col(col_date).str.to_date(strict=False).alias("award_date"))
        if col_start:
            exprs.append(pl.col(col_start).str.to_date(strict=False).alias("start_date"))
        if col_end:
            exprs.append(pl.col(col_end).str.to_date(strict=False).alias("end_date"))
        if col_amend:
            exprs.append(pl.col(col_amend).alias("amendment_number"))

        if not exprs:
            return df

        return df.with_columns(exprs).select(
            [
                e.meta.output_name()
                for e in exprs
                if e.meta.output_name() in df.with_columns(exprs).columns
            ]
        )

    def _transform_tenders(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map CanadaBuys API fields to tenders table schema."""
        col_map = {
            "tender_number": ["reference_number", "tender_number", "notice_id"],
            "title": ["title", "title_en", "subject"],
            "department": ["department", "buyer_name", "organization"],
            "closing_date": ["closing_date", "close_date", "closing"],
            "status": ["status", "notice_status"],
            "estimated_value": ["estimated_value", "budget", "contract_value"],
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
                    exprs.append(pl.col(src).str.to_date(strict=False).alias(out_col))
                elif out_col == "estimated_value":
                    exprs.append(pl.col(src).cast(pl.Float64, strict=False).alias(out_col))
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
        return df.with_columns(exprs).select(
            [e.meta.output_name() for e in exprs if e.meta.output_name() in df.with_columns(exprs).columns]
        )

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "contracts_url": self._PROACTIVE_URL,
            "tenders_url": self._CANADABUYS_TENDERS_URL,
            "description": "Federal proactive disclosure contracts and CanadaBuys tenders",
        }
