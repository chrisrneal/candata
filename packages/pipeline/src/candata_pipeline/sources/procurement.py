"""
sources/procurement.py — Federal proactive disclosure procurement source.

Pulls awarded contracts and open tenders from:
  1. open.canada.ca proactive disclosure CSV (contracts) via CKAN API
  2. CanadaBuys open-data CSV feeds (tenders)

Contract CSV dataset (CKAN):
  GET https://open.canada.ca/data/api/3/action/package_show?id=d8f85d91-7dec-4fd1-8055-483b77225d8b

Contract CSV columns (actual proactive disclosure format):
  reference_number, procurement_id, vendor_name, vendor_postal_code,
  buyer_name, contract_date, economic_object_code, description_en,
  contract_period_start, delivery_date, original_value, final_value,
  comments_en, additional_comments_en, amendment_value, agreement_type_code

CanadaBuys tender CSV feeds (replaced the retired REST API in 2023):
  - Open tenders:    https://canadabuys.canada.ca/opendata/pub/openTenderNotice-ouvertAvisAppelOffres.csv
  - Complete archive: https://canadabuys.canada.ca/opendata/pub/tenderNoticeComplete-avisAppelOffresComplet.csv
  Dataset catalogue: https://open.canada.ca/data/api/3/action/package_show?id=6abd20d4-7a1c-4b38-baa2-9525d0bb2fd2

  NOTE: The old CanadaBuys REST API at
  https://canadabuys.canada.ca/en/tender-opportunities/api/v1/notices
  was retired when buyandsell.gc.ca migrated to CanadaBuys (Drupal 10).
  It now returns HTTP 403.

Usage:
    source = ProcurementSource()
    contracts_df = await source.extract(dataset="contracts")
    tenders_df   = await source.extract(dataset="tenders")
"""

from __future__ import annotations

import io
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
_CKAN_DATASET_ID = "d8f85d91-7dec-4fd1-8055-483b77225d8b"
_CKAN_API_URL = "https://open.canada.ca/data/api/3/action/package_show"

# Fallback direct download URL if CKAN API fails
# (The old datastore/dump endpoint was retired; this is the current
#  direct-download link for "Contracts over $10,000".)
_PROACTIVE_CSV_URL = (
    "https://open.canada.ca/data/dataset/d8f85d91-7dec-4fd1-8055-483b77225d8b"
    "/resource/fac950c0-00d5-4ec1-a4d3-9cbebf98a305/download/contracts.csv"
)

# CanadaBuys tender CSV feeds (replaced the retired REST API)
_CANADABUYS_OPEN_TENDERS_URL = (
    "https://canadabuys.canada.ca/opendata/pub/"
    "openTenderNotice-ouvertAvisAppelOffres.csv"
)
_CANADABUYS_COMPLETE_TENDERS_URL = (
    "https://canadabuys.canada.ca/opendata/pub/"
    "tenderNoticeComplete-avisAppelOffresComplet.csv"
)
# CKAN dataset ID for the CanadaBuys tender notices catalogue
_TENDERS_CKAN_DATASET_ID = "6abd20d4-7a1c-4b38-baa2-9525d0bb2fd2"

# The CanadaBuys domain blocks requests without a browser-like User-Agent.
_BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/125.0.0.0 Safari/537.36"
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
        """Extract *contract* CSV resource URLs from CKAN package metadata.

        Filters to CSVs whose name matches the expected contract
        datasets ("Contracts over $10,000" and the legacy equivalent).
        Excludes unrelated CSVs like "Nothing to Report" or
        "Aggregated Total" that have different schemas.
        """
        _WANT = {"contracts over $10,000", "contracts over $10,000 – legacy data"}
        urls: list[str] = []
        for resource in ckan_result.get("resources", []):
            fmt = (resource.get("format") or "").upper()
            name = (resource.get("name") or "").strip().lower()
            url = resource.get("url", "")
            if fmt == "CSV" and url and name in _WANT:
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
    async def _download_tenders_csv(self, url: str) -> bytes:
        """Download a CanadaBuys tender-notice CSV.

        The canadabuys.canada.ca domain blocks requests without a
        browser-like User-Agent header, so we override it here.
        """
        self._log.info("tenders_csv_download", url=url[:120])
        async with httpx.AsyncClient(
            timeout=self._timeout,
            follow_redirects=True,
            headers={"User-Agent": _BROWSER_UA},
        ) as client:
            r = await client.get(url)
            if r.is_client_error:
                raise RuntimeError(
                    f"Client error '{r.status_code} {r.reason_phrase}' "
                    f"for url '{r.url}'"
                )
            r.raise_for_status()
            return r.content

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

        For tenders: downloads CanadaBuys open-data CSV feed.
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
        """Download tender notices from CanadaBuys open-data CSV feeds.

        Tries the "open tenders" CSV first (smaller, currently-open only).
        Falls back to the complete archive if the open-tenders endpoint
        fails.  If all downloads fail, logs a warning and returns an
        empty DataFrame — the pipeline continues without crashing.
        """
        urls = [
            ("open_tenders", _CANADABUYS_OPEN_TENDERS_URL),
            ("complete_tenders", _CANADABUYS_COMPLETE_TENDERS_URL),
        ]

        for label, url in urls:
            try:
                raw_bytes = await self._download_tenders_csv(url)
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
                    if max_tenders and len(df) > max_tenders:
                        df = df.head(max_tenders)
                    self._log.info(
                        "tenders_csv_loaded",
                        feed=label,
                        url=url[:80],
                        rows=len(df),
                    )
                    return df
            except Exception as exc:
                self._log.warning(
                    "tenders_csv_failed",
                    feed=label,
                    url=url[:80],
                    error=str(exc),
                )

        self._log.warning(
            "tenders_all_sources_failed",
            hint="All CanadaBuys CSV feeds failed. "
            "Tenders extraction is skipped.",
        )
        return pl.DataFrame()

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

        if dataset == "contracts":
            df = self._normalize_columns(raw)
            return self._transform_contracts(df)
        # Tenders: skip _normalize_columns — the CanadaBuys CSV uses
        # bilingual column names that are mapped directly in
        # _transform_tenders().
        return self._transform_tenders(raw)

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

        exprs: list[pl.Expr] = []
        col_mapping: dict[str, str] = {}

        if col_ref:
            exprs.append(pl.col(col_ref).alias("contract_number"))
            col_mapping["contract_number"] = col_ref

        # Vectorized vendor normalization: strip, collapse whitespace, title-case
        if col_vendor:
            exprs.append(
                pl.col(col_vendor)
                .str.strip_chars()
                .str.replace_all(r"\s+", " ")
                .str.to_titlecase()
                .alias("vendor_name")
            )
            col_mapping["vendor_name"] = col_vendor

        # Department normalization via batch lookup
        if col_dept:
            unique_depts = df.select(pl.col(col_dept).unique().drop_nulls()).to_series().to_list()
            norm_col = "__dept_normalized"
            dept_lookup = pl.DataFrame({
                col_dept: unique_depts,
                norm_col: [normalize_department(d) for d in unique_depts],
            })
            df = df.join(dept_lookup, on=col_dept, how="left").with_columns(
                pl.col(norm_col).alias("department")
            ).drop(norm_col)
            # Don't add to exprs — already a column
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

        # Economic object category via batch lookup
        if col_econ:
            unique_codes = df.select(pl.col(col_econ).unique().drop_nulls()).to_series().to_list()
            econ_lookup = pl.DataFrame({
                col_econ: unique_codes,
                "category": [categorize_economic_object(c) for c in unique_codes],
            })
            df = df.join(econ_lookup, on=col_econ, how="left")

        if not exprs and not col_dept and not col_econ:
            return df

        result = df.with_columns(exprs) if exprs else df

        # Select only the output columns
        output_cols = [e.meta.output_name() for e in exprs]
        if col_dept:
            output_cols.append("department")
        if col_econ:
            output_cols.append("category")
        result = result.select([c for c in output_cols if c in result.columns])

        # Add raw_data — build JSON column using polars struct serialisation
        # instead of materialising the entire DataFrame as Python dicts.
        # polars' .struct.json_encode() is vectorised and never creates
        # Python objects for every row.
        raw_struct = df.select(df.columns).to_struct("raw_struct")
        result = result.with_columns(
            raw_struct.struct.json_encode().alias("raw_data")
        )

        return result

    def _transform_tenders(self, df: pl.DataFrame) -> pl.DataFrame:
        """Map CanadaBuys CSV fields to tenders table schema.

        The CSV uses bilingual column names like
        ``referenceNumber-numeroReference``.  We match either the new
        CanadaBuys CSV names or the legacy API field names so the
        transform stays backwards-compatible.

        Column normalization is intentionally *not* applied here
        (unlike contracts) because the bilingual column names would
        produce unreadable snake_case identifiers.
        """
        # Strip stray quotes from column names (polars CSV reader
        # usually handles this, but belt-and-suspenders).
        rename = {c: c.strip('"') for c in df.columns if c.startswith('"') or c.endswith('"')}
        if rename:
            df = df.rename(rename)
        col_map = {
            "tender_number": [
                "referenceNumber-numeroReference",
                "reference_number",
                "tender_number",
                "notice_id",
            ],
            "title": [
                "title-titre-eng",
                "title",
                "title_en",
                "subject",
            ],
            "department": [
                "contractingEntityName-nomEntitContractante-eng",
                "department",
                "buyer_name",
                "organization",
            ],
            "closing_date": [
                "tenderClosingDate-appelOffresDateCloture",
                "closing_date",
                "close_date",
                "closing",
            ],
            "status": [
                "tenderStatus-appelOffresStatut-eng",
                "status",
                "notice_status",
            ],
            "estimated_value": [
                "estimated_value",
                "budget",
                "contract_value",
            ],
            "category": [
                "procurementCategory-categorieApprovisionnement",
                "category",
                "commodity",
                "gsin",
            ],
            "region": [
                "regionsOfDelivery-regionsLivraison-eng",
                "region",
                "delivery_region",
            ],
            "source_url": [
                "noticeURL-URLavis-eng",
                "url",
                "source_url",
                "link",
            ],
        }

        def pick(candidates: list[str]) -> str | None:
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        exprs: list[pl.Expr] = []
        dept_src: str | None = None
        for out_col, candidates in col_map.items():
            src = pick(candidates)
            if src:
                if out_col == "closing_date":
                    # CSV values may be ISO datetime (2026-03-24T14:00:00)
                    # or plain date; extract the date portion.
                    exprs.append(
                        pl.col(src)
                        .str.slice(0, 10)
                        .str.to_date(strict=False)
                        .alias(out_col)
                    )
                elif out_col == "estimated_value":
                    exprs.append(
                        pl.col(src).cast(pl.Float64, strict=False).alias(out_col)
                    )
                elif out_col == "department":
                    # Batch lookup instead of map_elements
                    dept_src = src
                elif out_col == "category":
                    # CanadaBuys CSV uses prefixed values like "*SRV";
                    # strip leading '*' and whitespace.
                    exprs.append(
                        pl.col(src)
                        .str.strip_chars()
                        .str.replace(r"^\*", "")
                        .alias(out_col)
                    )
                elif out_col == "region":
                    # Same star-prefix pattern for regions.
                    exprs.append(
                        pl.col(src)
                        .str.strip_chars()
                        .str.replace(r"^\*", "")
                        .alias(out_col)
                    )
                else:
                    exprs.append(pl.col(src).alias(out_col))

        if not exprs and dept_src is None:
            return df

        if dept_src:
            unique_depts = df.select(pl.col(dept_src).unique().drop_nulls()).to_series().to_list()
            norm_col = "__dept_normalized"
            dept_lookup = pl.DataFrame({
                dept_src: unique_depts,
                norm_col: [normalize_department(d) for d in unique_depts],
            })
            df = df.join(dept_lookup, on=dept_src, how="left").with_columns(
                pl.col(norm_col).alias("department")
            ).drop(norm_col)

        result = df.with_columns(exprs) if exprs else df
        output_cols = [e.meta.output_name() for e in exprs]
        if dept_src:
            output_cols.append("department")
        return result.select([c for c in output_cols if c in result.columns])

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "ckan_dataset_id": _CKAN_DATASET_ID,
            "tenders_ckan_dataset_id": _TENDERS_CKAN_DATASET_ID,
            "tenders_open_url": _CANADABUYS_OPEN_TENDERS_URL,
            "tenders_complete_url": _CANADABUYS_COMPLETE_TENDERS_URL,
            "description": (
                "Federal proactive disclosure contracts and "
                "CanadaBuys tender notices (CSV feeds)"
            ),
        }
