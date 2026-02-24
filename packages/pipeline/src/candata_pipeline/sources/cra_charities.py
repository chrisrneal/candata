"""
sources/cra_charities.py — CRA T3010 registered charities data source.

The CRA publishes a bulk CSV download of all registered charities
(current and revoked) from their T3010 annual filing. This includes:
  - Legal name, operating name(s)
  - Business Number
  - Category / sub-category
  - Registration date / revocation date + reason
  - Province, city, postal code
  - Website URL
  - Fiscal year end
  - Total receipted gifts, total expenditures, total assets

Dataset URL (open.canada.ca):
  https://open.canada.ca/data/en/dataset/a9afe7cd-7f60-4e6b-9ea4-9dcedf6f5cee

The downloaded CSV is large (~10 MB) and updated monthly.

Output schema:
    bn          String   — Business Number (9-digit BN)
    name        String   — Legal name
    province    String   — Province SGC code
    city        String   — City name
    postal_code String   — Postal code (FSA or full)
    category    String   — Charity category
    status      String   — "registered" | "revoked"
    reg_date    Date     — Registration date
    rev_date    Date     — Revocation date (null if still registered)
    fiscal_year_end  String — "MM-DD" format
    total_receipts   Float64
    total_expenditures Float64
    total_assets     Float64

Usage:
    source = CRACharitiesSource()
    df = await source.run()
    # Returns all charities with normalized columns
"""

from __future__ import annotations

import io
from typing import Any

import httpx
import polars as pl
import structlog

from candata_shared.geo import province_name_to_code
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# open.canada.ca resource ID for the bulk CRA charity CSV
_CHARITY_RESOURCE_ID = "a9afe7cd-7f60-4e6b-9ea4-9dcedf6f5cee"
_CHARITY_CSV_URL = (
    "https://apps.cra-arc.gc.ca/ebci/hacc/srch/pub/bscSrch"
    "?request=download&srchRqstType=xml&lang=eng"
)
# Fallback: open.canada.ca datastore
_OPEN_CANADA_CSV_URL = (
    "https://open.canada.ca/data/en/datastore/dump/a9afe7cd-7f60-4e6b-9ea4-9dcedf6f5cee"
    "?bom=True&format=csv"
)


class CRACharitiesSource(BaseSource):
    """Downloads and parses the CRA registered charities bulk dataset."""

    name = "CRA"

    def __init__(self, timeout: float = 180.0) -> None:
        super().__init__()
        self._timeout = timeout

    @with_retry(max_attempts=3, base_delay=5.0, retry_on=(httpx.HTTPError,))
    async def _download_csv(self) -> bytes:
        """Download the CRA charities CSV from open.canada.ca."""
        self._log.info("cra_download", url=_OPEN_CANADA_CSV_URL)
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            r = await client.get(_OPEN_CANADA_CSV_URL)
            r.raise_for_status()
            return r.content

    async def extract(self, **kwargs: Any) -> pl.DataFrame:
        """
        Download the CRA registered charities CSV.

        Returns:
            Raw polars DataFrame with all original columns as strings.
        """
        raw_bytes = await self._download_csv()
        if raw_bytes.startswith(b"\xef\xbb\xbf"):
            raw_bytes = raw_bytes[3:]
        return pl.read_csv(
            io.BytesIO(raw_bytes),
            infer_schema_length=0,
            truncate_ragged_lines=True,
            encoding="utf8-lossy",
        )

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize CRA charity CSV to standard schema.

        Handles multiple known CRA CSV column naming conventions.
        """
        df = self._normalize_columns(raw)

        def pick(*candidates: str) -> str | None:
            for c in candidates:
                if c in df.columns:
                    return c
            return None

        col_bn = pick("bn", "business_number", "registration_number", "charity_bn")
        col_name = pick("legal_name", "name", "charity_name", "registered_name")
        col_prov = pick("province", "prov", "province_of_head_office")
        col_city = pick("city", "municipality", "head_office_city")
        col_postal = pick("postal_code", "postal", "postcode")
        col_category = pick("category", "type_of_charity", "charity_type")
        col_status = pick("status", "charity_status")
        col_reg = pick("registration_date", "reg_date", "effective_date")
        col_rev = pick("revocation_date", "rev_date", "date_revoked")
        col_receipts = pick("total_receipted_gifts", "receipts", "total_revenue")
        col_expenditures = pick("total_expenditures", "expenditures", "total_expenses")
        col_assets = pick("total_assets", "assets")

        exprs: list[pl.Expr] = []

        if col_bn:
            exprs.append(pl.col(col_bn).alias("bn"))
        if col_name:
            exprs.append(pl.col(col_name).alias("name"))
        if col_prov:
            exprs.append(
                pl.col(col_prov)
                .map_elements(
                    lambda p: province_name_to_code(p) if p else None,
                    return_dtype=pl.String,
                )
                .alias("province")
            )
        if col_city:
            exprs.append(pl.col(col_city).alias("city"))
        if col_postal:
            exprs.append(pl.col(col_postal).alias("postal_code"))
        if col_category:
            exprs.append(pl.col(col_category).alias("category"))
        if col_status:
            exprs.append(
                pl.col(col_status)
                .map_elements(
                    lambda s: "registered" if s and "regist" in s.lower() else "revoked",
                    return_dtype=pl.String,
                )
                .alias("status")
            )
        if col_reg:
            exprs.append(pl.col(col_reg).str.to_date(strict=False).alias("reg_date"))
        if col_rev:
            exprs.append(pl.col(col_rev).str.to_date(strict=False).alias("rev_date"))
        if col_receipts:
            exprs.append(pl.col(col_receipts).cast(pl.Float64, strict=False).alias("total_receipts"))
        if col_expenditures:
            exprs.append(
                pl.col(col_expenditures).cast(pl.Float64, strict=False).alias("total_expenditures")
            )
        if col_assets:
            exprs.append(pl.col(col_assets).cast(pl.Float64, strict=False).alias("total_assets"))

        if not exprs:
            return df

        result_col_names = [e.meta.output_name() for e in exprs]
        return df.with_columns(exprs).select(result_col_names)

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "csv_url": _OPEN_CANADA_CSV_URL,
            "description": "CRA T3010 registered charities bulk dataset",
            "resource_id": _CHARITY_RESOURCE_ID,
        }
