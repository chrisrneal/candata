"""
sources/trade.py — Statistics Canada trade data source adapter.

Downloads the CIMT (Canadian International Merchandise Trade) bulk CSVs:
  - Table 12-10-0011-01: commodity trade by HS code, monthly
  - Table 12-10-0126-01: bilateral trade by partner country, monthly

Parses with polars, extracts HS codes from NAPCS descriptions, normalises
province names to SGC codes, and outputs rows matching the trade_flows
schema.

Usage:
    source = TradeSource()
    df = await source.run(table_pid="12100011")
"""

from __future__ import annotations

import io
import re
import zipfile
from datetime import date
from typing import Any

import httpx
import polars as pl
import structlog

from candata_shared.config import settings
from candata_shared.geo import normalize_statcan_geo
from candata_shared.time_utils import parse_statcan_date
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# StatCan suppressed value markers
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})

# Regex to extract leading HS-style numeric code from NAPCS description.
# Examples:
#   "0201 - Meat of bovine animals, fresh or chilled"  → "0201"
#   "[21] - Pharmaceutical products"                    → "21"
#   "Total of all merchandise"                          → None (no code)
_HS_CODE_RE = re.compile(r"^\[?(\d{2,10})\]?\s*[-–]")


def extract_hs_code(napcs: str | None) -> str | None:
    """Extract the leading HS code digits from a NAPCS commodity description."""
    if not napcs:
        return None
    m = _HS_CODE_RE.match(napcs.strip())
    return m.group(1) if m else None


# Mapping for normalizing partner country names to ISO 3166-1 alpha-3.
# The bilateral table uses free-text names; we map the most common ones.
_COUNTRY_ALIASES: dict[str, str] = {
    "united states": "USA",
    "united states of america": "USA",
    "u.s.": "USA",
    "china": "CHN",
    "people's republic of china": "CHN",
    "japan": "JPN",
    "united kingdom": "GBR",
    "germany": "DEU",
    "mexico": "MEX",
    "south korea": "KOR",
    "korea, south": "KOR",
    "republic of korea": "KOR",
    "france": "FRA",
    "india": "IND",
    "italy": "ITA",
    "brazil": "BRA",
    "australia": "AUS",
    "netherlands": "NLD",
    "taiwan": "TWN",
    "switzerland": "CHE",
    "saudi arabia": "SAU",
    "norway": "NOR",
    "belgium": "BEL",
    "spain": "ESP",
    "sweden": "SWE",
    "all countries": "WLD",
    "total, all countries": "WLD",
    "total all countries": "WLD",
}


def normalize_country(name: str | None) -> str | None:
    """Normalize a partner country name to ISO alpha-3, or return as-is."""
    if not name:
        return None
    key = name.strip().lower()
    return _COUNTRY_ALIASES.get(key, name.strip())


class TradeSource(BaseSource):
    """Downloads and parses StatCan CIMT trade CSV bundles."""

    name = "StatCan-Trade"

    def __init__(self, timeout: float = 180.0) -> None:
        super().__init__()
        self._base_url = settings.statcan_base_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # URL / parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_table_id(table_pid: str) -> str:
        return table_pid.replace("-", "")[:8]

    def _csv_zip_url(self, table_pid: str) -> str:
        table_id = self._to_table_id(table_pid)
        return f"{self._base_url}/n1/tbl/csv/{table_id}-eng.zip"

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_csv_zip(self, table_pid: str) -> bytes:
        url = self._csv_zip_url(table_pid)
        self._log.info("downloading", url=url, table_pid=table_pid)
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    @staticmethod
    def _parse_csv_zip(zip_bytes: bytes, table_pid: str) -> pl.DataFrame:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            data_files = [
                n for n in zf.namelist()
                if n.endswith(".csv") and "MetaData" not in n
            ]
            if not data_files:
                raise ValueError(f"No data CSV found in trade zip for pid={table_pid}")
            raw_bytes = zf.read(data_files[0])

        if raw_bytes.startswith(b"\xef\xbb\xbf"):
            raw_bytes = raw_bytes[3:]

        return pl.read_csv(
            io.BytesIO(raw_bytes),
            infer_schema_length=0,
            null_values=list(_SUPPRESSED),
            truncate_ragged_lines=True,
        )

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(self, *, table_pid: str = "12100011", **kwargs: Any) -> pl.DataFrame:
        """Download a StatCan trade table CSV bundle.

        Args:
            table_pid: StatCan table ID. Default is 12-10-0011 (commodity trade).
        """
        zip_bytes = await self._download_csv_zip(table_pid)
        return self._parse_csv_zip(zip_bytes, table_pid)

    def transform(
        self,
        raw: pl.DataFrame,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Transform raw commodity trade CSV into trade_flows schema.

        Output columns:
            direction       — "import" or "export"
            hs_code         — HS commodity code extracted from NAPCS
            hs_description  — full NAPCS description
            partner_country — "WLD" for commodity table (all countries)
            province        — SGC province code
            ref_date        — first day of reference month
            value_cad       — dollar value
            volume          — physical volume (if UOM is weight/units)
            volume_unit     — unit of measure for volume
        """
        df = raw.clone()

        # Standardize column names to uppercase
        df = df.rename({col: col.strip().upper() for col in df.columns})

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            pl.col("REF_DATE")
            .map_elements(parse_statcan_date, return_dtype=pl.Date)
            .alias("ref_date")
        ).filter(pl.col("ref_date").is_not_null())

        if start_date:
            df = df.filter(pl.col("ref_date") >= start_date)
        if end_date:
            df = df.filter(pl.col("ref_date") <= end_date)

        # Filter to Import/Export rows only (skip "Trade balance")
        trade_col = "TRADE" if "TRADE" in df.columns else None
        if trade_col:
            df = df.filter(
                pl.col(trade_col).str.to_lowercase().is_in(["import", "export"])
            ).with_columns(
                pl.col(trade_col).str.to_lowercase().alias("direction")
            )
        else:
            df = df.with_columns(pl.lit("export").alias("direction"))

        # Extract HS code from NAPCS description
        napcs_col = next(
            (c for c in df.columns if "NAPCS" in c.upper() or "PRODUCT" in c.upper() or "COMMODITY" in c.upper()),
            None,
        )
        if napcs_col:
            df = df.with_columns(
                pl.col(napcs_col)
                .map_elements(extract_hs_code, return_dtype=pl.String)
                .alias("hs_code"),
                pl.col(napcs_col).alias("hs_description"),
            )
        else:
            df = df.with_columns(
                pl.lit(None).cast(pl.String).alias("hs_code"),
                pl.lit(None).cast(pl.String).alias("hs_description"),
            )

        # Drop rows without HS code (aggregate/total rows)
        df = df.filter(pl.col("hs_code").is_not_null())

        # Normalize GEO → province SGC code
        def geo_to_province(geo: str | None) -> str | None:
            if not geo:
                return None
            result = normalize_statcan_geo(geo)
            if not result:
                return None
            level, code = result
            if level in ("country", "pr"):
                return code
            return None

        if "GEO" in df.columns:
            df = df.with_columns(
                pl.col("GEO")
                .map_elements(geo_to_province, return_dtype=pl.String)
                .alias("province")
            )
        else:
            df = df.with_columns(pl.lit("01").alias("province"))

        df = df.filter(pl.col("province").is_not_null())

        # Partner country: commodity table is all-countries aggregate
        if "PARTNER" in " ".join(df.columns).upper():
            partner_col = next(c for c in df.columns if "PARTNER" in c.upper())
            df = df.with_columns(
                pl.col(partner_col)
                .map_elements(normalize_country, return_dtype=pl.String)
                .alias("partner_country")
            )
        else:
            df = df.with_columns(pl.lit("WLD").alias("partner_country"))

        # Parse VALUE → value_cad
        if "VALUE" in df.columns:
            df = df.with_columns(
                pl.col("VALUE").cast(pl.Float64, strict=False).alias("value_cad")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value_cad"))

        # Volume and unit
        uom_col = "UOM" if "UOM" in df.columns else None
        if uom_col:
            df = df.with_columns(pl.col(uom_col).alias("volume_unit"))
        else:
            df = df.with_columns(pl.lit(None).cast(pl.String).alias("volume_unit"))
        df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("volume"))

        # Select final columns
        keep = [
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        ]
        df = df.select([c for c in keep if c in df.columns])

        self._log.info(
            "transform_complete",
            output_rows=len(df),
            unique_hs_codes=df["hs_code"].n_unique() if "hs_code" in df.columns else 0,
        )
        return df

    def transform_bilateral(
        self,
        raw: pl.DataFrame,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> pl.DataFrame:
        """Transform bilateral trade table (12-10-0126) into trade_flows schema.

        Same output schema as transform(), but with real partner_country values.
        """
        df = raw.clone()
        df = df.rename({col: col.strip().upper() for col in df.columns})

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            pl.col("REF_DATE")
            .map_elements(parse_statcan_date, return_dtype=pl.Date)
            .alias("ref_date")
        ).filter(pl.col("ref_date").is_not_null())

        if start_date:
            df = df.filter(pl.col("ref_date") >= start_date)
        if end_date:
            df = df.filter(pl.col("ref_date") <= end_date)

        # Filter Import/Export
        trade_col = "TRADE" if "TRADE" in df.columns else None
        if trade_col:
            df = df.filter(
                pl.col(trade_col).str.to_lowercase().is_in(["import", "export"])
            ).with_columns(
                pl.col(trade_col).str.to_lowercase().alias("direction")
            )
        else:
            df = df.with_columns(pl.lit("export").alias("direction"))

        # HS code from NAPCS/commodity column
        napcs_col = next(
            (c for c in df.columns if "NAPCS" in c.upper() or "PRODUCT" in c.upper() or "COMMODITY" in c.upper()),
            None,
        )
        if napcs_col:
            df = df.with_columns(
                pl.col(napcs_col)
                .map_elements(extract_hs_code, return_dtype=pl.String)
                .alias("hs_code"),
                pl.col(napcs_col).alias("hs_description"),
            )
        else:
            df = df.with_columns(
                pl.lit("00").alias("hs_code"),
                pl.lit(None).cast(pl.String).alias("hs_description"),
            )

        df = df.filter(pl.col("hs_code").is_not_null())

        # Province
        def geo_to_province(geo: str | None) -> str | None:
            if not geo:
                return None
            result = normalize_statcan_geo(geo)
            if not result:
                return None
            level, code = result
            if level in ("country", "pr"):
                return code
            return None

        if "GEO" in df.columns:
            df = df.with_columns(
                pl.col("GEO")
                .map_elements(geo_to_province, return_dtype=pl.String)
                .alias("province")
            )
        else:
            df = df.with_columns(pl.lit("01").alias("province"))

        df = df.filter(pl.col("province").is_not_null())

        # Partner country
        partner_col = next(
            (c for c in df.columns if "PARTNER" in c.upper() or "TRADING" in c.upper()),
            None,
        )
        if partner_col:
            df = df.with_columns(
                pl.col(partner_col)
                .map_elements(normalize_country, return_dtype=pl.String)
                .alias("partner_country")
            )
        else:
            df = df.with_columns(pl.lit("WLD").alias("partner_country"))

        # VALUE
        if "VALUE" in df.columns:
            df = df.with_columns(
                pl.col("VALUE").cast(pl.Float64, strict=False).alias("value_cad")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value_cad"))

        df = df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("volume"),
            pl.lit(None).cast(pl.String).alias("volume_unit"),
        )

        keep = [
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        ]
        df = df.select([c for c in keep if c in df.columns])

        self._log.info(
            "transform_bilateral_complete",
            output_rows=len(df),
            unique_partners=df["partner_country"].n_unique() if "partner_country" in df.columns else 0,
        )
        return df

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "base_url": self._base_url,
            "description": "Statistics Canada CIMT — international merchandise trade",
            "tables": {
                "12100011": "Commodity trade by HS code, monthly",
                "12100126": "Bilateral trade by partner country, monthly",
            },
        }
