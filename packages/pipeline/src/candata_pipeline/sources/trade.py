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

import re
import shutil
import tempfile
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import structlog

from candata_shared.config import settings
from candata_shared.geo import normalize_geo_column, normalize_statcan_geo
from candata_shared.time_utils import parse_statcan_date, parse_statcan_date_expr
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
    async def _download_csv_zip(self, table_pid: str) -> Path:
        """Download a StatCan trade ZIP to a temp file and return its path."""
        url = self._csv_zip_url(table_pid)
        self._log.info("downloading", url=url, table_pid=table_pid)
        tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
        try:
            async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
                async with client.stream("GET", url) as response:
                    response.raise_for_status()
                    async for chunk in response.aiter_bytes(chunk_size=256 * 1024):
                        tmp.write(chunk)
            tmp.close()
            return Path(tmp.name)
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    @staticmethod
    def _parse_csv_zip(zip_path: Path, table_pid: str) -> pl.DataFrame:
        """Extract and parse the data CSV from a StatCan ZIP on disk.

        Streams the CSV entry to a temp file so the full uncompressed
        content is never held in memory alongside the parsed DataFrame.
        """
        csv_tmp_path: Path | None = None
        try:
            with zipfile.ZipFile(zip_path) as zf:
                data_files = [
                    n for n in zf.namelist()
                    if n.endswith(".csv") and "MetaData" not in n
                ]
                if not data_files:
                    raise ValueError(f"No data CSV found in trade zip for pid={table_pid}")

                csv_fd = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
                csv_tmp_path = Path(csv_fd.name)
                with zf.open(data_files[0]) as src:
                    shutil.copyfileobj(src, csv_fd, length=256 * 1024)
                csv_fd.close()

            df = pl.read_csv(
                csv_tmp_path,
                infer_schema_length=0,
                null_values=list(_SUPPRESSED),
                truncate_ragged_lines=True,
            )

            first_col = df.columns[0]
            if first_col.startswith("\ufeff"):
                df = df.rename({first_col: first_col.lstrip("\ufeff")})

            return df
        finally:
            zip_path.unlink(missing_ok=True)
            if csv_tmp_path:
                csv_tmp_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(self, *, table_pid: str = "12100011", **kwargs: Any) -> pl.DataFrame:
        """Download a StatCan trade table CSV bundle.

        Args:
            table_pid: StatCan table ID. Default is 12-10-0011 (commodity trade).
        """
        zip_path = await self._download_csv_zip(table_pid)
        return self._parse_csv_zip(zip_path, table_pid)

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
        # Operate on the input directly — polars expressions create new
        # DataFrames so there is no need for an expensive deep copy.
        df = raw.rename({col: col.strip().upper() for col in raw.columns})

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
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

        # Extract HS code using vectorized regex instead of map_elements
        napcs_col = next(
            (c for c in df.columns if "NAPCS" in c.upper() or "PRODUCT" in c.upper() or "COMMODITY" in c.upper()),
            None,
        )
        if napcs_col:
            df = df.with_columns(
                pl.col(napcs_col)
                .str.strip_chars()
                .str.extract(r"^\[?(\d{2,10})\]?\s*[-\u2013]", 1)
                .alias("hs_code"),
                pl.col(napcs_col).alias("hs_description"),
            )
            # Drop rows without HS code (aggregate/total rows)
            df = df.filter(pl.col("hs_code").is_not_null())
        else:
            # Table 12-10-0011 has no commodity/NAPCS column — it is a
            # partner-level aggregate.  Use "TOTAL" as the HS placeholder
            # so the rows are not discarded.
            self._log.info("no_napcs_column", columns=df.columns)
            df = df.with_columns(
                pl.lit("TOTAL").alias("hs_code"),
                pl.lit("Total of all commodities").alias("hs_description"),
            )

        # Normalize GEO → province SGC code using batch lookup
        if "GEO" in df.columns:
            df = normalize_geo_column(df, "GEO")
            # Keep only country/province level rows as province
            df = df.with_columns(
                pl.when(pl.col("geo_level").is_in(["country", "pr"]))
                .then(pl.col("sgc_code"))
                .otherwise(pl.lit(None))
                .alias("province")
            )
            df = df.drop(["sgc_code", "geo_level"])
        else:
            df = df.with_columns(pl.lit("01").alias("province"))

        df = df.filter(pl.col("province").is_not_null())

        # Partner country: commodity table is all-countries aggregate
        # Use a batch lookup via join instead of row-by-row map_elements
        if "PARTNER" in " ".join(df.columns).upper():
            partner_col = next(c for c in df.columns if "PARTNER" in c.upper())
            unique_partners = df.select(pl.col(partner_col).unique().drop_nulls()).to_series().to_list()
            partner_lookup = pl.DataFrame({
                partner_col: unique_partners,
                "partner_country": [normalize_country(p) for p in unique_partners],
            })
            df = df.join(partner_lookup, on=partner_col, how="left")
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
        df = raw.rename({col: col.strip().upper() for col in raw.columns})

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
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

        # HS code from NAPCS/commodity column — vectorized regex
        # Table 12-10-0126 uses trailing bracket codes like "Description [411]"
        # or "Description [C153]".  Extract the alphanumeric code from brackets.
        napcs_col = next(
            (c for c in df.columns if "NAPCS" in c.upper() or "PRODUCT" in c.upper() or "COMMODITY" in c.upper()),
            None,
        )
        if napcs_col:
            df = df.with_columns(
                pl.col(napcs_col)
                .str.strip_chars()
                .str.extract(r"\[([A-Za-z]?\d{1,10})\]\s*$", 1)
                .alias("hs_code"),
                pl.col(napcs_col).alias("hs_description"),
            )
        else:
            df = df.with_columns(
                pl.lit("00").alias("hs_code"),
                pl.lit(None).cast(pl.String).alias("hs_description"),
            )

        df = df.filter(pl.col("hs_code").is_not_null())

        # Province — batch geo lookup
        if "GEO" in df.columns:
            df = normalize_geo_column(df, "GEO")
            df = df.with_columns(
                pl.when(pl.col("geo_level").is_in(["country", "pr"]))
                .then(pl.col("sgc_code"))
                .otherwise(pl.lit(None))
                .alias("province")
            )
            df = df.drop(["sgc_code", "geo_level"])
        else:
            df = df.with_columns(pl.lit("01").alias("province"))

        df = df.filter(pl.col("province").is_not_null())

        # Partner country — batch lookup
        partner_col = next(
            (c for c in df.columns if "PARTNER" in c.upper() or "TRADING" in c.upper()),
            None,
        )
        if partner_col:
            unique_partners = df.select(pl.col(partner_col).unique().drop_nulls()).to_series().to_list()
            partner_lookup = pl.DataFrame({
                partner_col: unique_partners,
                "partner_country": [normalize_country(p) for p in unique_partners],
            })
            df = df.join(partner_lookup, on=partner_col, how="left")
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
