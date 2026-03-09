"""
sources/trade.py — Statistics Canada trade data source adapter.

Downloads the CIMT (Canadian International Merchandise Trade) bulk CSVs:
  - Table 12-10-0121-01: commodity trade by NAPCS code, monthly (dollar values)
  - Table 12-10-0011-01: aggregate trade by principal trading partner, monthly

Parses with polars, extracts commodity codes from NAPCS descriptions,
normalises province names to SGC codes, and outputs rows matching the
trade_flows schema.

Note on tables:
  - 12-10-0121 has commodity-level data with real CAD dollar values and
    NAPCS product codes (3-digit numeric ≈ HS chapter equivalent).
  - 12-10-0011 has aggregate trade totals by partner country with real
    dollar values. No commodity breakdown, so hs_code = "TOTAL".
  - 12-10-0126 is intentionally NOT used — it contains price indexes,
    not dollar trade values.

Usage:
    source = TradeSource()
    df = await source.run(table_pid="12100121")
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

# Regex to extract bracketed NAPCS codes from descriptions.
# Examples:
#   "Live animals [111]"                               → "111"
#   "Farm, fishing and intermediate food products [C11]" → "C11"
#   "Total of all merchandise"                          → None (no code)
_NAPCS_BRACKET_RE = re.compile(r"\[([A-Za-z]?\d{1,10})\]\s*$")

# Legacy regex for leading code patterns (kept for backward compat)
_HS_CODE_RE = re.compile(r"^\[?(\d{2,10})\]?\s*[-–]")


def extract_hs_code(napcs: str | None) -> str | None:
    """Extract the leading HS code digits from a NAPCS commodity description."""
    if not napcs:
        return None
    m = _HS_CODE_RE.match(napcs.strip())
    return m.group(1) if m else None


# Scalar factor multipliers for StatCan data
_SCALAR_MULTIPLIER: dict[str, float] = {
    "units": 1.0,
    "tens": 10.0,
    "hundreds": 100.0,
    "thousands": 1_000.0,
    "millions": 1_000_000.0,
    "billions": 1_000_000_000.0,
}


# Mapping for normalizing partner country names to ISO 3166-1 alpha-3.
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


def _apply_scalar(df: pl.DataFrame) -> pl.DataFrame:
    """Multiply VALUE by the SCALAR_FACTOR to get actual dollar amounts.

    StatCan tables often report values in "millions" or "thousands".
    This converts them to actual CAD values.
    """
    if "SCALAR_FACTOR" not in df.columns or "VALUE" not in df.columns:
        return df

    # Build a lookup from unique scalar factor strings to multipliers
    unique_scalars = (
        df.select(pl.col("SCALAR_FACTOR").unique().drop_nulls())
        .to_series()
        .to_list()
    )
    scalar_lookup = pl.DataFrame({
        "SCALAR_FACTOR": unique_scalars,
        "_scalar_mult": [
            _SCALAR_MULTIPLIER.get(s.strip().lower(), 1.0) for s in unique_scalars
        ],
    })
    df = df.join(scalar_lookup, on="SCALAR_FACTOR", how="left")
    df = df.with_columns(
        (pl.col("VALUE").cast(pl.Float64, strict=False) * pl.col("_scalar_mult"))
        .alias("value_cad")
    )
    return df.drop("_scalar_mult")


def _debug_stage(
    label: str, df: pl.DataFrame, debug: bool, extra: dict[str, Any] | None = None
) -> None:
    """Print a debug summary line for a transform stage."""
    if not debug:
        return
    msg = f"  [{label}] {len(df):,} rows"
    if extra:
        msg += " | " + ", ".join(f"{k}={v}" for k, v in extra.items())
    print(msg)


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

    async def extract(self, *, table_pid: str = "12100121", **kwargs: Any) -> pl.DataFrame:
        """Download a StatCan trade table CSV bundle.

        Args:
            table_pid: StatCan table ID. Default is 12-10-0121 (commodity trade).
        """
        zip_path = await self._download_csv_zip(table_pid)
        return self._parse_csv_zip(zip_path, table_pid)

    def transform(
        self,
        raw: pl.DataFrame,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        debug: bool = False,
    ) -> pl.DataFrame:
        """Transform table 12-10-0121 (commodity trade) into trade_flows schema.

        Table 12-10-0121 has NAPCS-coded commodity breakdowns with dollar
        values in millions.  We extract numeric-only NAPCS codes (3-digit,
        roughly equivalent to HS chapters), filter to Customs basis /
        Unadjusted, and apply the scalar factor.

        Output columns:
            direction       — "import" or "export"
            hs_code         — NAPCS commodity code (numeric 3-digit)
            hs_description  — full NAPCS description
            partner_country — "WLD" (all countries aggregate)
            province        — SGC province code
            ref_date        — first day of reference month
            value_cad       — actual CAD dollar value (scalar applied)
            volume          — null (not available in this table)
            volume_unit     — null
        """
        df = raw.rename({col: col.strip().upper() for col in raw.columns})

        if debug:
            print(f"\n=== transform (12-10-0121 commodity) ===")
            print(f"  [raw] {len(df):,} rows, columns: {df.columns}")
            if len(df) > 0:
                print(f"  [raw] sample (first 3 rows, first 5 cols):")
                print(df.head(3).select(df.columns[:5]))

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
        ).filter(pl.col("ref_date").is_not_null())
        _debug_stage("parse_ref_date", df, debug)

        if start_date:
            df = df.filter(pl.col("ref_date") >= start_date)
        if end_date:
            df = df.filter(pl.col("ref_date") <= end_date)
        if start_date or end_date:
            _debug_stage("date_filter", df, debug)

        # Filter to Import/Export only (skip "Trade balance")
        trade_col = "TRADE" if "TRADE" in df.columns else None
        if trade_col:
            df = df.filter(
                pl.col(trade_col).str.to_lowercase().is_in(["import", "export"])
            ).with_columns(
                pl.col(trade_col).str.to_lowercase().alias("direction")
            )
        else:
            df = df.with_columns(pl.lit("export").alias("direction"))
        _debug_stage("trade_filter", df, debug)

        # Filter to Customs basis + Unadjusted to avoid double-counting
        basis_col = "BASIS" if "BASIS" in df.columns else None
        adj_col = "SEASONAL ADJUSTMENT" if "SEASONAL ADJUSTMENT" in df.columns else None
        if basis_col:
            df = df.filter(pl.col(basis_col).str.to_lowercase() == "customs")
        if adj_col:
            df = df.filter(pl.col(adj_col).str.to_lowercase() == "unadjusted")
        _debug_stage("basis_filter", df, debug, {
            "basis": "customs", "adjustment": "unadjusted",
        })

        # Extract NAPCS code from bracket notation: "Description [CODE]"
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
            pre_filter = len(df)
            # Keep only numeric codes (3-digit NAPCS ≈ HS chapter).
            # Drop alpha-prefix codes (C11, C151 = aggregate sections)
            # and rows without codes (e.g. "Total of all merchandise").
            df = df.filter(
                pl.col("hs_code").is_not_null()
                & pl.col("hs_code").str.contains(r"^\d+$")
            )
            _debug_stage("napcs_extract", df, debug, {
                "napcs_col": napcs_col,
                "dropped_no_code": pre_filter - len(df),
                "unique_codes": df["hs_code"].n_unique(),
            })
        else:
            self._log.warning("no_napcs_column", columns=df.columns)
            _debug_stage("napcs_extract", df, debug, {"napcs_col": "NONE"})
            df = df.with_columns(
                pl.lit("TOTAL").alias("hs_code"),
                pl.lit("Total of all commodities").alias("hs_description"),
            )

        # Normalize GEO → province SGC code
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
        _debug_stage("geo_normalize", df, debug)

        # Partner country: commodity table is all-countries aggregate
        df = df.with_columns(pl.lit("WLD").alias("partner_country"))

        # Apply scalar factor to VALUE → value_cad
        df = _apply_scalar(df)
        # If _apply_scalar didn't produce value_cad (missing columns), fall back
        if "value_cad" not in df.columns:
            if "VALUE" in df.columns:
                df = df.with_columns(
                    pl.col("VALUE").cast(pl.Float64, strict=False).alias("value_cad")
                )
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value_cad"))
        _debug_stage("value_parse", df, debug, {
            "null_values": df["value_cad"].null_count(),
        })

        # Volume and unit (not available in 12-10-0121)
        df = df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("volume"),
            pl.lit(None).cast(pl.String).alias("volume_unit"),
        )

        # Select final columns
        keep = [
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        ]
        df = df.select([c for c in keep if c in df.columns])

        _debug_stage("final", df, debug, {
            "unique_hs_codes": df["hs_code"].n_unique(),
            "unique_directions": df["direction"].n_unique(),
        })

        self._log.info(
            "transform_complete",
            output_rows=len(df),
            unique_hs_codes=df["hs_code"].n_unique() if "hs_code" in df.columns else 0,
        )
        return df

    def transform_partner(
        self,
        raw: pl.DataFrame,
        *,
        start_date: date | None = None,
        end_date: date | None = None,
        debug: bool = False,
    ) -> pl.DataFrame:
        """Transform table 12-10-0011 (partner trade) into trade_flows schema.

        Table 12-10-0011 has aggregate trade totals by principal trading
        partner (country).  No commodity breakdown — hs_code = "TOTAL".
        Values are in millions of dollars; scalar factor is applied.

        Output columns match the trade_flows schema.
        """
        df = raw.rename({col: col.strip().upper() for col in raw.columns})

        if debug:
            print(f"\n=== transform_partner (12-10-0011 partner) ===")
            print(f"  [raw] {len(df):,} rows, columns: {df.columns}")
            if len(df) > 0:
                print(f"  [raw] sample (first 3 rows, first 5 cols):")
                print(df.head(3).select(df.columns[:5]))

        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        # Parse ref_date
        df = df.filter(
            pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
        ).with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
        ).filter(pl.col("ref_date").is_not_null())
        _debug_stage("parse_ref_date", df, debug)

        if start_date:
            df = df.filter(pl.col("ref_date") >= start_date)
        if end_date:
            df = df.filter(pl.col("ref_date") <= end_date)
        if start_date or end_date:
            _debug_stage("date_filter", df, debug)

        # Filter Import/Export (skip "Trade Balance")
        trade_col = "TRADE" if "TRADE" in df.columns else None
        if trade_col:
            df = df.filter(
                pl.col(trade_col).str.to_lowercase().is_in(["import", "export"])
            ).with_columns(
                pl.col(trade_col).str.to_lowercase().alias("direction")
            )
        else:
            df = df.with_columns(pl.lit("export").alias("direction"))
        _debug_stage("trade_filter", df, debug)

        # Filter to Customs basis + Unadjusted
        basis_col = "BASIS" if "BASIS" in df.columns else None
        adj_col = "SEASONAL ADJUSTMENT" if "SEASONAL ADJUSTMENT" in df.columns else None
        if basis_col:
            df = df.filter(pl.col(basis_col).str.to_lowercase() == "customs")
        if adj_col:
            df = df.filter(pl.col(adj_col).str.to_lowercase() == "unadjusted")
        _debug_stage("basis_filter", df, debug)

        # hs_code = TOTAL (no commodity breakdown in this table)
        df = df.with_columns(
            pl.lit("TOTAL").alias("hs_code"),
            pl.lit("Total of all commodities").alias("hs_description"),
        )

        # Normalize GEO → province
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
        _debug_stage("geo_normalize", df, debug)

        # Partner country from "Principal trading partners" column
        partner_col = next(
            (c for c in df.columns if "PARTNER" in c.upper()),
            None,
        )
        if partner_col:
            unique_partners = (
                df.select(pl.col(partner_col).unique().drop_nulls())
                .to_series()
                .to_list()
            )
            partner_lookup = pl.DataFrame({
                partner_col: unique_partners,
                "partner_country": [normalize_country(p) for p in unique_partners],
            })
            df = df.join(partner_lookup, on=partner_col, how="left")
            _debug_stage("partner_normalize", df, debug, {
                "unique_partners": df["partner_country"].n_unique(),
            })
        else:
            df = df.with_columns(pl.lit("WLD").alias("partner_country"))
            _debug_stage("partner_normalize", df, debug, {"fallback": "WLD"})

        # Apply scalar factor to VALUE → value_cad
        df = _apply_scalar(df)
        if "value_cad" not in df.columns:
            if "VALUE" in df.columns:
                df = df.with_columns(
                    pl.col("VALUE").cast(pl.Float64, strict=False).alias("value_cad")
                )
            else:
                df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value_cad"))
        _debug_stage("value_parse", df, debug, {
            "null_values": df["value_cad"].null_count(),
        })

        # Volume/unit not available
        df = df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("volume"),
            pl.lit(None).cast(pl.String).alias("volume_unit"),
        )

        keep = [
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        ]
        df = df.select([c for c in keep if c in df.columns])

        _debug_stage("final", df, debug, {
            "unique_partners": df["partner_country"].n_unique(),
        })

        self._log.info(
            "transform_partner_complete",
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
                "12100121": "Commodity trade by NAPCS code, monthly (dollar values)",
                "12100011": "Aggregate trade by principal trading partner, monthly",
            },
        }
