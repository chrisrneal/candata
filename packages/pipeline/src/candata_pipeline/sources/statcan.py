"""
sources/statcan.py — Statistics Canada WDS data source adapter.

Downloads full table CSV bundles from the StatCan Web Data Service,
stages them in DuckDB, and returns normalized polars DataFrames.

Supported tables (PIDs):
  3610043401 — GDP by industry, monthly, chained 2017 dollars
  1810000401 — CPI, all-items, monthly
  1410028701 — Labour Force Survey, monthly
  2010000801 — Retail Trade, monthly
  1210001101 — International merchandise trade (imports/exports)

StatCan CSV format notes:
  - First row is the header (after stripping BOM if present)
  - Standard columns: REF_DATE, GEO, DGUID, <subject>, UOM, UOM_ID,
    SCALAR_FACTOR, SCALAR_ID, VECTOR, FRAME, COORDINATE, VALUE,
    STATUS, SYMBOL, TERMINATED, DECIMALS
  - REF_DATE format: "YYYY-MM" for monthly, "YYYY" for annual
  - VALUE is numeric or suppressed ('..' / 'x' / 'F' / 'E')
  - The zip download contains two files: {pid}_en.csv and {pid}_MetaData_en.csv

Usage:
    source = StatCanSource()
    df = await source.run(table_pid="1810000401")
    # columns: ref_date, geo, sgc_code, value, vector, scalar_factor, uom
"""

from __future__ import annotations

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
from candata_shared.db import get_duckdb_connection
from candata_shared.geo import normalize_geo_column, normalize_statcan_geo
from candata_shared.time_utils import parse_statcan_date, parse_statcan_date_expr
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# Suppressed value markers used by StatCan
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})

# Known table file IDs (8-digit) with their indicator mappings
TABLE_INDICATOR_MAP: dict[str, str] = {
    "36100434": "gdp_monthly",
    "18100004": "cpi_monthly",
    "14100287": "unemployment_rate",   # also employment_monthly (different coord)
    "20100008": "retail_sales_monthly",
    "12100011": "trade_flows",         # handled separately by trade pipeline
}


class StatCanSource(BaseSource):
    """Downloads and parses Statistics Canada full-table CSV bundles."""

    name = "StatCan"

    def __init__(self, timeout: float = 120.0) -> None:
        super().__init__()
        self._base_url = settings.statcan_base_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # URL helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_table_id(table_pid: str) -> str:
        """
        Convert a StatCan PID to an 8-digit table file ID.

        StatCan PIDs come in two forms:
          - 10-digit: "3610043401" (full PID including revision suffix)
          - 8-digit:  "36100434" (table file ID, no revision suffix)
          - dashed:   "36-10-0434-01" (canonical dash form)

        The bulk CSV download URL uses only the 8-digit form.
        """
        pid_nodash = table_pid.replace("-", "")
        return pid_nodash[:8]

    def _csv_zip_url(self, table_pid: str) -> str:
        """Return the bulk CSV download URL for a StatCan table.

        Format: /n1/tbl/csv/{table_id}-eng.zip
        where table_id is the 8-digit form (e.g. 36100434).
        """
        table_id = self._to_table_id(table_pid)
        return f"{self._base_url}/n1/tbl/csv/{table_id}-eng.zip"

    def _metadata_url(self, table_pid: str) -> str:
        """WDS endpoint for table metadata (JSON format)."""
        table_id = self._to_table_id(table_pid)
        return f"{self._base_url}/t1/tbl1/en/dtbl!downloadTbl/metadataDownload/{table_id}"

    # ------------------------------------------------------------------
    # DuckDB caching
    # ------------------------------------------------------------------

    def _staging_table(self, table_pid: str) -> str:
        table_id = self._to_table_id(table_pid)
        return f"statcan_raw_{table_id}"

    def _is_cached(self, table_pid: str, max_age_hours: int = 24) -> bool:
        """Return True if a recent raw download exists in DuckDB staging."""
        try:
            duck = get_duckdb_connection()
            staging = self._staging_table(table_pid)
            result = duck.execute(
                f"""
                SELECT 1 FROM information_schema.tables
                WHERE table_name = ?
                """,
                [staging],
            ).fetchone()
            if result is None:
                return False

            # Check freshness via a candata_cache_meta table if it exists
            try:
                row = duck.execute(
                    "SELECT downloaded_at FROM candata_cache_meta WHERE table_name = ?",
                    [staging],
                ).fetchone()
                if row:
                    import datetime
                    age = datetime.datetime.now(datetime.UTC) - row[0].replace(
                        tzinfo=datetime.UTC
                    )
                    return age.total_seconds() < max_age_hours * 3600
            except Exception:
                pass
            return True
        except Exception:
            return False

    def _cache_to_duckdb(self, table_pid: str, df: pl.DataFrame) -> None:
        """Write raw DataFrame to DuckDB and record download timestamp."""
        try:
            duck = get_duckdb_connection()
            staging = self._staging_table(table_pid)

            # Create cache meta table if absent
            duck.execute(
                """
                CREATE TABLE IF NOT EXISTS candata_cache_meta (
                    table_name TEXT PRIMARY KEY,
                    downloaded_at TIMESTAMPTZ DEFAULT NOW()
                )
                """
            )

            # Write or replace staging table — register the polars DataFrame
            # directly (DuckDB supports polars natively) to avoid creating an
            # intermediate Arrow copy.
            duck.register("_tmp_statcan", df)
            duck.execute(f"CREATE OR REPLACE TABLE {staging} AS SELECT * FROM _tmp_statcan")
            duck.unregister("_tmp_statcan")

            duck.execute(
                """
                INSERT INTO candata_cache_meta (table_name, downloaded_at)
                VALUES (?, NOW())
                ON CONFLICT (table_name) DO UPDATE SET downloaded_at = NOW()
                """,
                [staging],
            )
            self._log.debug("duckdb_cached", table=staging, rows=len(df))
        except Exception as exc:
            self._log.warning("duckdb_cache_failed", error=str(exc))

    def _load_from_cache(self, table_pid: str) -> pl.DataFrame | None:
        """Load a previously cached DataFrame from DuckDB."""
        try:
            duck = get_duckdb_connection()
            staging = self._staging_table(table_pid)
            arrow = duck.execute(f"SELECT * FROM {staging}").arrow()
            df = pl.from_arrow(arrow)
            self._log.info("loaded_from_cache", table=staging, rows=len(df))
            return df
        except Exception:
            return None

    # ------------------------------------------------------------------
    # HTTP fetch
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_csv_zip(self, table_pid: str) -> Path:
        """Download a StatCan table ZIP to a temp file and return its path.

        Streams the response to disk instead of buffering the entire
        ZIP in memory, which avoids holding hundreds of MB of raw bytes
        alongside the parsed DataFrame.
        """
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
    def _parse_csv_zip(
        zip_path: Path,
        table_pid: str,
        *,
        columns: list[str] | None = None,
    ) -> pl.DataFrame:
        """
        Extract and parse the data CSV from a StatCan zip bundle on disk.

        Streams the CSV entry from the zip to a temp file so the full
        uncompressed content is never held in memory alongside the
        parsed DataFrame.

        Uses ``pl.scan_csv`` (lazy) so that column projection pushdown
        keeps only *columns* (if given) before materialising, which
        prevents OOM on very large tables like building permits.

        The zip contains:
          - {pid}_en.csv       — data (may be UTF-8 with BOM)
          - {pid}_MetaData_en.csv — metadata (skipped)
        """
        csv_tmp_path: Path | None = None
        try:
            with zipfile.ZipFile(zip_path) as zf:
                # Find the data file (not metadata)
                data_files = [
                    n for n in zf.namelist()
                    if n.endswith(".csv") and "MetaData" not in n
                ]
                if not data_files:
                    raise ValueError(f"No data CSV found in StatCan zip for pid={table_pid}")

                # Stream CSV from zip entry to a temp file instead of
                # loading the entire uncompressed content into memory.
                csv_fd = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
                csv_tmp_path = Path(csv_fd.name)
                with zf.open(data_files[0]) as src:
                    shutil.copyfileobj(src, csv_fd, length=256 * 1024)
                csv_fd.close()

            # --- Strip BOM from first line if present ---
            with open(csv_tmp_path, "rb") as f:
                head = f.read(3)
            if head == b"\xef\xbb\xbf":
                import mmap, os
                size = os.path.getsize(csv_tmp_path)
                with open(csv_tmp_path, "r+b") as f:
                    mm = mmap.mmap(f.fileno(), 0)
                    mm.move(0, 3, size - 3)
                    mm.flush()
                    mm.close()
                    f.truncate(size - 3)

            # Use scan_csv (lazy) so polars can push down column
            # selection before reading the full file into RAM.
            lf = pl.scan_csv(
                csv_tmp_path,
                infer_schema_length=0,
                null_values=list(_SUPPRESSED),
                truncate_ragged_lines=True,
            )

            # Strip BOM remnants from first column header
            first_col = lf.collect_schema().names()[0]
            if first_col.startswith("\ufeff"):
                lf = lf.rename({first_col: first_col.lstrip("\ufeff")})

            if columns:
                # Only keep requested columns (case-insensitive match).
                available = {c.upper(): c for c in lf.collect_schema().names()}
                selected = [available[c.upper()] for c in columns if c.upper() in available]
                if selected:
                    lf = lf.select(selected)

            df = lf.collect()
            return df
        finally:
            zip_path.unlink(missing_ok=True)
            if csv_tmp_path:
                csv_tmp_path.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(  # noqa: E501
        self,
        *,
        table_pid: str,
        use_cache: bool = True,
        columns: list[str] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download a StatCan full-table CSV (or return cached version).

        Args:
            table_pid:  StatCan product ID, e.g. "1810000401".
            use_cache:  If True, use DuckDB cache if recent enough.
            columns:    If provided, only materialise these columns
                        (case-insensitive). Drastically cuts memory for
                        large tables like building permits.

        Returns:
            Raw polars DataFrame with original StatCan column names.
        """
        if use_cache and self._is_cached(table_pid):
            cached = self._load_from_cache(table_pid)
            if cached is not None:
                if columns:
                    available = {c.upper(): c for c in cached.columns}
                    selected = [available[c.upper()] for c in columns if c.upper() in available]
                    if selected:
                        cached = cached.select(selected)
                return cached

        zip_path = await self._download_csv_zip(table_pid)
        df = self._parse_csv_zip(zip_path, table_pid, columns=columns)
        self._cache_to_duckdb(table_pid, df)
        return df

    def transform(
        self,
        raw: pl.DataFrame,
        *,
        value_filter: str | None = None,
        start_date: date | None = None,
    ) -> pl.DataFrame:
        """
        Normalize a raw StatCan CSV DataFrame.

        Output columns:
            ref_date     DATE       — first day of the reference period
            geo          String     — original GEO string
            sgc_code     String     — normalized SGC code ("35", "01", etc.)
            geo_level    String     — "pr", "country", "cma", "fsa"
            value        Float64    — numeric value (null if suppressed)
            vector       String     — StatCan series vector code (V-number)
            scalar_factor String   — "Units", "Thousands", etc.
            uom          String     — unit of measure

        Args:
            raw:          Raw DataFrame from extract().
            value_filter: If set, filter rows where a subject/topic column equals this.
            start_date:   Drop rows before this date (optional).

        Returns:
            Normalized polars DataFrame.
        """
        # Operate on the input directly — polars expressions return new
        # DataFrames so there is no need for an expensive .clone().
        df = raw.rename({col: col.strip().upper() for col in raw.columns})

        # Drop rows where REF_DATE is missing (footnote rows at end of file)
        if "REF_DATE" not in df.columns:
            raise ValueError(f"REF_DATE column not found. Columns: {df.columns}")

        df = df.filter(pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != ""))

        # Parse ref_date vectorially (handles YYYY-MM and YYYY-MM-DD
        # native in polars; rare formats fall back to Python).
        df = df.with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
        ).filter(pl.col("ref_date").is_not_null())

        # Filter by start_date
        if start_date:
            df = df.filter(pl.col("ref_date") >= start_date)

        # Parse VALUE column
        value_col = "VALUE" if "VALUE" in df.columns else None
        if value_col:
            df = df.with_columns(
                pl.col(value_col)
                .cast(pl.Float64, strict=False)
                .alias("value")
            )
        else:
            df = df.with_columns(pl.lit(None).cast(pl.Float64).alias("value"))

        # Normalize GEO → sgc_code + geo_level using a batch lookup
        # (resolves each unique GEO string once then joins back).
        geo_col = "GEO" if "GEO" in df.columns else None
        if geo_col:
            df = df.with_columns(pl.col(geo_col).alias("geo"))
            df = normalize_geo_column(df, geo_col)
        else:
            df = df.with_columns(
                pl.lit(None).cast(pl.String).alias("geo"),
                pl.lit(None).cast(pl.String).alias("sgc_code"),
                pl.lit(None).cast(pl.String).alias("geo_level"),
            )

        # Keep useful metadata columns
        keep = {"ref_date", "geo", "sgc_code", "geo_level", "value"}
        optional = {
            "VECTOR": "vector",
            "SCALAR_FACTOR": "scalar_factor",
            "UOM": "uom",
        }
        for src, dst in optional.items():
            if src in df.columns:
                df = df.with_columns(pl.col(src).alias(dst))
                keep.add(dst)

        # Carry subject/topic column if present (used for filtering)
        for col in df.columns:
            if col not in keep and col not in (
                "REF_DATE", "GEO", "DGUID", "VALUE", "STATUS",
                "SYMBOL", "TERMINATED", "DECIMALS", "UOM_ID",
                "SCALAR_ID", "FRAME", "COORDINATE",
            ):
                keep.add(col)

        select = [c for c in keep if c in df.columns]
        df = df.select(select)

        self._log.debug(
            "transform_stats",
            output_rows=len(df),
            null_values=df["value"].null_count() if "value" in df.columns else 0,
            unmapped_geos=(
                df["sgc_code"].null_count() if "sgc_code" in df.columns else 0
            ),
        )
        return df

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "base_url": self._base_url,
            "description": "Statistics Canada Web Data Service — full-table CSV downloads",
            "supported_tables": list(TABLE_INDICATOR_MAP.keys()),
        }
