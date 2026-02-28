"""
pipelines/housing_enrichment.py — Three additional housing data sources.

Sources:
  1. NHPI (New Housing Price Index) — StatCan 18-10-0205-01
  2. Building Permits — StatCan 34-10-0066-01
  3. Teranet-National Bank House Price Index — housepriceindex.ca

Each source has an independent ingestion function. The ``run`` function
orchestrates one, several, or all sources and returns per-table LoadResults.

Uses polars lazy scanning with streaming collection and column pushdown
to process multi-GB StatCan CSVs without loading them entirely into memory.

Usage:
    from candata_pipeline.pipelines.housing_enrichment import run
    results = await run(source="all", dry_run=False)
    results = await run(source="nhpi")
    results = await run(source="teranet", dry_run=True)
"""

from __future__ import annotations

import io
import shutil
import zipfile
from datetime import date
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.utils.large_file import (
    check_available_memory,
    estimate_csv_rows,
    get_or_download,
    monitor_memory,
)
from candata_pipeline.utils.logging import configure_logging
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# StatCan bulk CSV URLs (8-digit table IDs)
_NHPI_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/18100205-eng.zip"
_PERMITS_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/34100066-eng.zip"

# Teranet public CSV
_TERANET_CSV_URL = "https://housepriceindex.ca/wp-content/uploads/hpi_download.csv"

# Cache settings
CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "cache"
CACHE_MAX_AGE_HOURS = 168  # weekly

# StatCan suppressed markers (same set as statcan.py)
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _extract_csv_from_zip(zip_path: Path, dest_csv: Path) -> Path:
    """Extract the data CSV (not MetaData) from a StatCan ZIP."""
    dest_csv.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        data_files = [
            n for n in zf.namelist()
            if n.endswith(".csv") and "MetaData" not in n
        ]
        if not data_files:
            raise ValueError("No data CSV found in ZIP")

        with zf.open(data_files[0]) as src, open(dest_csv, "wb") as dst:
            shutil.copyfileobj(src, dst, length=256 * 1024)

    # Strip BOM if present — streaming approach (no full-file read)
    with open(dest_csv, "rb") as f:
        head = f.read(3)
    if head == b"\xef\xbb\xbf":
        tmp = dest_csv.with_suffix(".nobom")
        with open(dest_csv, "rb") as src, open(tmp, "wb") as dst:
            src.seek(3)
            shutil.copyfileobj(src, dst, length=256 * 1024)
        tmp.replace(dest_csv)

    return dest_csv


def _strip_bom(raw: bytes) -> bytes:
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:]
    return raw


def _parse_ref_date(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Split REF_DATE 'YYYY-MM' into integer year and month columns (polars lazy)."""
    return lf.with_columns(
        pl.col("REF_DATE").str.strip_chars().str.slice(0, 4).cast(pl.Int32).alias("year"),
        pl.col("REF_DATE").str.strip_chars().str.slice(5, 2).cast(pl.Int32).alias("month"),
    )


def _find_column(columns: list[str], *patterns: str) -> str | None:
    """Find the first column whose uppercased name contains any of the patterns."""
    for c in columns:
        cu = c.upper()
        for p in patterns:
            if p in cu:
                return c
    return None


# ------------------------------------------------------------------
# Source 1: NHPI (polars lazy)
# ------------------------------------------------------------------


@monitor_memory
async def ingest_nhpi(
    *,
    dry_run: bool = False,
    start_date: date | None = None,
) -> LoadResult:
    """
    Ingest New Housing Price Index (StatCan 18-10-0205-01) using polars lazy scanning.
    """
    log.info("nhpi_start")
    check_available_memory(required_gb=1.0)

    table_name = "nhpi"
    conflict_columns = ["cma_name", "year", "month", "house_type", "index_component"]

    # Download and extract
    zip_path = get_or_download(
        _NHPI_CSV_URL, CACHE_DIR, "18100205-eng.zip",
        max_age_hours=CACHE_MAX_AGE_HOURS,
    )
    csv_path = CACHE_DIR / "18100205-eng.csv"
    if not csv_path.exists() or csv_path.stat().st_mtime < zip_path.stat().st_mtime:
        print("Extracting NHPI CSV from ZIP...")
        csv_path = _extract_csv_from_zip(zip_path, csv_path)

    estimate_csv_rows(csv_path)

    # Scan lazily — all strings, let us handle types
    lf = pl.scan_csv(
        csv_path,
        infer_schema_length=0,
        null_values=list(_SUPPRESSED),
        truncate_ragged_lines=True,
    )

    all_cols = lf.collect_schema().names()

    # Identify the component and house_type columns (names vary between revisions)
    component_col = _find_column(all_cols, "COMPONENTS OF NEW HOUSING", "NEW HOUSING PRICE INDEX")
    house_type_col = _find_column(all_cols, "TYPE OF HOUSE")

    if not component_col:
        log.error("nhpi_missing_component_column", columns=all_cols)
        return LoadResult(table=table_name)

    # Select only needed columns
    select_cols = ["REF_DATE", "GEO", "VALUE"]
    if component_col:
        select_cols.append(component_col)
    if house_type_col:
        select_cols.append(house_type_col)
    # Only keep columns that exist
    select_cols = [c for c in select_cols if c in all_cols]

    lf = lf.select(select_cols)

    # Filter out rows with missing REF_DATE or VALUE
    lf = lf.filter(
        pl.col("REF_DATE").is_not_null()
        & (pl.col("REF_DATE") != "")
        & pl.col("VALUE").is_not_null()
    )

    # Parse dates
    lf = _parse_ref_date(lf)

    # Date filter predicate pushdown
    if start_date:
        lf = lf.filter(
            (pl.col("year") > start_date.year)
            | (
                (pl.col("year") == start_date.year)
                & (pl.col("month") >= start_date.month)
            )
        )

    # Build output columns
    exprs = [
        pl.col("GEO").str.strip_chars().alias("cma_name"),
        pl.col("year"),
        pl.col("month"),
        pl.col("VALUE").cast(pl.Float64, strict=False).alias("index_value"),
        pl.col(component_col).str.strip_chars().alias("index_component"),
    ]
    if house_type_col:
        exprs.append(pl.col(house_type_col).str.strip_chars().alias("house_type"))
    else:
        exprs.append(pl.lit("Total").alias("house_type"))

    lf = lf.select(exprs)

    # Drop rows where VALUE couldn't be parsed as float
    lf = lf.filter(pl.col("index_value").is_not_null())

    # Final column order
    lf = lf.select("cma_name", "year", "month", "house_type", "index_component", "index_value")

    # Collect with streaming to bound memory
    print("Collecting NHPI data (streaming)...")
    df = lf.collect(streaming=True)
    print(f"NHPI: {len(df):,} rows after filtering")

    if dry_run:
        _print_sample_polars("nhpi", df)
        return LoadResult(table=table_name, records_loaded=len(df))

    loader = SupabaseLoader()
    return await loader.upsert(
        table=table_name,
        df=df,
        conflict_columns=conflict_columns,
    )


# ------------------------------------------------------------------
# Source 2: Building Permits (polars lazy + year partitioning)
# ------------------------------------------------------------------

@monitor_memory
async def ingest_permits(
    *,
    dry_run: bool = False,
    start_date: date | None = None,
) -> LoadResult:
    """
    Ingest building permits by municipality (StatCan 34-10-0066-01).

    Processes one year at a time to bound peak memory usage on the large table.
    """
    log.info("permits_start")
    check_available_memory(required_gb=1.0)

    table_name = "building_permits"
    conflict_columns = ["dguid", "year", "month", "structure_type", "work_type"]

    # Download and extract
    zip_path = get_or_download(
        _PERMITS_CSV_URL, CACHE_DIR, "34100066-eng.zip",
        max_age_hours=CACHE_MAX_AGE_HOURS,
    )
    csv_path = CACHE_DIR / "34100066-eng.csv"
    if not csv_path.exists() or csv_path.stat().st_mtime < zip_path.stat().st_mtime:
        print("Extracting Building Permits CSV from ZIP...")
        csv_path = _extract_csv_from_zip(zip_path, csv_path)

    estimate_csv_rows(csv_path)

    # Scan lazily
    lf_base = pl.scan_csv(
        csv_path,
        infer_schema_length=0,
        null_values=list(_SUPPRESSED),
        truncate_ragged_lines=True,
    )

    all_cols = lf_base.collect_schema().names()

    # Identify structure and work type columns
    structure_col = _find_column(all_cols, "TYPE OF STRUCTURE")
    work_col = _find_column(all_cols, "TYPE OF WORK")

    if not structure_col or not work_col:
        log.error("permits_missing_columns", columns=all_cols)
        return LoadResult(table=table_name)

    # Select only needed columns
    select_cols = ["REF_DATE", "GEO", "VALUE"]
    if "DGUID" in all_cols:
        select_cols.append("DGUID")
    select_cols.extend([structure_col, work_col])
    select_cols = [c for c in select_cols if c in all_cols]

    lf_base = lf_base.select(select_cols)

    # Filter valid rows
    lf_base = lf_base.filter(
        pl.col("REF_DATE").is_not_null()
        & (pl.col("REF_DATE") != "")
        & pl.col("VALUE").is_not_null()
    )

    # Parse dates on base lazy frame
    lf_base = _parse_ref_date(lf_base)

    # Date filter
    if start_date:
        lf_base = lf_base.filter(
            (pl.col("year") > start_date.year)
            | (
                (pl.col("year") == start_date.year)
                & (pl.col("month") >= start_date.month)
            )
        )

    # Discover unique years (lightweight scan — only REF_DATE column)
    print("Scanning for unique years in permits data...")
    years_df = (
        pl.scan_csv(csv_path, infer_schema_length=0, truncate_ragged_lines=True)
        .select(pl.col("REF_DATE").str.strip_chars().str.slice(0, 4))
        .unique()
        .collect(streaming=True)
    )
    years = sorted(
        y for y in years_df["REF_DATE"].to_list()
        if y is not None and y.isdigit()
    )
    print(f"Found {len(years)} years: {years[0]}–{years[-1]}" if years else "No years found")

    has_dguid = "DGUID" in all_cols

    # Build output expressions (reused per year)
    def _build_output_exprs() -> list[pl.Expr]:
        exprs = [
            pl.col("GEO").str.strip_chars().alias("municipality_name"),
            (pl.col("DGUID").str.strip_chars() if has_dguid else pl.lit("")).alias("dguid"),
            pl.col("year"),
            pl.col("month"),
            pl.col(structure_col).str.strip_chars().alias("structure_type"),
            pl.col(work_col).str.strip_chars().alias("work_type"),
            pl.col("VALUE").cast(pl.Float64, strict=False).alias("permits_value_cad_thousands"),
        ]
        return exprs

    total_loaded = 0
    total_failed = 0
    loader = None if dry_run else SupabaseLoader()

    for year_str in years:
        year_int = int(year_str)

        # Skip years before start_date
        if start_date and year_int < start_date.year:
            continue

        # Filter to this year, collect with streaming
        lf_year = lf_base.filter(pl.col("year") == year_int)
        lf_year = lf_year.select(_build_output_exprs())
        lf_year = lf_year.filter(pl.col("permits_value_cad_thousands").is_not_null())
        lf_year = lf_year.filter(
            pl.col("dguid").is_not_null() & (pl.col("dguid") != "")
        )
        lf_year = lf_year.select(
            "municipality_name", "dguid", "year", "month",
            "structure_type", "work_type", "permits_value_cad_thousands",
        )

        df_year = lf_year.collect(streaming=True)

        # Deduplicate on conflict key — StatCan CSVs can contain
        # duplicate rows after stripping whitespace, and PostgREST
        # rejects batches where the same key appears twice.
        df_year = df_year.unique(
            subset=conflict_columns, keep="last",
        )

        if df_year.is_empty():
            continue

        print(f"  Permits {year_str}: {len(df_year):,} rows")

        if dry_run:
            if total_loaded == 0:
                _print_sample_polars("building_permits", df_year)
            total_loaded += len(df_year)
        else:
            result = await loader.upsert(
                table=table_name,
                df=df_year,
                conflict_columns=conflict_columns,
            )
            total_loaded += result.records_loaded
            total_failed += result.records_failed

        # df_year goes out of scope here — memory freed before next year

    log.info("permits_complete", rows=total_loaded, errors=total_failed)
    return LoadResult(table=table_name, records_loaded=total_loaded, records_failed=total_failed)


# ------------------------------------------------------------------
# Source 3: Teranet HPI
# ------------------------------------------------------------------


@with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
async def _download_teranet_csv() -> bytes:
    """Download the Teranet public CSV with retry."""
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        resp = await client.get(_TERANET_CSV_URL)
        resp.raise_for_status()
        return resp.content


async def ingest_teranet(
    *,
    dry_run: bool = False,
    start_date: date | None = None,
) -> LoadResult:
    """
    Ingest Teranet-National Bank House Price Index.

    Downloads the public CSV from housepriceindex.ca. The file typically
    has columns: Date, plus one column per market (e.g. Calgary, Montreal,
    Toronto, …). Each row is a month; values are composite HPI.

    If the download fails, logs a warning and returns an empty result
    rather than crashing the pipeline.
    """
    log.info("teranet_start")

    try:
        raw_bytes = await _download_teranet_csv()
    except Exception as exc:
        log.warning(
            "teranet_download_failed",
            error=str(exc),
            hint="Teranet URL may be inaccessible; skipping source.",
        )
        return LoadResult(table="teranet_hpi")

    raw_bytes = _strip_bom(raw_bytes)

    try:
        df = pl.read_csv(
            io.BytesIO(raw_bytes),
            infer_schema_length=0,
            truncate_ragged_lines=True,
        )
    except Exception as exc:
        log.warning("teranet_parse_failed", error=str(exc))
        return LoadResult(table="teranet_hpi")

    if df.is_empty():
        log.warning("teranet_empty")
        return LoadResult(table="teranet_hpi")

    # The first column is typically "Date" (or similar). Identify it.
    date_col = df.columns[0]
    market_cols = [c for c in df.columns if c != date_col]

    if not market_cols:
        log.warning("teranet_no_markets", columns=df.columns)
        return LoadResult(table="teranet_hpi")

    # Melt wide → long: one row per (date, market)
    df_long = df.unpivot(
        index=date_col,
        on=market_cols,
        variable_name="market_name",
        value_name="hpi_value_str",
    )

    # Parse the date column — try common formats
    df_long = df_long.with_columns(
        pl.col(date_col).alias("_raw_date"),
    )

    df_long = _parse_teranet_date(df_long, date_col)

    # Cast HPI value
    df_long = df_long.with_columns(
        pl.col("hpi_value_str").cast(pl.Float64, strict=False).alias("hpi_value"),
    )

    df_long = df_long.filter(
        pl.col("hpi_value").is_not_null()
        & pl.col("year").is_not_null()
        & pl.col("month").is_not_null()
    )

    if start_date:
        df_long = df_long.filter(
            (pl.col("year") > start_date.year)
            | (
                (pl.col("year") == start_date.year)
                & (pl.col("month") >= start_date.month)
            )
        )

    df_long = df_long.with_columns(
        pl.col("market_name").str.strip_chars(),
    )

    df_out = df_long.select("market_name", "year", "month", "hpi_value")

    log.info("teranet_transformed", rows=len(df_out), markets=len(market_cols))

    if dry_run:
        _print_sample_polars("teranet_hpi", df_out)
        return LoadResult(table="teranet_hpi", records_loaded=len(df_out))

    loader = SupabaseLoader()
    return await loader.upsert(
        table="teranet_hpi",
        df=df_out,
        conflict_columns=["market_name", "year", "month"],
    )


def _parse_teranet_date(df: pl.DataFrame, date_col: str) -> pl.DataFrame:
    """
    Parse various date formats from the Teranet CSV into year/month ints.

    Handles:
      - "YYYY-MM" or "YYYY-MM-DD"
      - "MM/YYYY" or "M/YYYY"
      - "Jan-2000", "January 2000"
    """
    c = pl.col(date_col).str.strip_chars()

    # Try YYYY-MM or YYYY-MM-DD first (vectorized)
    year_ym = c.str.extract(r"^(\d{4})-(\d{1,2})", 1).cast(pl.Int32, strict=False)
    month_ym = c.str.extract(r"^(\d{4})-(\d{1,2})", 2).cast(pl.Int32, strict=False)

    # Try MM/YYYY
    year_slash = c.str.extract(r"^(\d{1,2})/(\d{4})$", 2).cast(pl.Int32, strict=False)
    month_slash = c.str.extract(r"^(\d{1,2})/(\d{4})$", 1).cast(pl.Int32, strict=False)

    # Build month-name lookup for Mon-YYYY / Month YYYY patterns
    _MONTHS = {
        "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
        "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
        "january": 1, "february": 2, "march": 3, "april": 4,
        "june": 6, "july": 7, "august": 8, "september": 9,
        "october": 10, "november": 11, "december": 12,
    }
    year_text = c.str.extract(r"^[A-Za-z]+[\s\-]+(\d{4})$", 1).cast(pl.Int32, strict=False)
    month_name_raw = c.str.extract(r"^([A-Za-z]+)[\s\-]+\d{4}$", 1).str.to_lowercase()

    # Resolve month names via a small join instead of row-by-row Python
    unique_names = df.select(
        c.str.extract(r"^([A-Za-z]+)[\s\-]+\d{4}$", 1)
        .str.to_lowercase()
        .unique()
        .drop_nulls()
        .alias("_mn")
    ).to_series().to_list()
    name_lookup = {n: _MONTHS.get(n) for n in unique_names if n in _MONTHS}

    # Build a replace expression for known month names
    month_text = month_name_raw
    for name, num in name_lookup.items():
        month_text = month_text.replace(name, str(num))
    month_text = month_text.cast(pl.Int32, strict=False)

    return df.with_columns(
        pl.coalesce([year_ym, year_slash, year_text]).alias("year"),
        pl.coalesce([month_ym, month_slash, month_text]).alias("month"),
    )


# ------------------------------------------------------------------
# Dry-run helpers
# ------------------------------------------------------------------


def _print_sample_polars(table: str, df: pl.DataFrame, n: int = 5) -> None:
    """Print a summary and first N rows for dry-run inspection (polars)."""
    print(f"\n[dry-run] {table}: {len(df)} rows")
    if not df.is_empty():
        print(df.head(n))


# ------------------------------------------------------------------
# Orchestrator
# ------------------------------------------------------------------


async def run(
    *,
    source: str = "all",
    dry_run: bool = False,
    start_date: date | None = None,
) -> dict[str, LoadResult]:
    """
    Run one or all housing enrichment pipelines.

    Args:
        source:     "nhpi", "permits", "teranet", or "all".
        dry_run:    Transform only, do not write to Supabase.
        start_date: Earliest reference date to include.

    Returns:
        Dict of {table_name → LoadResult}.
    """
    configure_logging()
    log.info("housing_enrichment_start", source=source, dry_run=dry_run)

    results: dict[str, LoadResult] = {}
    kwargs: dict[str, Any] = {"dry_run": dry_run, "start_date": start_date}

    runners: list[tuple[str, str]] = []
    if source in ("nhpi", "all"):
        runners.append(("nhpi", "nhpi"))
    if source in ("permits", "all"):
        runners.append(("building_permits", "permits"))
    if source in ("teranet", "all"):
        runners.append(("teranet_hpi", "teranet"))

    ingest_map = {
        "nhpi": ingest_nhpi,
        "permits": ingest_permits,
        "teranet": ingest_teranet,
    }

    connection_dead = False

    for table_name, source_key in runners:
        if connection_dead:
            log.warning(
                "source_skipped_connection_dead",
                table=table_name,
                reason="Previous source failed due to connection error; skipping remaining sources.",
            )
            results[table_name] = LoadResult(table=table_name)
            continue

        try:
            result = await ingest_map[source_key](**kwargs)
            results[table_name] = result
            log.info(
                "source_complete",
                table=table_name,
                records_loaded=result.records_loaded,
                status=result.status,
            )
            # If every record failed, check whether it looks like a connection issue
            if (
                result.records_loaded == 0
                and result.records_failed > 0
                and any("Connection refused" in e for e in result.errors)
            ):
                connection_dead = True
                log.error(
                    "connection_dead_detected",
                    table=table_name,
                    hint="All records failed with connection errors; aborting remaining sources.",
                )
        except Exception as exc:
            log.error("source_failed", table=table_name, error=str(exc), exc_info=True)
            results[table_name] = LoadResult(table=table_name)

    # Summary
    total = sum(r.records_loaded for r in results.values())
    failed = sum(r.records_failed for r in results.values())
    print(f"\nHousing enrichment complete: {total} records loaded, {failed} failed.")
    for t, r in results.items():
        print(f"  {t}: {r.records_loaded} loaded, {r.records_failed} failed")

    return results
