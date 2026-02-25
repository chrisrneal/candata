"""
pipelines/housing_enrichment.py — Three additional housing data sources.

Sources:
  1. NHPI (New Housing Price Index) — StatCan 18-10-0205-01
  2. Building Permits — StatCan 34-10-0066-01
  3. Teranet-National Bank House Price Index — housepriceindex.ca

Each source has an independent ingestion function. The ``run`` function
orchestrates one, several, or all sources and returns per-table LoadResults.

Usage:
    from candata_pipeline.pipelines.housing_enrichment import run
    results = await run(source="all", dry_run=False)
    results = await run(source="nhpi")
    results = await run(source="teranet", dry_run=True)
"""

from __future__ import annotations

import io
import zipfile
from datetime import date

import httpx
import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.statcan import StatCanSource
from candata_pipeline.utils.logging import configure_logging
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# StatCan PIDs
_NHPI_PID = "1810020501"
_PERMITS_PID = "3410006601"

# Teranet public CSV
_TERANET_CSV_URL = "https://housepriceindex.ca/wp-content/uploads/hpi_download.csv"

# StatCan suppressed markers (same set as statcan.py)
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------


def _parse_ref_date(df: pl.DataFrame) -> pl.DataFrame:
    """Split REF_DATE 'YYYY-MM' into integer year and month columns."""
    return df.with_columns(
        pl.col("REF_DATE").str.slice(0, 4).cast(pl.Int32).alias("year"),
        pl.col("REF_DATE").str.slice(5, 2).cast(pl.Int32).alias("month"),
    )


def _strip_bom(raw: bytes) -> bytes:
    if raw.startswith(b"\xef\xbb\xbf"):
        return raw[3:]
    return raw


# ------------------------------------------------------------------
# Source 1: NHPI
# ------------------------------------------------------------------


async def ingest_nhpi(
    *,
    dry_run: bool = False,
    start_date: date | None = None,
) -> LoadResult:
    """
    Ingest New Housing Price Index (StatCan 18-10-0205-01).

    Parses CMA-level monthly index values for Total/Detached house types
    and Total/Land/Building index components. Base period 2017=100.
    """
    log.info("nhpi_start")

    source = StatCanSource(timeout=180.0)
    raw = await source.extract(table_pid=_NHPI_PID)

    # Uppercase column names for consistent access
    raw = raw.rename({c: c.strip().upper() for c in raw.columns})

    # Filter to rows with valid REF_DATE and VALUE
    df = raw.filter(
        pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
    )

    df = _parse_ref_date(df)

    if start_date:
        df = df.filter(
            (pl.col("year") > start_date.year)
            | (
                (pl.col("year") == start_date.year)
                & (pl.col("month") >= start_date.month)
            )
        )

    # Identify the subject columns (names vary by table revision)
    # Expected: "Type of house" and "Components of new housing price indexes"
    house_type_col = _find_column(df, ["TYPE OF HOUSE"])
    component_col = _find_column(
        df, ["COMPONENTS OF NEW HOUSING PRICE INDEXES", "NEW HOUSING PRICE INDEXES"]
    )

    if not house_type_col or not component_col:
        log.error(
            "nhpi_missing_columns",
            columns=df.columns,
            house_type_col=house_type_col,
            component_col=component_col,
        )
        return LoadResult(table="nhpi")

    df = df.with_columns(
        pl.col("GEO").str.strip_chars().alias("cma_name"),
        pl.col(house_type_col).str.strip_chars().alias("house_type"),
        pl.col(component_col).str.strip_chars().alias("index_component"),
        pl.col("VALUE").cast(pl.Float64, strict=False).alias("index_value"),
    )

    # Keep only rows with a numeric value
    df = df.filter(pl.col("index_value").is_not_null())

    df = df.select("cma_name", "year", "month", "house_type", "index_component", "index_value")

    log.info("nhpi_transformed", rows=len(df))

    if dry_run:
        _print_sample("nhpi", df)
        return LoadResult(table="nhpi", records_loaded=len(df))

    loader = SupabaseLoader()
    return await loader.upsert(
        table="nhpi",
        df=df,
        conflict_columns=["cma_name", "year", "month", "house_type", "index_component"],
    )


# ------------------------------------------------------------------
# Source 2: Building Permits
# ------------------------------------------------------------------


async def ingest_permits(
    *,
    dry_run: bool = False,
    start_date: date | None = None,
) -> LoadResult:
    """
    Ingest building permits by municipality (StatCan 34-10-0066-01).

    Monthly data by type of structure and type of work. Values are in
    thousands of Canadian dollars.
    """
    log.info("permits_start")

    source = StatCanSource(timeout=300.0)  # large table
    raw = await source.extract(table_pid=_PERMITS_PID)

    raw = raw.rename({c: c.strip().upper() for c in raw.columns})

    df = raw.filter(
        pl.col("REF_DATE").is_not_null() & (pl.col("REF_DATE") != "")
    )

    df = _parse_ref_date(df)

    if start_date:
        df = df.filter(
            (pl.col("year") > start_date.year)
            | (
                (pl.col("year") == start_date.year)
                & (pl.col("month") >= start_date.month)
            )
        )

    structure_col = _find_column(df, ["TYPE OF STRUCTURE"])
    work_col = _find_column(df, ["TYPE OF WORK"])

    if not structure_col or not work_col:
        log.error(
            "permits_missing_columns",
            columns=df.columns,
            structure_col=structure_col,
            work_col=work_col,
        )
        return LoadResult(table="building_permits")

    dguid_col = "DGUID" if "DGUID" in df.columns else None

    df = df.with_columns(
        pl.col("GEO").str.strip_chars().alias("municipality_name"),
        (pl.col(dguid_col).str.strip_chars() if dguid_col else pl.lit("")).alias("dguid"),
        pl.col(structure_col).str.strip_chars().alias("structure_type"),
        pl.col(work_col).str.strip_chars().alias("work_type"),
        pl.col("VALUE").cast(pl.Float64, strict=False).alias("permits_value_cad_thousands"),
    )

    df = df.filter(pl.col("permits_value_cad_thousands").is_not_null())

    df = df.select(
        "municipality_name", "dguid", "year", "month",
        "structure_type", "work_type", "permits_value_cad_thousands",
    )

    log.info("permits_transformed", rows=len(df))

    if dry_run:
        _print_sample("building_permits", df)
        return LoadResult(table="building_permits", records_loaded=len(df))

    loader = SupabaseLoader()
    return await loader.upsert(
        table="building_permits",
        df=df,
        conflict_columns=["dguid", "year", "month", "structure_type", "work_type"],
    )


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
    # Teranet uses formats like "MM/YYYY", "YYYY-MM", "Jan-2000", etc.
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
        _print_sample("teranet_hpi", df_out)
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
    import re

    def _extract_ym(raw: str | None) -> tuple[int | None, int | None]:
        if not raw:
            return (None, None)
        raw = raw.strip()
        # YYYY-MM or YYYY-MM-DD
        m = re.match(r"^(\d{4})-(\d{1,2})", raw)
        if m:
            return (int(m.group(1)), int(m.group(2)))
        # MM/YYYY
        m = re.match(r"^(\d{1,2})/(\d{4})$", raw)
        if m:
            return (int(m.group(2)), int(m.group(1)))
        # Mon-YYYY or Month YYYY
        _MONTHS = {
            "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
            "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
            "january": 1, "february": 2, "march": 3, "april": 4,
            "june": 6, "july": 7, "august": 8, "september": 9,
            "october": 10, "november": 11, "december": 12,
        }
        m = re.match(r"^([A-Za-z]+)[\s\-]+(\d{4})$", raw)
        if m:
            mon = _MONTHS.get(m.group(1).lower())
            if mon:
                return (int(m.group(2)), mon)
        return (None, None)

    years: list[int | None] = []
    months: list[int | None] = []
    for val in df[date_col].to_list():
        y, m = _extract_ym(val)
        years.append(y)
        months.append(m)

    return df.with_columns(
        pl.Series("year", years, dtype=pl.Int32),
        pl.Series("month", months, dtype=pl.Int32),
    )


# ------------------------------------------------------------------
# Column-finding helper
# ------------------------------------------------------------------


def _find_column(df: pl.DataFrame, candidates: list[str]) -> str | None:
    """
    Find a column in the DataFrame whose uppercased name contains one
    of the candidate strings.
    """
    upper_cols = {c.upper(): c for c in df.columns}
    for candidate in candidates:
        # Exact match first
        if candidate in upper_cols:
            return upper_cols[candidate]
        # Substring match
        for uc, original in upper_cols.items():
            if candidate in uc:
                return original
    return None


# ------------------------------------------------------------------
# Dry-run helper
# ------------------------------------------------------------------


def _print_sample(table: str, df: pl.DataFrame, n: int = 5) -> None:
    """Print a summary and first N rows for dry-run inspection."""
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
    kwargs = {"dry_run": dry_run, "start_date": start_date}

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

    for table_name, source_key in runners:
        try:
            result = await ingest_map[source_key](**kwargs)
            results[table_name] = result
            log.info(
                "source_complete",
                table=table_name,
                records_loaded=result.records_loaded,
                status=result.status,
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
