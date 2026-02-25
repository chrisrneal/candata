"""
pipelines/statcan_trade_hs6.py — Canadian trade data at NAPCS / HS6 product level.

Ingests StatCan Table 12-10-0119-01 (Canadian International Merchandise Trade
by NAPCS), transforms to a clean trade_flows_hs6 schema, optionally joins an
NAPCS→HS6 concordance table, and upserts to Supabase.

Usage (as module):
    from candata_pipeline.pipelines.statcan_trade_hs6 import run
    result = await run(from_year=2022, dry_run=True)

CLI (via run_pipeline.py):
    python scripts/run_pipeline.py trade-hs6 --from-year 2022 --dry-run
"""

from __future__ import annotations

import io
import re
import shutil
import tempfile
import zipfile
from datetime import date, datetime
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.utils.logging import configure_logging, get_logger
from candata_pipeline.utils.retry import with_retry

log = get_logger(__name__, pipeline="trade_hs6")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

TABLE_PID = "12100119"  # StatCan table 12-10-0119-01
BULK_CSV_URL = (
    "https://www150.statcan.gc.ca/n1/tbl/csv/12100119-eng.zip"
)

# Concordance CSV (NAPCS → HS6) — fallback if table only has NAPCS codes
CONCORDANCE_URL = (
    "https://www.statcan.gc.ca/eng/statistical-programs/document/2646_D51_T9"
)

TABLE_NAME = "trade_flows_hs6"

CONFLICT_COLUMNS = [
    "ref_year", "ref_month", "province", "trade_flow",
    "partner_country", "napcs_code",
]

# StatCan suppressed / missing markers
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})

# Pattern to extract NAPCS code from description field.
# StatCan format: "Description text [C21]"  → "C21"
#                 "Total of all merchandise" → None (no code)
_CODE_RE = re.compile(r"\[([A-Z0-9][A-Z0-9.]*)\]\s*$", re.IGNORECASE)


# ---------------------------------------------------------------------------
# Download helpers
# ---------------------------------------------------------------------------

@with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
async def _download_zip(url: str, timeout: float = 300.0) -> Path:
    """Download a URL to a temp file and return its path."""
    log.info("downloading", url=url)
    tmp = tempfile.NamedTemporaryFile(suffix=".zip", delete=False)
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as client:
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                async for chunk in resp.aiter_bytes(chunk_size=256 * 1024):
                    tmp.write(chunk)
        tmp.close()
        return Path(tmp.name)
    except Exception:
        tmp.close()
        Path(tmp.name).unlink(missing_ok=True)
        raise


def _parse_csv_from_zip(zip_path: Path) -> pl.DataFrame:
    """Extract the data CSV (not MetaData) from a StatCan ZIP on disk.

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
                raise ValueError("No data CSV found in ZIP")

            csv_fd = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
            csv_tmp_path = Path(csv_fd.name)
            with zf.open(data_files[0]) as src:
                shutil.copyfileobj(src, csv_fd, length=256 * 1024)
            csv_fd.close()

        df = pl.read_csv(
            csv_tmp_path,
            infer_schema_length=0,       # Read everything as Utf8 first
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


async def _try_download_concordance() -> pl.DataFrame | None:
    """Attempt to download the NAPCS→HS6 concordance table.

    Returns a DataFrame with columns [napcs_code, hs6_code, hs6_description]
    if successful, or None if the URL is unavailable / unparseable.
    """
    try:
        log.info("concordance_download_attempt", url=CONCORDANCE_URL)
        async with httpx.AsyncClient(
            timeout=60.0, follow_redirects=True
        ) as client:
            resp = await client.get(CONCORDANCE_URL)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")

            if "csv" in content_type or "text/plain" in content_type:
                body = resp.content
                if body.startswith(b"\xef\xbb\xbf"):
                    body = body[3:]
                df = pl.read_csv(io.BytesIO(body), infer_schema_length=0)
            elif "html" in content_type:
                # Page is HTML, not a direct CSV download — skip
                log.warning("concordance_html_page", msg="URL returned HTML, not CSV")
                return None
            else:
                body = resp.content
                if body.startswith(b"\xef\xbb\xbf"):
                    body = body[3:]
                df = pl.read_csv(
                    io.BytesIO(body),
                    infer_schema_length=0,
                    truncate_ragged_lines=True,
                )

        # Normalize column names
        rename_map: dict[str, str] = {}
        for c in df.columns:
            cl = c.strip().lower()
            if "napcs" in cl and "code" in cl:
                rename_map[c] = "napcs_code"
            elif "hs" in cl and "code" in cl:
                rename_map[c] = "hs6_code"
            elif "hs" in cl and ("desc" in cl or "name" in cl):
                rename_map[c] = "hs6_description"

        if "napcs_code" not in rename_map.values() or "hs6_code" not in rename_map.values():
            log.warning("concordance_columns_unrecognised", columns=df.columns)
            return None

        df = df.rename(rename_map)
        keep = [c for c in ["napcs_code", "hs6_code", "hs6_description"] if c in df.columns]
        df = df.select(keep).unique(subset=["napcs_code"])
        log.info("concordance_loaded", rows=len(df))
        return df

    except Exception as exc:
        log.warning("concordance_download_failed", error=str(exc))
        return None


# ---------------------------------------------------------------------------
# Extract / Transform
# ---------------------------------------------------------------------------

def _extract_code(text: str | None) -> str | None:
    """Extract the bracketed NAPCS code from a description string.

    Examples:
        "Energy products [C12]" → "C12"
        "Total of all merchandise"  → None
    """
    if not text:
        return None
    m = _CODE_RE.search(text.strip())
    return m.group(1) if m else None


def transform(
    raw: pl.DataFrame,
    *,
    from_year: int | None = None,
    to_year: int | None = None,
    province: str | None = None,
    concordance: pl.DataFrame | None = None,
) -> pl.DataFrame:
    """Transform the raw 12-10-0119-01 CSV into trade_flows_hs6 schema.

    Args:
        raw:         Raw polars DataFrame from the CSV.
        from_year:   Keep only rows with ref_year >= from_year.
        to_year:     Keep only rows with ref_year <= to_year.
        province:    If set, keep only rows matching this province.
        concordance: Optional NAPCS→HS6 concordance DataFrame.

    Returns:
        Cleaned DataFrame matching trade_flows_hs6 schema.
    """
    df = raw.rename({c: c.strip() for c in raw.columns})
    log.info("raw_columns", columns=df.columns)

    # ---- Identify key columns by pattern matching ----
    cols_upper = {c: c.upper() for c in df.columns}

    def _find_col(*patterns: str) -> str | None:
        for c, cu in cols_upper.items():
            for p in patterns:
                if p in cu:
                    return c
        return None

    ref_date_col = _find_col("REF_DATE")
    geo_col = _find_col("GEO")
    trade_col = _find_col("TRADE")
    partner_col = _find_col("PRINCIPAL TRADING", "TRADING PARTNER", "PARTNER")
    napcs_col = _find_col("NAPCS", "NORTH AMERICAN PRODUCT", "COMMODITY", "PRODUCT")
    value_col = _find_col("VALUE")
    status_col = _find_col("STATUS")
    scalar_col = _find_col("SCALAR_FACTOR")

    if ref_date_col is None:
        raise ValueError(f"Cannot find REF_DATE column. Available: {df.columns}")

    # ---- Drop suppressed rows ----
    if status_col:
        df = df.filter(
            pl.col(status_col).is_null() | (pl.col(status_col) != "x")
        )

    # ---- Drop null VALUE rows ----
    if value_col:
        df = df.filter(pl.col(value_col).is_not_null())

    # ---- Parse REF_DATE → ref_year, ref_month ----
    df = df.filter(
        pl.col(ref_date_col).is_not_null() & (pl.col(ref_date_col) != "")
    )

    # StatCan REF_DATE can be "YYYY-MM" or "YYYY"
    df = df.with_columns(
        pl.col(ref_date_col).str.slice(0, 4).cast(pl.Int32).alias("ref_year"),
        pl.when(pl.col(ref_date_col).str.len_chars() >= 7)
        .then(pl.col(ref_date_col).str.slice(5, 2).cast(pl.Int32))
        .otherwise(pl.lit(1))
        .alias("ref_month"),
    )

    if from_year:
        df = df.filter(pl.col("ref_year") >= from_year)
    if to_year:
        df = df.filter(pl.col("ref_year") <= to_year)

    # ---- GEO → province ----
    if geo_col:
        df = df.with_columns(
            pl.when(pl.col(geo_col).str.to_lowercase().str.contains("canada"))
            .then(pl.lit("Canada"))
            .otherwise(pl.col(geo_col).str.strip_chars())
            .alias("province")
        )
    else:
        df = df.with_columns(pl.lit("Canada").alias("province"))

    if province:
        df = df.filter(
            pl.col("province").str.to_lowercase() == province.lower()
        )

    # ---- Trade column → trade_flow ----
    if trade_col:
        # Keep only Import/Export rows (skip "Trade balance", totals, etc.)
        df = df.filter(
            pl.col(trade_col).str.to_lowercase().str.contains("import")
            | pl.col(trade_col).str.to_lowercase().str.contains("export")
        ).with_columns(
            pl.when(pl.col(trade_col).str.to_lowercase().str.contains("import"))
            .then(pl.lit("Import"))
            .otherwise(pl.lit("Export"))
            .alias("trade_flow")
        )
    else:
        df = df.with_columns(pl.lit("Export").alias("trade_flow"))

    # ---- Partner country ----
    if partner_col:
        df = df.with_columns(
            pl.col(partner_col).str.strip_chars().alias("partner_country")
        )
    else:
        df = df.with_columns(pl.lit("All countries").alias("partner_country"))

    # ---- NAPCS code + description ----
    if napcs_col:
        # Vectorized regex extract instead of map_elements
        df = df.with_columns(
            pl.col(napcs_col)
            .str.strip_chars()
            .str.extract(r"\[([A-Z0-9][A-Z0-9.]*)\]\s*$", 1)
            .alias("napcs_code"),
            pl.col(napcs_col).str.strip_chars().alias("napcs_description"),
        )
        # Drop total / aggregate rows that don't have a code
        df = df.filter(pl.col("napcs_code").is_not_null())
    else:
        df = df.with_columns(
            pl.lit("0000").alias("napcs_code"),
            pl.lit("Unknown").alias("napcs_description"),
        )

    # ---- VALUE → value_cad_millions ----
    if value_col:
        # Determine scalar factor
        scalar_mult = 1.0
        if scalar_col:
            # Try to read the first non-null scalar factor
            sample = df.select(pl.col(scalar_col).drop_nulls().first()).item()
            if sample:
                sample_lc = str(sample).strip().lower()
                if "thousand" in sample_lc:
                    scalar_mult = 1e-3  # thousands → millions
                elif "million" in sample_lc:
                    scalar_mult = 1.0   # already millions
                elif "billion" in sample_lc:
                    scalar_mult = 1e3   # billions → millions
                elif "unit" in sample_lc:
                    scalar_mult = 1e-6  # units → millions
                else:
                    scalar_mult = 1e-6  # assume raw dollars → millions
                log.info("scalar_factor", sample=sample, multiplier=scalar_mult)

        df = df.with_columns(
            (pl.col(value_col).cast(pl.Float64, strict=False) * scalar_mult)
            .alias("value_cad_millions")
        )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Float64).alias("value_cad_millions")
        )

    # ---- Concordance join (NAPCS → HS6) ----
    if concordance is not None and not concordance.is_empty():
        log.info("concordance_join", concordance_rows=len(concordance))
        df = df.join(concordance, on="napcs_code", how="left")
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.String).alias("hs6_code"),
            pl.lit(None).cast(pl.String).alias("hs6_description"),
        )

    # ---- Select final columns ----
    keep = [
        "ref_year", "ref_month", "province", "trade_flow",
        "partner_country", "napcs_code", "napcs_description",
        "hs6_code", "hs6_description", "value_cad_millions",
    ]
    df = df.select([c for c in keep if c in df.columns])

    # Deduplicate on the unique key — keep last occurrence
    df = df.unique(subset=CONFLICT_COLUMNS, keep="last")

    log.info(
        "transform_complete",
        rows=len(df),
        unique_napcs=df["napcs_code"].n_unique() if "napcs_code" in df.columns else 0,
        year_range=(
            f"{df['ref_year'].min()}-{df['ref_year'].max()}"
            if not df.is_empty()
            else "empty"
        ),
    )
    return df


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _print_summary(df: pl.DataFrame) -> None:
    """Print a human-readable summary of the loaded data."""
    if df.is_empty():
        print("No records loaded.")
        return

    min_row = df.sort(["ref_year", "ref_month"]).head(1)
    min_date = f"{min_row['ref_year'][0]:04d}-{min_row['ref_month'][0]:02d}"
    max_row = df.sort(["ref_year", "ref_month"], descending=True).head(1)
    max_date = f"{max_row['ref_year'][0]:04d}-{max_row['ref_month'][0]:02d}"

    # Top 5 NAPCS by import value
    imports = (
        df.filter(pl.col("trade_flow") == "Import")
        .group_by("napcs_code")
        .agg(pl.col("value_cad_millions").sum().alias("total"))
        .sort("total", descending=True)
        .head(5)
    )
    top_imports = [
        f"  {r['napcs_code']}: {r['total']:.2f}M"
        for r in imports.iter_rows(named=True)
    ]

    # Top 5 NAPCS by export value
    exports = (
        df.filter(pl.col("trade_flow") == "Export")
        .group_by("napcs_code")
        .agg(pl.col("value_cad_millions").sum().alias("total"))
        .sort("total", descending=True)
        .head(5)
    )
    top_exports = [
        f"  {r['napcs_code']}: {r['total']:.2f}M"
        for r in exports.iter_rows(named=True)
    ]

    print(f"\nLoaded {len(df)} records. Date range: {min_date} to {max_date}.")
    print("Top 5 NAPCS codes by total import value:")
    print("\n".join(top_imports) if top_imports else "  (none)")
    print("Top 5 NAPCS codes by total export value:")
    print("\n".join(top_exports) if top_exports else "  (none)")
    print()


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

async def run(
    *,
    from_year: int | None = 2019,
    to_year: int | None = None,
    province: str | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the StatCan trade HS6 pipeline.

    Downloads table 12-10-0119-01, transforms to trade_flows_hs6 schema,
    optionally joins NAPCS→HS6 concordance, and upserts to Supabase.

    Args:
        from_year: Earliest year to include (default 2019).
        to_year:   Latest year to include (default: current year).
        province:  Filter to a single province name (default: all).
        dry_run:   Transform and print sample but do not write to DB.

    Returns:
        LoadResult with record counts.
    """
    configure_logging()

    if to_year is None:
        to_year = datetime.now().year

    log.info(
        "trade_hs6_pipeline_start",
        from_year=from_year,
        to_year=to_year,
        province=province,
        dry_run=dry_run,
    )

    # ---- 1. Download bulk CSV ----
    zip_path = await _download_zip(BULK_CSV_URL)
    raw = _parse_csv_from_zip(zip_path)
    log.info("raw_data_loaded", rows=len(raw), columns=raw.columns)

    # ---- 2. Attempt concordance download ----
    concordance = await _try_download_concordance()

    # ---- 3. Transform ----
    df = transform(
        raw,
        from_year=from_year,
        to_year=to_year,
        province=province,
        concordance=concordance,
    )

    # Free raw data — it can be very large and is no longer needed
    del raw, concordance

    if df.is_empty():
        log.warning("trade_hs6_empty")
        return LoadResult(table=TABLE_NAME)

    # ---- 4. Dry-run or load ----
    if dry_run:
        print("\n=== DRY RUN — first 20 rows ===")
        with pl.Config(tbl_cols=-1, tbl_rows=20, fmt_str_lengths=60):
            print(df.head(20))
        _print_summary(df)
        return LoadResult(table=TABLE_NAME, records_loaded=len(df))

    # ---- 5. Upsert to Supabase ----
    loader = SupabaseLoader()
    run_id = await loader.start_pipeline_run(
        "trade_hs6",
        "StatCan-Trade-HS6",
        metadata={
            "from_year": from_year,
            "to_year": to_year,
            "province": province,
        },
    )

    try:
        result = await loader.upsert(
            TABLE_NAME, df, conflict_columns=CONFLICT_COLUMNS
        )
        await loader.finish_pipeline_run(
            run_id,
            result,
            metadata={"rows": result.records_loaded},
        )
    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise

    _print_summary(df)
    log.info(
        "trade_hs6_pipeline_complete",
        records_loaded=result.records_loaded,
        records_failed=result.records_failed,
        status=result.status,
    )
    return result


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

def main() -> None:
    """Standalone CLI entry point for the trade HS6 pipeline."""
    import argparse
    import asyncio

    parser = argparse.ArgumentParser(
        description="StatCan Trade HS6 pipeline (table 12-10-0119-01)",
    )
    parser.add_argument(
        "--from-year", type=int, default=2019,
        help="Start year (default: 2019)",
    )
    parser.add_argument(
        "--to-year", type=int, default=None,
        help="End year (default: current year)",
    )
    parser.add_argument(
        "--province", type=str, default=None,
        help="Filter to a specific province (default: all)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print first 20 rows, do not write to DB",
    )
    args = parser.parse_args()

    asyncio.run(run(
        from_year=args.from_year,
        to_year=args.to_year,
        province=args.province,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
