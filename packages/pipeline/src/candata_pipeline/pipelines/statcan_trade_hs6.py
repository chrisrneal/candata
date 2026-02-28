"""
pipelines/statcan_trade_hs6.py — Canadian trade data at NAPCS / HS6 product level.

Ingests StatCan Table 12-10-0119-01 (Canadian International Merchandise Trade
by NAPCS), transforms to a clean trade_flows_hs6 schema, optionally joins an
NAPCS→HS6 concordance table, and upserts to Supabase.

Uses chunked CSV reading via the large_file utility module to avoid OOM
errors on the 2GB+ bulk CSV.

Usage (as module):
    from candata_pipeline.pipelines.statcan_trade_hs6 import run
    result = await run(from_year=2022, dry_run=True)

CLI (via run_pipeline.py):
    python scripts/run_pipeline.py trade-hs6 --from-year 2022 --dry-run
"""

from __future__ import annotations

import io
import os
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
from candata_pipeline.utils.checkpoint import (
    clear_checkpoint,
    load_checkpoint,
    save_checkpoint,
)
from candata_pipeline.utils.large_file import (
    check_available_memory,
    estimate_csv_rows,
    get_or_download,
    monitor_memory,
    stream_csv_chunks,
    upsert_chunk,
)
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

PIPELINE_NAME = "trade_hs6"

# Cache settings
CACHE_DIR = Path(__file__).resolve().parents[3] / "data" / "cache"
CACHE_FILENAME = "12100119-eng.zip"
CACHE_CSV_FILENAME = "12100119-eng.csv"
CACHE_MAX_AGE_HOURS = 168  # weekly

# StatCan suppressed / missing markers
_SUPPRESSED: frozenset[str] = frozenset({"x", "..", "...", "F", "E", "r", "p", ""})

# Pattern to extract NAPCS code from description field.
# StatCan format: "Description text [C21]"  → "C21"
#                 "Total of all merchandise" → None (no code)
_CODE_RE = re.compile(r"\[([A-Z0-9][A-Z0-9.]*)\]\s*$", re.IGNORECASE)

# Columns to read from the bulk CSV
_USECOLS = [
    "REF_DATE", "GEO", "Trade",
    "Principal trading partners",
    "North American Product Classification System (NAPCS)",
    "VALUE", "STATUS",
]

_DTYPE = {"VALUE": "float32", "STATUS": "str"}


# ---------------------------------------------------------------------------
# Download / extract helpers
# ---------------------------------------------------------------------------


def _extract_csv_from_zip(zip_path: Path, dest_dir: Path) -> Path:
    """Extract the data CSV (not MetaData) from a StatCan ZIP.

    Returns the path to the extracted CSV file.
    """
    dest_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path) as zf:
        data_files = [
            n for n in zf.namelist()
            if n.endswith(".csv") and "MetaData" not in n
        ]
        if not data_files:
            raise ValueError("No data CSV found in ZIP")

        csv_name = data_files[0]
        dest_csv = dest_dir / CACHE_CSV_FILENAME

        with zf.open(csv_name) as src, open(dest_csv, "wb") as dst:
            shutil.copyfileobj(src, dst, length=256 * 1024)

    # Strip BOM if present
    with open(dest_csv, "rb") as f:
        head = f.read(3)
    if head == b"\xef\xbb\xbf":
        content = dest_csv.read_bytes()[3:]
        dest_csv.write_bytes(content)

    return dest_csv


async def _try_download_concordance() -> dict[str, tuple[str, str]]:
    """Attempt to download the NAPCS→HS6 concordance table.

    Returns a dict mapping napcs_code → (hs6_code, hs6_description),
    or an empty dict if the URL is unavailable.
    """
    try:
        log.info("concordance_download_attempt", url=CONCORDANCE_URL)
        async with httpx.AsyncClient(
            timeout=60.0, follow_redirects=True
        ) as client:
            resp = await client.get(CONCORDANCE_URL)
            resp.raise_for_status()
            content_type = resp.headers.get("content-type", "")

            if "html" in content_type:
                log.warning("concordance_html_page", msg="URL returned HTML, not CSV")
                return {}

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
            return {}

        df = df.rename(rename_map)
        keep = [c for c in ["napcs_code", "hs6_code", "hs6_description"] if c in df.columns]
        df = df.select(keep).unique(subset=["napcs_code"])

        result = {}
        for row in df.iter_rows(named=True):
            result[row["napcs_code"]] = (
                row.get("hs6_code", ""),
                row.get("hs6_description", ""),
            )
        log.info("concordance_loaded", rows=len(result))
        return result

    except Exception as exc:
        log.warning("concordance_download_failed", error=str(exc))
        return {}


# ---------------------------------------------------------------------------
# Per-chunk transform (polars)
# ---------------------------------------------------------------------------


def _extract_code(text: str | None) -> str | None:
    """Extract the bracketed NAPCS code from a description string."""
    if not isinstance(text, str) or not text:
        return None
    m = _CODE_RE.search(text.strip())
    return m.group(1) if m else None


def _extract_description(text: str | None) -> str | None:
    """Return the description part before the bracketed code."""
    if not isinstance(text, str) or not text:
        return None
    return re.sub(r"\s*\[[A-Z0-9][A-Z0-9.]*\]\s*$", "", text.strip(), flags=re.IGNORECASE)


def _transform_chunk(
    chunk: pl.DataFrame,
    concordance: dict[str, tuple[str, str]],
    *,
    from_year: int | None = None,
    to_year: int | None = None,
    province: str | None = None,
) -> list[dict[str, Any]]:
    """Transform a single polars chunk into a list of upsert-ready dicts.

    Filters suppressed rows, parses dates, extracts NAPCS codes, and
    optionally joins the concordance mapping.
    """
    df = chunk.clone()

    # Filter rows where STATUS is suppressed or VALUE is null
    if "STATUS" in df.columns:
        df = df.filter(
            ~pl.col("STATUS").is_in(list(_SUPPRESSED)) | pl.col("STATUS").is_null()
        )
    df = df.filter(pl.col("VALUE").is_not_null())

    if df.is_empty():
        return []

    # Parse REF_DATE into ref_year and ref_month
    df = df.with_columns(pl.col("REF_DATE").cast(pl.Utf8).str.strip_chars())
    df = df.filter(pl.col("REF_DATE").str.len_chars() >= 4)
    df = df.with_columns(
        pl.col("REF_DATE").str.slice(0, 4).cast(pl.Int32).alias("ref_year"),
        pl.col("REF_DATE").str.slice(5, 2).cast(pl.Int32, strict=False).fill_null(1).alias("ref_month"),
    )

    # Year filtering
    if from_year:
        df = df.filter(pl.col("ref_year") >= from_year)
    if to_year:
        df = df.filter(pl.col("ref_year") <= to_year)

    if df.is_empty():
        return []

    # GEO → province
    napcs_col = "North American Product Classification System (NAPCS)"
    trade_col = "Trade"
    partner_col = "Principal trading partners"

    if "GEO" in df.columns:
        df = df.with_columns(
            pl.when(pl.col("GEO").str.strip_chars().str.to_lowercase().str.contains("canada"))
            .then(pl.lit("Canada"))
            .otherwise(pl.col("GEO").str.strip_chars())
            .alias("province")
        )
    else:
        df = df.with_columns(pl.lit("Canada").alias("province"))

    if province:
        df = df.filter(pl.col("province").str.to_lowercase() == province.lower())
        if df.is_empty():
            return []

    # Trade flow
    if trade_col in df.columns:
        trade_lower = pl.col(trade_col).str.to_lowercase()
        df = df.filter(
            trade_lower.str.contains("import") | trade_lower.str.contains("export")
        )
        df = df.with_columns(
            pl.when(pl.col(trade_col).str.to_lowercase().str.contains("import"))
            .then(pl.lit("Import"))
            .otherwise(pl.lit("Export"))
            .alias("trade_flow")
        )
    else:
        df = df.with_columns(pl.lit("Export").alias("trade_flow"))

    # Partner country
    if partner_col in df.columns:
        df = df.with_columns(pl.col(partner_col).str.strip_chars().alias("partner_country"))
    else:
        df = df.with_columns(pl.lit("All countries").alias("partner_country"))

    # NAPCS code + description using map_elements for regex extraction
    if napcs_col in df.columns:
        df = df.with_columns(
            pl.col(napcs_col).map_elements(_extract_code, return_dtype=pl.Utf8).alias("napcs_code"),
            pl.col(napcs_col).map_elements(_extract_description, return_dtype=pl.Utf8).alias("napcs_description"),
        )
        df = df.filter(pl.col("napcs_code").is_not_null())
    else:
        df = df.with_columns(
            pl.lit("0000").alias("napcs_code"),
            pl.lit("Unknown").alias("napcs_description"),
        )

    if df.is_empty():
        return []

    # VALUE → value_cad_millions
    df = df.with_columns(
        (pl.col("VALUE").cast(pl.Float64) * 1e-6).alias("value_cad_millions")
    )

    # Concordance join
    if concordance:
        df = df.with_columns(
            pl.col("napcs_code").map_elements(
                lambda c: concordance.get(c, ("", ""))[0], return_dtype=pl.Utf8
            ).alias("hs6_code"),
            pl.col("napcs_code").map_elements(
                lambda c: concordance.get(c, ("", ""))[1], return_dtype=pl.Utf8
            ).alias("hs6_description"),
        )
    else:
        df = df.with_columns(
            pl.lit(None).cast(pl.Utf8).alias("hs6_code"),
            pl.lit(None).cast(pl.Utf8).alias("hs6_description"),
        )

    # Select final columns
    keep = [
        "ref_year", "ref_month", "province", "trade_flow",
        "partner_country", "napcs_code", "napcs_description",
        "hs6_code", "hs6_description", "value_cad_millions",
    ]
    df = df.select([c for c in keep if c in df.columns])

    # Deduplicate within chunk
    df = df.unique(subset=CONFLICT_COLUMNS, keep="last")

    # Convert to dicts, stripping None values
    records = []
    for row in df.iter_rows(named=True):
        clean = {k: v for k, v in row.items() if v is not None}
        records.append(clean)

    return records


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

def _print_summary(
    total_rows: int,
    total_inserted: int,
    total_skipped: int,
    total_errors: int,
    cache_path: Path,
) -> None:
    """Print a human-readable summary of the pipeline run."""
    cache_size_mb = cache_path.stat().st_size / 1_048_576 if cache_path.exists() else 0
    print(f"\n{'='*60}")
    print(f"StatCan Trade HS6 Pipeline Summary")
    print(f"{'='*60}")
    print(f"  Rows processed:  {total_rows:,}")
    print(f"  Rows inserted:   {total_inserted:,}")
    print(f"  Rows skipped:    {total_skipped:,}")
    print(f"  Errors:          {total_errors:,}")
    print(f"  Cache file size: {cache_size_mb:.1f}MB")
    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------


@monitor_memory
async def run(
    *,
    from_year: int | None = 2019,
    to_year: int | None = None,
    province: str | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the StatCan trade HS6 pipeline with chunked processing.

    Downloads table 12-10-0119-01, streams the CSV in chunks, transforms
    each chunk, and upserts to Supabase.  Uses checkpointing so
    interrupted runs can resume.

    Args:
        from_year: Earliest year to include (default 2019).
        to_year:   Latest year to include (default: current year).
        province:  Filter to a single province name (default: all).
        dry_run:   Transform and print sample but do not write to DB.

    Returns:
        LoadResult with record counts.
    """
    configure_logging()
    check_available_memory(required_gb=1.0)

    if to_year is None:
        to_year = datetime.now().year

    log.info(
        "trade_hs6_pipeline_start",
        from_year=from_year,
        to_year=to_year,
        province=province,
        dry_run=dry_run,
    )

    # ---- 1. Download bulk CSV (cached weekly) ----
    zip_path = get_or_download(
        BULK_CSV_URL, CACHE_DIR, CACHE_FILENAME,
        max_age_hours=CACHE_MAX_AGE_HOURS,
    )

    # Extract CSV from ZIP if not already done
    csv_path = CACHE_DIR / CACHE_CSV_FILENAME
    if not csv_path.exists() or csv_path.stat().st_mtime < zip_path.stat().st_mtime:
        print("Extracting CSV from ZIP...")
        csv_path = _extract_csv_from_zip(zip_path, CACHE_DIR)

    # ---- 2. Estimate rows ----
    estimated_rows = estimate_csv_rows(csv_path)

    # ---- 3. Attempt concordance download ----
    concordance = await _try_download_concordance()

    # ---- 4. Load checkpoint ----
    start_row = load_checkpoint(PIPELINE_NAME)
    if start_row > 0:
        print(f"Resuming from checkpoint: row {start_row:,}")

    # ---- 5. Stream and process chunks ----
    total_rows = 0
    total_inserted = 0
    total_skipped = 0
    total_errors = 0

    loader = None
    run_id = None

    if not dry_run:
        from candata_shared.db import get_supabase_client
        supabase_client = get_supabase_client(service_role=True)
        loader = SupabaseLoader()
        run_id = await loader.start_pipeline_run(
            PIPELINE_NAME,
            "StatCan-Trade-HS6",
            metadata={
                "from_year": from_year,
                "to_year": to_year,
                "province": province,
                "estimated_rows": estimated_rows,
            },
        )

    try:
        for chunk in stream_csv_chunks(
            csv_path,
            chunksize=50_000,
            usecols=_USECOLS,
            dtype=_DTYPE,
            start_row=start_row,
        ):
            records = _transform_chunk(
                chunk,
                concordance,
                from_year=from_year,
                to_year=to_year,
                province=province,
            )
            total_rows += len(chunk)
            skipped = len(chunk) - len(records)
            total_skipped += skipped

            if not records:
                save_checkpoint(PIPELINE_NAME, total_rows)
                continue

            if dry_run:
                # Show first chunk sample only
                if total_inserted == 0:
                    print("\n=== DRY RUN — first 20 records ===")
                    for r in records[:20]:
                        print(r)
                total_inserted += len(records)
            else:
                inserted, errs = upsert_chunk(
                    supabase_client, TABLE_NAME, records, CONFLICT_COLUMNS,
                )
                total_inserted += inserted
                total_errors += errs

            save_checkpoint(PIPELINE_NAME, total_rows)

        # ---- 6. Success — clear checkpoint ----
        clear_checkpoint(PIPELINE_NAME)

        if run_id and loader:
            result = LoadResult(
                table=TABLE_NAME,
                records_loaded=total_inserted,
                records_failed=total_errors,
            )
            await loader.finish_pipeline_run(
                run_id, result,
                metadata={"rows": total_inserted, "estimated_rows": estimated_rows},
            )

    except Exception as exc:
        if run_id and loader:
            await loader.fail_pipeline_run(run_id, str(exc))
        raise

    _print_summary(total_rows, total_inserted, total_skipped, total_errors, csv_path)

    log.info(
        "trade_hs6_pipeline_complete",
        records_loaded=total_inserted,
        records_failed=total_errors,
        total_rows=total_rows,
    )
    return LoadResult(
        table=TABLE_NAME,
        records_loaded=total_inserted,
        records_failed=total_errors,
    )


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
