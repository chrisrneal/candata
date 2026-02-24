"""
pipelines/procurement.py — Federal contracts and tenders pipeline.

Ingests:
  - Proactive disclosure contracts CSV → contracts table
  - CanadaBuys active tenders → tenders table

Entity resolution: vendor names are normalized and optionally linked to
entities table entries (requires entity cache loaded separately).

Usage:
    from candata_pipeline.pipelines.procurement import run
    result = await run(datasets=["contracts", "tenders"])

    # Filter by fiscal year (April–March)
    result = await run(fiscal_year="2024-2025")
"""

from __future__ import annotations

import uuid as uuid_module
from datetime import date
from typing import Literal

import polars as pl

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.procurement import ProcurementSource
from candata_pipeline.transforms.time_series import deduplicate_series
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="procurement")

Dataset = Literal["contracts", "tenders"]


def _parse_fiscal_year(fy: str) -> tuple[date, date]:
    """
    Parse a fiscal year string like "2024-2025" into a date range.

    Canadian federal fiscal year: April 1 to March 31.
    Returns (start_date, end_date) inclusive.
    """
    parts = fy.split("-")
    if len(parts) != 2:
        raise ValueError(f"Invalid fiscal year format: {fy!r} — expected YYYY-YYYY")
    start_year = int(parts[0])
    end_year = int(parts[1])
    if end_year != start_year + 1:
        raise ValueError(
            f"Invalid fiscal year: {fy!r} — end year must be start year + 1"
        )
    return date(start_year, 4, 1), date(end_year, 3, 31)


async def _load_contracts(
    source: ProcurementSource,
    loader: SupabaseLoader,
    fiscal_year: str | None,
    dry_run: bool,
) -> LoadResult:
    """Fetch, transform, and upsert contracts."""
    raw = await source.extract(dataset="contracts")
    df = source.transform(raw, dataset="contracts")

    if df.is_empty():
        log.warning("contracts_empty")
        return LoadResult(table="contracts")

    # Filter by fiscal year (April–March)
    if fiscal_year and "award_date" in df.columns:
        fy_start, fy_end = _parse_fiscal_year(fiscal_year)
        df = df.filter(
            pl.col("award_date").is_not_null()
            & (pl.col("award_date") >= fy_start)
            & (pl.col("award_date") <= fy_end)
        )

    # Drop rows with no vendor or department
    for col in ["vendor_name", "department"]:
        if col in df.columns:
            df = df.filter(pl.col(col).is_not_null() & (pl.col(col) != ""))

    # Deduplicate by contract_number if present
    if "contract_number" in df.columns:
        df = deduplicate_series(df, ["contract_number"], keep="last")

    # Generate UUIDs for rows that don't have a contract_number PK
    if "id" not in df.columns:
        df = df.with_columns(
            pl.Series(
                "id",
                [str(uuid_module.uuid4()) for _ in range(len(df))],
                dtype=pl.String,
            )
        )

    # Ensure raw_data column exists
    if "raw_data" not in df.columns:
        df = df.with_columns(pl.lit("{}").alias("raw_data"))

    log.info("contracts_ready", rows=len(df))

    if dry_run:
        return LoadResult(table="contracts", records_loaded=len(df))

    return await loader.upsert(
        "contracts",
        df,
        conflict_columns=(
            ["contract_number"] if "contract_number" in df.columns else ["id"]
        ),
    )


async def _load_tenders(
    source: ProcurementSource,
    loader: SupabaseLoader,
    dry_run: bool,
) -> LoadResult:
    """Fetch, transform, and upsert tenders."""
    raw = await source.extract(dataset="tenders", max_tenders=1000)
    df = source.transform(raw, dataset="tenders")

    if df.is_empty():
        log.warning("tenders_empty")
        return LoadResult(table="tenders")

    # Drop rows with no title or department
    for col in ["title", "department"]:
        if col in df.columns:
            df = df.filter(pl.col(col).is_not_null() & (pl.col(col) != ""))

    if "id" not in df.columns:
        df = df.with_columns(
            pl.Series(
                "id",
                [str(uuid_module.uuid4()) for _ in range(len(df))],
                dtype=pl.String,
            )
        )

    if "raw_data" not in df.columns:
        df = df.with_columns(pl.lit("{}").alias("raw_data"))

    log.info("tenders_ready", rows=len(df))

    if dry_run:
        return LoadResult(table="tenders", records_loaded=len(df))

    conflict = ["tender_number"] if "tender_number" in df.columns else ["id"]
    return await loader.upsert("tenders", df, conflict_columns=conflict)


async def run(
    *,
    datasets: list[Dataset] | None = None,
    fiscal_year: str | None = None,
    dry_run: bool = False,
) -> dict[str, LoadResult]:
    """
    Run the procurement pipeline.

    Args:
        datasets:    Subset to run (default: both "contracts" and "tenders").
        fiscal_year: Filter contracts to this fiscal year, e.g. "2024-2025".
        dry_run:     Transform but do not write to Supabase.

    Returns:
        Dict of {table_name → LoadResult}.
    """
    configure_logging()
    datasets = datasets or ["contracts", "tenders"]
    log.info(
        "procurement_pipeline_start",
        datasets=datasets,
        fiscal_year=fiscal_year,
        dry_run=dry_run,
    )

    loader = SupabaseLoader()
    source = ProcurementSource()

    run_id = await loader.start_pipeline_run(
        "procurement",
        "open.canada.ca",
        metadata={
            "datasets": datasets,
            "fiscal_year": fiscal_year,
            "dry_run": dry_run,
        },
    )

    results: dict[str, LoadResult] = {}
    try:
        if "contracts" in datasets:
            results["contracts"] = await _load_contracts(
                source, loader, fiscal_year, dry_run
            )

        if "tenders" in datasets:
            results["tenders"] = await _load_tenders(source, loader, dry_run)

        total_loaded = sum(r.records_loaded for r in results.values())
        total_failed = sum(r.records_failed for r in results.values())
        combined = LoadResult(
            table="procurement_combined",
            records_loaded=total_loaded,
            records_failed=total_failed,
        )
        await loader.finish_pipeline_run(run_id, combined)

    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise

    return results
