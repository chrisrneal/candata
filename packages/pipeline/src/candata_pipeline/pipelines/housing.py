"""
pipelines/housing.py — CMHC housing market data pipeline.

Ingests:
  - Vacancy rates by CMA + bedroom type  → vacancy_rates table
  - Average asking rents by CMA + bedroom → average_rents table
  - Housing starts by CMA/province + dwelling type → housing_starts table

Each dataset is fetched from CMHC HMIP and transformed into table-specific
schemas (NOT indicator_values — these have dedicated tables).

Usage:
    from candata_pipeline.pipelines.housing import run
    result = await run(year=2023)
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.cmhc import CMHCSource
from candata_pipeline.transforms.normalize import GeoNormalizer
from candata_pipeline.transforms.time_series import deduplicate_series
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="housing")


async def _fetch_vacancy_rates(
    source: CMHCSource,
    year: int,
    normalizer: GeoNormalizer,
) -> pl.DataFrame:
    """Fetch and normalize CMHC vacancy rates."""
    raw = await source.extract(dataset="vacancy_rates", year=year)
    df = source.transform(raw, dataset="vacancy_rates")

    if df.is_empty() or "sgc_code" not in df.columns:
        log.warning("vacancy_rates_empty", year=year)
        return pl.DataFrame()

    # Resolve geography_id
    df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
    df = df.filter(pl.col("geography_id").is_not_null())

    # Ensure required columns exist
    required = ["geography_id", "ref_date", "bedroom_type"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.warning("vacancy_rates_missing_cols", missing=missing)
        return pl.DataFrame()

    # Add uuid
    import uuid as uuid_module
    df = df.with_columns(
        pl.Series("id", [str(uuid_module.uuid4()) for _ in range(len(df))], dtype=pl.String)
    )

    output_cols = [c for c in ["id", "geography_id", "ref_date", "bedroom_type",
                                 "vacancy_rate", "universe"] if c in df.columns]
    df = df.select(output_cols)
    df = deduplicate_series(df, ["geography_id", "ref_date", "bedroom_type"])
    log.info("vacancy_rates_ready", rows=len(df), year=year)
    return df


async def _fetch_average_rents(
    source: CMHCSource,
    year: int,
    normalizer: GeoNormalizer,
) -> pl.DataFrame:
    """Fetch and normalize CMHC average rents."""
    raw = await source.extract(dataset="average_rents", year=year)
    df = source.transform(raw, dataset="average_rents")

    if df.is_empty() or "sgc_code" not in df.columns:
        log.warning("average_rents_empty", year=year)
        return pl.DataFrame()

    df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
    df = df.filter(pl.col("geography_id").is_not_null())

    import uuid as uuid_module
    df = df.with_columns(
        pl.Series("id", [str(uuid_module.uuid4()) for _ in range(len(df))], dtype=pl.String)
    )

    output_cols = [c for c in ["id", "geography_id", "ref_date", "bedroom_type",
                                 "average_rent"] if c in df.columns]
    df = df.select(output_cols)
    df = deduplicate_series(df, ["geography_id", "ref_date", "bedroom_type"])
    log.info("average_rents_ready", rows=len(df), year=year)
    return df


async def _fetch_housing_starts(
    source: CMHCSource,
    year: int,
    normalizer: GeoNormalizer,
) -> pl.DataFrame:
    """Fetch and normalize CMHC housing starts."""
    raw = await source.extract(dataset="housing_starts", year=year)
    df = source.transform(raw, dataset="housing_starts")

    if df.is_empty() or "sgc_code" not in df.columns:
        log.warning("housing_starts_empty", year=year)
        return pl.DataFrame()

    df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
    df = df.filter(pl.col("geography_id").is_not_null())

    import uuid as uuid_module
    df = df.with_columns(
        pl.Series("id", [str(uuid_module.uuid4()) for _ in range(len(df))], dtype=pl.String)
    )

    output_cols = [c for c in ["id", "geography_id", "ref_date", "dwelling_type",
                                 "units"] if c in df.columns]
    df = df.select(output_cols)
    df = deduplicate_series(df, ["geography_id", "ref_date", "dwelling_type"])
    log.info("housing_starts_ready", rows=len(df), year=year)
    return df


async def run(
    *,
    year: int | None = None,
    start_date: date | None = None,
    dry_run: bool = False,
) -> dict[str, LoadResult]:
    """
    Run all three housing pipelines for a given year.

    Args:
        year:       Reference year (defaults to last full year).
        start_date: If set, overrides year to start_date.year.
        dry_run:    Transform but do not write to Supabase.

    Returns:
        Dict of {dataset_name → LoadResult}.
    """
    configure_logging()
    import datetime
    if year is None:
        year = (start_date or datetime.date.today()).year - 1 if start_date is None else start_date.year

    log.info("housing_pipeline_start", year=year, dry_run=dry_run)

    loader = SupabaseLoader()
    geo_lookup = await loader.build_geo_lookup()

    normalizer = GeoNormalizer()
    normalizer._cache = geo_lookup
    normalizer._loaded = True

    source = CMHCSource()

    run_id = await loader.start_pipeline_run(
        "housing", "CMHC", metadata={"year": year, "dry_run": dry_run}
    )

    results: dict[str, LoadResult] = {}

    try:
        vacancy_df, rents_df, starts_df = await asyncio.gather(
            _fetch_vacancy_rates(source, year, normalizer),
            _fetch_average_rents(source, year, normalizer),
            _fetch_housing_starts(source, year, normalizer),
            return_exceptions=False,
        )

        for table, df, conflict_cols in [
            ("vacancy_rates", vacancy_df, ["geography_id", "ref_date", "bedroom_type"]),
            ("average_rents", rents_df, ["geography_id", "ref_date", "bedroom_type"]),
            ("housing_starts", starts_df, ["geography_id", "ref_date", "dwelling_type"]),
        ]:
            if df is None or (isinstance(df, pl.DataFrame) and df.is_empty()):
                results[table] = LoadResult(table=table)
                continue

            if dry_run:
                log.info("dry_run_skip", table=table, rows=len(df))
                results[table] = LoadResult(table=table, records_loaded=len(df))
                continue

            result = await loader.upsert(table, df, conflict_columns=conflict_cols)
            results[table] = result

        # Summarize
        total_loaded = sum(r.records_loaded for r in results.values())
        total_failed = sum(r.records_failed for r in results.values())
        combined_result = LoadResult(
            table="housing_combined",
            records_loaded=total_loaded,
            records_failed=total_failed,
        )
        await loader.finish_pipeline_run(run_id, combined_result)

    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise

    return results
