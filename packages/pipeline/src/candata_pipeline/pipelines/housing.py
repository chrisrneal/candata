"""
pipelines/housing.py — CMHC housing market data pipeline.

Ingests:
  - Vacancy rates by CMA + bedroom type  -> vacancy_rates table
  - Average asking rents by CMA + bedroom -> average_rents table
  - Housing starts by CMA + dwelling type -> housing_starts table
  - Summary indicators -> indicator_values table

Each dataset is fetched from CMHC's HMIP internal API (with Open Canada
CSV fallback) per CMA, transformed into table-specific schemas, and
upserted to Supabase.

Usage:
    from candata_pipeline.pipelines.housing import run
    result = await run()                                    # all CMAs
    result = await run(cmas=["toronto", "vancouver"])       # specific CMAs
    result = await run(dry_run=True)                        # no DB writes
"""

from __future__ import annotations

import asyncio
import uuid as uuid_module
from datetime import date
from typing import Any

import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.cmhc import (
    CMA_GEOUIDS,
    CMHC_GEO_NAMES,
    CMHC_GEO_TO_SGC,
    CMA_NAME_TO_CMHC,
    CMHCSource,
    Dataset,
)
from candata_pipeline.transforms.normalize import GeoNormalizer
from candata_pipeline.transforms.time_series import deduplicate_series
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="housing")


def _resolve_cma_filter(cmas: list[str]) -> list[int]:
    """
    Resolve CMA name strings to CMHC geo IDs.

    Accepts full names (e.g. "Toronto"), short aliases (e.g. "montreal"),
    or CMHC geo IDs as strings (e.g. "2270").
    """
    geo_ids: list[int] = []
    for cma in cmas:
        key = cma.strip().lower()

        # Try name lookup first
        if key in CMA_NAME_TO_CMHC:
            geo_ids.append(CMA_NAME_TO_CMHC[key])
            continue

        # Try as numeric CMHC geo ID
        try:
            geo_id = int(key)
            if geo_id in CMHC_GEO_TO_SGC:
                geo_ids.append(geo_id)
                continue
        except ValueError:
            pass

        log.warning("unknown_cma_filter", cma=cma)

    return geo_ids


def _add_uuid_column(df: pl.DataFrame) -> pl.DataFrame:
    """Add a UUID 'id' column to a DataFrame."""
    return df.with_columns(
        pl.Series("id", [str(uuid_module.uuid4()) for _ in range(len(df))], dtype=pl.String)
    )


async def _fetch_dataset(
    source: CMHCSource,
    dataset: Dataset,
    normalizer: GeoNormalizer,
    cmhc_geo_ids: list[int] | None,
    start_date: date | None,
    *,
    dry_run: bool = False,
) -> pl.DataFrame:
    """Fetch and normalize one CMHC dataset across all selected CMAs."""
    raw = await source.extract(
        dataset=dataset,
        cmhc_geo_ids=cmhc_geo_ids,
        start_date=start_date,
    )

    if raw.is_empty():
        log.warning("dataset_empty", dataset=dataset)
        return pl.DataFrame()

    # Data from extract() already has sgc_code column
    if "sgc_code" not in raw.columns:
        log.warning("dataset_missing_sgc", dataset=dataset)
        return pl.DataFrame()

    if dry_run:
        # In dry-run mode, use sgc_code as a placeholder geography_id
        df = raw.with_columns(pl.col("sgc_code").alias("geography_id"))
    else:
        # Resolve sgc_code -> geography_id via DB lookup
        df = normalizer.add_geography_id(raw, sgc_code_col="sgc_code")
        df = df.filter(pl.col("geography_id").is_not_null())

        if df.is_empty():
            log.warning("dataset_no_mapped_geos", dataset=dataset)
            return pl.DataFrame()

    # Add UUID primary key
    df = _add_uuid_column(df)

    log.info("dataset_ready", dataset=dataset, rows=len(df))
    return df


def _prepare_vacancy_df(df: pl.DataFrame) -> pl.DataFrame:
    """Select and deduplicate vacancy_rates columns."""
    if df.is_empty():
        return df
    output_cols = [c for c in ["id", "geography_id", "ref_date", "bedroom_type",
                                "vacancy_rate", "universe"] if c in df.columns]
    df = df.select(output_cols)
    return deduplicate_series(df, ["geography_id", "ref_date", "bedroom_type"])


def _prepare_rents_df(df: pl.DataFrame) -> pl.DataFrame:
    """Select and deduplicate average_rents columns."""
    if df.is_empty():
        return df
    output_cols = [c for c in ["id", "geography_id", "ref_date", "bedroom_type",
                                "average_rent"] if c in df.columns]
    df = df.select(output_cols)
    return deduplicate_series(df, ["geography_id", "ref_date", "bedroom_type"])


def _prepare_starts_df(df: pl.DataFrame) -> pl.DataFrame:
    """Select and deduplicate housing_starts columns."""
    if df.is_empty():
        return df
    output_cols = [c for c in ["id", "geography_id", "ref_date", "dwelling_type",
                                "units"] if c in df.columns]
    df = df.select(output_cols)
    return deduplicate_series(df, ["geography_id", "ref_date", "dwelling_type"])


def _build_indicator_values(
    vacancy_df: pl.DataFrame,
    rents_df: pl.DataFrame,
    starts_df: pl.DataFrame,
) -> pl.DataFrame:
    """
    Build indicator_values rows from the housing data.

    Extracts summary values per CMA (latest date):
      - vacancy_rate indicator: total vacancy rate
      - average_rent indicator: 2BR average rent
      - housing_starts indicator: total starts
    """
    indicator_rows: list[dict[str, Any]] = []

    # Vacancy rate: total vacancy rate per CMA (latest date)
    if not vacancy_df.is_empty() and "bedroom_type" in vacancy_df.columns:
        totals = vacancy_df.filter(pl.col("bedroom_type") == "total")
        if not totals.is_empty() and "ref_date" in totals.columns:
            latest = totals.sort("ref_date", descending=True).unique(
                subset=["geography_id"], keep="first"
            )
            for row in latest.iter_rows(named=True):
                if row.get("vacancy_rate") is not None:
                    indicator_rows.append({
                        "indicator_id": "vacancy_rate",
                        "geography_id": row["geography_id"],
                        "ref_date": row["ref_date"],
                        "value": float(row["vacancy_rate"]),
                    })

    # Average rent: 2BR rent per CMA (latest date)
    if not rents_df.is_empty() and "bedroom_type" in rents_df.columns:
        two_br = rents_df.filter(pl.col("bedroom_type") == "2br")
        if not two_br.is_empty() and "ref_date" in two_br.columns:
            latest = two_br.sort("ref_date", descending=True).unique(
                subset=["geography_id"], keep="first"
            )
            for row in latest.iter_rows(named=True):
                if row.get("average_rent") is not None:
                    indicator_rows.append({
                        "indicator_id": "average_rent",
                        "geography_id": row["geography_id"],
                        "ref_date": row["ref_date"],
                        "value": float(row["average_rent"]),
                    })

    # Housing starts: total starts per CMA (latest date)
    if not starts_df.is_empty() and "dwelling_type" in starts_df.columns:
        totals = starts_df.filter(pl.col("dwelling_type") == "total")
        if not totals.is_empty() and "ref_date" in totals.columns:
            latest = totals.sort("ref_date", descending=True).unique(
                subset=["geography_id"], keep="first"
            )
            for row in latest.iter_rows(named=True):
                if row.get("units") is not None:
                    indicator_rows.append({
                        "indicator_id": "housing_starts",
                        "geography_id": row["geography_id"],
                        "ref_date": row["ref_date"],
                        "value": float(row["units"]),
                    })

    if not indicator_rows:
        return pl.DataFrame(schema={
            "indicator_id": pl.String,
            "geography_id": pl.String,
            "ref_date": pl.Date,
            "value": pl.Float64,
        })

    return pl.from_dicts(indicator_rows)


async def run(
    *,
    year: int | None = None,
    start_date: date | None = None,
    cmas: list[str] | None = None,
    dry_run: bool = False,
) -> dict[str, LoadResult]:
    """
    Run all three housing pipelines across selected CMAs.

    Args:
        year:       Reference year (unused, kept for CLI compat).
        start_date: If set, fetch data from this date onwards.
        cmas:       Optional list of CMA names to filter (default: all).
        dry_run:    Transform but do not write to Supabase.

    Returns:
        Dict of {table_name -> LoadResult}.
    """
    configure_logging()
    log.info(
        "housing_pipeline_start",
        cmas=cmas,
        start_date=str(start_date) if start_date else None,
        dry_run=dry_run,
    )

    # Resolve CMA filter
    cmhc_geo_ids: list[int] | None = None
    if cmas:
        cmhc_geo_ids = _resolve_cma_filter(cmas)
        if not cmhc_geo_ids:
            log.error("no_valid_cmas", cmas=cmas)
            return {}
        log.info(
            "cma_filter_resolved",
            n_cmas=len(cmhc_geo_ids),
            geo_ids=cmhc_geo_ids,
        )

    loader: SupabaseLoader | None = None
    run_id: str | None = None

    if not dry_run:
        loader = SupabaseLoader()
        geo_lookup = await loader.build_geo_lookup()
        normalizer = GeoNormalizer()
        normalizer._cache = geo_lookup
        normalizer._loaded = True
        run_id = await loader.start_pipeline_run(
            "housing",
            "CMHC",
            metadata={
                "cmas": cmas,
                "start_date": str(start_date) if start_date else None,
                "dry_run": dry_run,
            },
        )
    else:
        # In dry-run mode, use an identity normalizer (no DB needed)
        normalizer = GeoNormalizer()
        normalizer._cache = {}
        normalizer._loaded = True

    source = CMHCSource()

    results: dict[str, LoadResult] = {}

    try:
        # Fetch all three StatCan datasets in parallel
        vacancy_raw, rents_raw, starts_raw = await asyncio.gather(
            _fetch_dataset(source, "vacancy_rates", normalizer, cmhc_geo_ids, start_date, dry_run=dry_run),
            _fetch_dataset(source, "average_rents", normalizer, cmhc_geo_ids, start_date, dry_run=dry_run),
            _fetch_dataset(source, "housing_starts", normalizer, cmhc_geo_ids, start_date, dry_run=dry_run),
        )

        # Prepare DataFrames for loading
        vacancy_df = _prepare_vacancy_df(vacancy_raw)
        rents_df = _prepare_rents_df(rents_raw)
        starts_df = _prepare_starts_df(starts_raw)

        # Build indicator_values summary
        indicators_df = _build_indicator_values(vacancy_df, rents_df, starts_df)

        # Fetch CMHC API data (starts/completions/under-construction, all CMAs)
        log.info("cmhc_api_fetch_start")
        cmhc_api_df, cmhc_api_errors = await source.extract_cmhc_api(
            cma_names=cmas,
        )

        if dry_run and not cmhc_api_df.is_empty():
            # Group by CMA and print first 5 records per CMA
            for cma_name in cmhc_api_df["cma_name"].unique().sort().to_list():
                subset = cmhc_api_df.filter(pl.col("cma_name") == cma_name).head(5)
                print(f"\n--- {cma_name} (first 5 records) ---")
                for row in subset.iter_rows(named=True):
                    print(
                        f"  {row['year']}-{row['month']:02d} | "
                        f"{row['dwelling_type']:>9s} | "
                        f"{row['data_type']:<20s} | "
                        f"{row['intended_market']:<10s} | "
                        f"{row['value']}"
                    )

        # Upsert to each table
        tables_to_load: list[tuple[str, pl.DataFrame, list[str]]] = [
            ("vacancy_rates", vacancy_df, ["geography_id", "ref_date", "bedroom_type"]),
            ("average_rents", rents_df, ["geography_id", "ref_date", "bedroom_type"]),
            ("housing_starts", starts_df, ["geography_id", "ref_date", "dwelling_type"]),
            ("indicator_values", indicators_df, ["indicator_id", "geography_id", "ref_date"]),
            ("cmhc_housing", cmhc_api_df, [
                "cma_geoid", "year", "month",
                "dwelling_type", "data_type", "intended_market",
            ]),
        ]

        for table, df, conflict_cols in tables_to_load:
            if df is None or (isinstance(df, pl.DataFrame) and df.is_empty()):
                results[table] = LoadResult(table=table)
                continue

            if dry_run:
                log.info("dry_run_skip", table=table, rows=len(df))
                results[table] = LoadResult(table=table, records_loaded=len(df))
                continue

            assert loader is not None
            result = await loader.upsert(table, df, conflict_columns=conflict_cols)
            results[table] = result

        # Summarize
        total_loaded = sum(r.records_loaded for r in results.values())
        total_failed = sum(r.records_failed for r in results.values()) + cmhc_api_errors
        combined_result = LoadResult(
            table="housing_combined",
            records_loaded=total_loaded,
            records_failed=total_failed,
        )
        if loader and run_id:
            await loader.finish_pipeline_run(run_id, combined_result)

        cmhc_api_n_cmas = len(cmas) if cmas else len(CMA_GEOUIDS)
        cmhc_api_inserted = results.get("cmhc_housing", LoadResult(table="cmhc_housing")).records_loaded
        log.info(
            "housing_pipeline_complete",
            total_loaded=total_loaded,
            total_failed=total_failed,
            tables={t: r.records_loaded for t, r in results.items()},
        )
        print(
            f"Processed {cmhc_api_n_cmas} CMAs, "
            f"{cmhc_api_inserted} records inserted, "
            f"{cmhc_api_errors} errors. See cmhc_errors.log."
        )

    except Exception as exc:
        if loader and run_id:
            await loader.fail_pipeline_run(run_id, str(exc))
        raise

    return results
