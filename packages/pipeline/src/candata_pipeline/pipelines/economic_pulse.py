"""
pipelines/economic_pulse.py — GDP, CPI, employment, and interest rate pipeline.

Orchestrates:
  1. StatCan source → GDP, CPI, Labour Force, Retail Trade (parallel)
  2. Bank of Canada Valet → overnight rate, prime rate, mortgage rate, USD/CAD
  3. Transform → indicator_values schema: (indicator_id, geography_id, ref_date, value)
  4. Upsert → indicator_values table
  5. Record pipeline_run metadata

StatCan tables pulled:
  3610043401 — GDP by industry (monthly)     → gdp_monthly
  1810000401 — CPI all-items (monthly)       → cpi_monthly
  1410028701 — Labour Force Survey (monthly) → unemployment_rate, employment_monthly
  2010000801 — Retail Trade (monthly)        → retail_sales_monthly

BoC series pulled:
  FXUSDCAD   → usdcad
  V39079     → overnight_rate
  V122530    → prime_rate
  V80691338  → mortgage_5yr_fixed

Usage:
    from candata_pipeline.pipelines.economic_pulse import run
    result = await run(start_date=date(2020, 1, 1))
    print(result.records_loaded)
"""

from __future__ import annotations

import asyncio
from datetime import date
from typing import Any

import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.bankofcanada import BankOfCanadaSource
from candata_pipeline.sources.statcan import StatCanSource, TABLE_INDICATOR_MAP
from candata_pipeline.transforms.normalize import GeoNormalizer
from candata_pipeline.transforms.time_series import (
    align_to_period_start,
    deduplicate_series,
)
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="economic_pulse")

# StatCan table → (indicator_id, which VALUE filter if needed)
STATCAN_TABLES: dict[str, dict[str, Any]] = {
    "1810000401": {
        "indicator_id": "cpi_monthly",
        "value_filter": None,
        "frequency": "monthly",
    },
    "3610043401": {
        "indicator_id": "gdp_monthly",
        "value_filter": None,
        "frequency": "monthly",
    },
    "1410028701": {
        "indicator_id": "unemployment_rate",
        "value_filter": None,
        "frequency": "monthly",
    },
    "2010000801": {
        "indicator_id": "retail_sales_monthly",
        "value_filter": None,
        "frequency": "monthly",
    },
}

BOC_SERIES: list[str] = ["FXUSDCAD", "V39079", "V122530", "V80691338"]

# SGC code for Canada (all BoC indicators are national)
CANADA_SGC = "01"


async def _fetch_statcan(
    pid: str,
    cfg: dict[str, Any],
    normalizer: GeoNormalizer,
    start_date: date | None,
) -> pl.DataFrame:
    """Extract + transform one StatCan table into indicator_values rows."""
    source = StatCanSource()
    raw = await source.extract(table_pid=pid)
    df = source.transform(raw, start_date=start_date)

    # Filter to Canada and province-level rows only
    if "geo_level" in df.columns:
        df = df.filter(pl.col("geo_level").is_in(["country", "pr"]))

    # Drop rows with null sgc_code or null value
    df = df.filter(
        pl.col("sgc_code").is_not_null() & pl.col("value").is_not_null()
    )

    # Align to period start
    df = align_to_period_start(df, "ref_date", cfg["frequency"])

    # Add indicator_id
    df = df.with_columns(pl.lit(cfg["indicator_id"]).alias("indicator_id"))

    # Resolve sgc_code → geography_id
    df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
    df = df.filter(pl.col("geography_id").is_not_null())

    # Keep only output columns
    df = df.select(["indicator_id", "geography_id", "ref_date", "value"])

    # Deduplicate
    df = deduplicate_series(df, ["indicator_id", "geography_id", "ref_date"])

    log.info(
        "statcan_table_ready",
        pid=pid,
        indicator=cfg["indicator_id"],
        rows=len(df),
    )
    return df


async def _fetch_boc(
    geo_lookup: dict[str, str],
    start_date: date | None,
    end_date: date | None,
) -> pl.DataFrame:
    """Extract + transform BoC Valet observations into indicator_values rows."""
    source = BankOfCanadaSource()
    raw = await source.extract(series=BOC_SERIES, start_date=start_date, end_date=end_date)
    df = source.transform(raw)

    if df.is_empty():
        return df

    # All BoC indicators are national — assign Canada geography_id
    canada_geo_id = geo_lookup.get(CANADA_SGC)
    if not canada_geo_id:
        log.warning("canada_geo_id_not_found", sgc=CANADA_SGC)
        return pl.DataFrame(
            schema={
                "indicator_id": pl.String,
                "geography_id": pl.String,
                "ref_date": pl.Date,
                "value": pl.Float64,
            }
        )

    df = df.with_columns(pl.lit(canada_geo_id).alias("geography_id"))
    df = df.select(["indicator_id", "geography_id", "ref_date", "value"])
    df = deduplicate_series(df, ["indicator_id", "geography_id", "ref_date"])

    log.info("boc_ready", rows=len(df))
    return df


async def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the economic pulse pipeline end-to-end.

    Args:
        start_date: Earliest ref_date to fetch (None = all available).
        end_date:   Latest ref_date to fetch (None = today).
        dry_run:    If True, transform but do not write to Supabase.

    Returns:
        LoadResult with records_loaded, records_failed, and status.
    """
    configure_logging()
    log.info("economic_pulse_start", start_date=str(start_date), dry_run=dry_run)

    loader = SupabaseLoader()
    geo_lookup = await loader.build_geo_lookup()

    normalizer = GeoNormalizer()
    normalizer._cache = geo_lookup
    normalizer._loaded = True

    run_id = await loader.start_pipeline_run(
        "economic_pulse",
        "StatCan+BoC",
        metadata={"start_date": str(start_date), "dry_run": dry_run},
    )

    try:
        # Fetch all StatCan tables + BoC in parallel
        tasks = [
            _fetch_statcan(pid, cfg, normalizer, start_date)
            for pid, cfg in STATCAN_TABLES.items()
        ]
        tasks.append(_fetch_boc(geo_lookup, start_date, end_date))

        dfs = await asyncio.gather(*tasks, return_exceptions=True)

        # Collect successful results; log failed tasks
        valid_dfs: list[pl.DataFrame] = []
        for i, df_or_exc in enumerate(dfs):
            if isinstance(df_or_exc, Exception):
                log.error("fetch_task_failed", task_index=i, error=str(df_or_exc))
            elif isinstance(df_or_exc, pl.DataFrame) and not df_or_exc.is_empty():
                valid_dfs.append(df_or_exc)

        if not valid_dfs:
            raise RuntimeError("All fetch tasks failed — no data to load.")

        combined = pl.concat(valid_dfs)
        combined = deduplicate_series(
            combined, ["indicator_id", "geography_id", "ref_date"]
        )

        log.info("combined_rows", total=len(combined))

        if dry_run:
            log.info("dry_run_complete", rows=len(combined))
            dummy = LoadResult(table="indicator_values", records_loaded=len(combined))
            await loader.finish_pipeline_run(run_id, dummy, records_extracted=len(combined))
            return dummy

        result = await loader.upsert(
            "indicator_values",
            combined,
            conflict_columns=["indicator_id", "geography_id", "ref_date"],
        )
        await loader.finish_pipeline_run(
            run_id, result, records_extracted=len(combined)
        )
        return result

    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise
