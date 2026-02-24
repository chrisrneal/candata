"""
pipelines/trade.py — Canadian international merchandise trade pipeline.

Ingests two StatCan CIMT tables:
  - Table 12-10-0011-01: commodity trade by HS code (imports/exports)
  - Table 12-10-0126-01: bilateral trade by partner country

Upserts to trade_flows with composite unique key:
  (direction, hs_code, partner_country, province, ref_date)

Usage:
    from candata_pipeline.pipelines.trade import run
    result = await run(start_date=date(2020, 1, 1), dry_run=True)

CLI:
    python scripts/run_pipeline.py trade
    python scripts/run_pipeline.py trade --start-date 2020-01-01
    python scripts/run_pipeline.py trade --dry-run
"""

from __future__ import annotations

from datetime import date

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.trade import TradeSource
from candata_pipeline.transforms.time_series import deduplicate_series
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="trade")

CONFLICT_COLUMNS = ["direction", "hs_code", "partner_country", "province", "ref_date"]


async def _load_commodity_trade(
    source: TradeSource,
    loader: SupabaseLoader,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
) -> LoadResult:
    """Fetch, transform, and upsert commodity trade data (table 12-10-0011)."""
    raw = await source.extract(table_pid="12100011")
    df = source.transform(raw, start_date=start_date, end_date=end_date)

    if df.is_empty():
        log.warning("commodity_trade_empty")
        return LoadResult(table="trade_flows")

    df = deduplicate_series(df, CONFLICT_COLUMNS, keep="last")
    log.info("commodity_trade_ready", rows=len(df))

    if dry_run:
        return LoadResult(table="trade_flows", records_loaded=len(df))

    return await loader.upsert("trade_flows", df, conflict_columns=CONFLICT_COLUMNS)


async def _load_bilateral_trade(
    source: TradeSource,
    loader: SupabaseLoader,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
) -> LoadResult:
    """Fetch, transform, and upsert bilateral trade data (table 12-10-0126)."""
    raw = await source.extract(table_pid="12100126")
    df = source.transform_bilateral(raw, start_date=start_date, end_date=end_date)

    if df.is_empty():
        log.warning("bilateral_trade_empty")
        return LoadResult(table="trade_flows")

    df = deduplicate_series(df, CONFLICT_COLUMNS, keep="last")
    log.info("bilateral_trade_ready", rows=len(df))

    if dry_run:
        return LoadResult(table="trade_flows", records_loaded=len(df))

    return await loader.upsert("trade_flows", df, conflict_columns=CONFLICT_COLUMNS)


async def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the trade pipeline.

    Downloads commodity and bilateral trade tables from StatCan, transforms
    to the trade_flows schema, and upserts to Supabase.

    Args:
        start_date: Earliest reference date to include (optional).
        end_date:   Latest reference date to include (optional).
        dry_run:    Transform but do not write to Supabase.

    Returns:
        Combined LoadResult across both tables.
    """
    configure_logging()
    log.info(
        "trade_pipeline_start",
        start_date=str(start_date) if start_date else None,
        end_date=str(end_date) if end_date else None,
        dry_run=dry_run,
    )

    loader = SupabaseLoader()
    source = TradeSource()

    run_id = await loader.start_pipeline_run(
        "trade",
        "StatCan-Trade",
        metadata={
            "start_date": str(start_date) if start_date else None,
            "end_date": str(end_date) if end_date else None,
            "dry_run": dry_run,
        },
    )

    try:
        commodity_result = await _load_commodity_trade(
            source, loader, start_date, end_date, dry_run
        )
        bilateral_result = await _load_bilateral_trade(
            source, loader, start_date, end_date, dry_run
        )

        combined = LoadResult(
            table="trade_flows",
            records_loaded=commodity_result.records_loaded + bilateral_result.records_loaded,
            records_failed=commodity_result.records_failed + bilateral_result.records_failed,
        )
        await loader.finish_pipeline_run(
            run_id,
            combined,
            metadata={
                "commodity_rows": commodity_result.records_loaded,
                "bilateral_rows": bilateral_result.records_loaded,
            },
        )

    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise

    log.info(
        "trade_pipeline_complete",
        records_loaded=combined.records_loaded,
        records_failed=combined.records_failed,
        status=combined.status,
    )
    return combined
