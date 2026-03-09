"""
pipelines/trade.py — Canadian international merchandise trade pipeline.

Ingests two StatCan CIMT tables:
  - Table 12-10-0121-01: commodity trade by NAPCS code (dollar values)
  - Table 12-10-0011-01: aggregate trade by principal trading partner

Upserts to trade_flows with composite unique key:
  (direction, hs_code, partner_country, province, ref_date)

Note: Table 12-10-0126-01 (price indexes) is intentionally NOT used —
its VALUE column contains index values, not dollar amounts.

Usage:
    from candata_pipeline.pipelines.trade import run
    result = await run(start_date=date(2020, 1, 1), dry_run=True)

CLI:
    python scripts/run_pipeline.py trade
    python scripts/run_pipeline.py trade --start-date 2020-01-01
    python scripts/run_pipeline.py trade --dry-run
    python scripts/run_pipeline.py trade --debug
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
    debug: bool = False,
) -> LoadResult:
    """Fetch, transform, and upsert commodity trade data (table 12-10-0121)."""
    raw = await source.extract(table_pid="12100121")
    if debug:
        print(f"\n--- Commodity trade (12-10-0121) ---")
        print(f"  Downloaded: {len(raw):,} raw rows")

    df = source.transform(raw, start_date=start_date, end_date=end_date, debug=debug)

    if df.is_empty():
        log.warning("commodity_trade_empty")
        if debug:
            print("  RESULT: 0 rows after transform (empty)")
        return LoadResult(table="trade_flows")

    df = deduplicate_series(df, CONFLICT_COLUMNS, keep="last")
    log.info("commodity_trade_ready", rows=len(df))
    if debug:
        print(f"  After dedup: {len(df):,} rows")

    if dry_run:
        return LoadResult(table="trade_flows", records_loaded=len(df))

    return await loader.upsert("trade_flows", df, conflict_columns=CONFLICT_COLUMNS)


async def _load_partner_trade(
    source: TradeSource,
    loader: SupabaseLoader,
    start_date: date | None,
    end_date: date | None,
    dry_run: bool,
    debug: bool = False,
) -> LoadResult:
    """Fetch, transform, and upsert partner-level trade data (table 12-10-0011)."""
    raw = await source.extract(table_pid="12100011")
    if debug:
        print(f"\n--- Partner trade (12-10-0011) ---")
        print(f"  Downloaded: {len(raw):,} raw rows")

    df = source.transform_partner(raw, start_date=start_date, end_date=end_date, debug=debug)

    if df.is_empty():
        log.warning("partner_trade_empty")
        if debug:
            print("  RESULT: 0 rows after transform (empty)")
        return LoadResult(table="trade_flows")

    df = deduplicate_series(df, CONFLICT_COLUMNS, keep="last")
    log.info("partner_trade_ready", rows=len(df))
    if debug:
        print(f"  After dedup: {len(df):,} rows")

    if dry_run:
        return LoadResult(table="trade_flows", records_loaded=len(df))

    return await loader.upsert("trade_flows", df, conflict_columns=CONFLICT_COLUMNS)


async def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dry_run: bool = False,
    debug: bool = False,
) -> LoadResult:
    """
    Run the trade pipeline.

    Downloads commodity and partner trade tables from StatCan, transforms
    to the trade_flows schema, and upserts to Supabase.

    Args:
        start_date: Earliest reference date to include (optional).
        end_date:   Latest reference date to include (optional).
        dry_run:    Transform but do not write to Supabase.
        debug:      Print row counts at each transform stage.

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

    source = TradeSource()

    loader: SupabaseLoader | None = None
    run_id: str | None = None

    if not dry_run:
        loader = SupabaseLoader()
        run_id = await loader.start_pipeline_run(
            "trade",
            "StatCan-Trade",
            metadata={
                "start_date": str(start_date) if start_date else None,
                "end_date": str(end_date) if end_date else None,
            },
        )

    try:
        commodity_result = await _load_commodity_trade(
            source, loader, start_date, end_date, dry_run, debug
        )
        partner_result = await _load_partner_trade(
            source, loader, start_date, end_date, dry_run, debug
        )

        combined = LoadResult(
            table="trade_flows",
            records_loaded=commodity_result.records_loaded + partner_result.records_loaded,
            records_failed=commodity_result.records_failed + partner_result.records_failed,
        )
        if loader and run_id:
            await loader.finish_pipeline_run(
                run_id,
                combined,
                metadata={
                    "commodity_rows": commodity_result.records_loaded,
                    "partner_rows": partner_result.records_loaded,
                },
            )

    except Exception as exc:
        if loader and run_id:
            await loader.fail_pipeline_run(run_id, str(exc))
        raise

    if debug:
        print(f"\n{'='*50}")
        print(f"  TOTAL: {combined.records_loaded:,} loaded, {combined.records_failed:,} failed")
        print(f"    commodity: {commodity_result.records_loaded:,}")
        print(f"    partner:   {partner_result.records_loaded:,}")
        print(f"{'='*50}")

    log.info(
        "trade_pipeline_complete",
        records_loaded=combined.records_loaded,
        records_failed=combined.records_failed,
        status=combined.status,
    )
    return combined
