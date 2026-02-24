#!/usr/bin/env python3
"""
scripts/backfill.py — Historical data backfill for one-time or periodic loads.

Runs the specified pipelines over a historical date range in configurable
yearly or monthly chunks to avoid oversized requests and allow resumability.

Usage:
    python scripts/backfill.py economic-pulse --from 2010-01-01 --to 2024-12-31
    python scripts/backfill.py housing --from 2005 --to 2023 --chunk-size year
    python scripts/backfill.py trade --from 2015-01-01 --chunk-size month
    python scripts/backfill.py all --from 2015-01-01 --dry-run

Chunk sizes:
    year   — Requests one calendar year at a time (default)
    month  — Requests one month at a time (slower but safer for large tables)

The backfill script is idempotent: all writes use upsert so re-running will
update existing rows and add new ones without creating duplicates.
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date, timedelta
from typing import Generator


def parse_date(s: str) -> date:
    """Accept YYYY-MM-DD or YYYY (defaults to Jan 1)."""
    if len(s) == 4:
        return date(int(s), 1, 1)
    return date.fromisoformat(s)


def year_chunks(
    start: date, end: date
) -> Generator[tuple[date, date], None, None]:
    """Yield (chunk_start, chunk_end) pairs, one per calendar year."""
    year = start.year
    while True:
        chunk_start = date(year, 1, 1) if year > start.year else start
        chunk_end = min(date(year, 12, 31), end)
        yield chunk_start, chunk_end
        if chunk_end >= end:
            break
        year += 1


def month_chunks(
    start: date, end: date
) -> Generator[tuple[date, date], None, None]:
    """Yield (chunk_start, chunk_end) pairs, one per calendar month."""
    import calendar
    current = date(start.year, start.month, 1)
    while current <= end:
        _, last_day = calendar.monthrange(current.year, current.month)
        chunk_end = min(date(current.year, current.month, last_day), end)
        chunk_start = max(current, start)
        yield chunk_start, chunk_end
        # Advance to first day of next month
        if current.month == 12:
            current = date(current.year + 1, 1, 1)
        else:
            current = date(current.year, current.month + 1, 1)


async def backfill_pipeline(
    pipeline: str,
    start: date,
    end: date,
    chunk_size: str,
    dry_run: bool,
) -> None:
    """Run a single pipeline over all chunks in the date range."""
    import structlog
    log = structlog.get_logger("backfill")

    chunks = list(year_chunks(start, end) if chunk_size == "year" else month_chunks(start, end))
    log.info(
        "backfill_start",
        pipeline=pipeline,
        start=str(start),
        end=str(end),
        chunk_size=chunk_size,
        n_chunks=len(chunks),
        dry_run=dry_run,
    )

    for i, (chunk_start, chunk_end) in enumerate(chunks):
        log.info(
            "chunk_start",
            pipeline=pipeline,
            chunk=f"{i + 1}/{len(chunks)}",
            chunk_start=str(chunk_start),
            chunk_end=str(chunk_end),
        )
        try:
            if pipeline == "economic-pulse":
                from candata_pipeline.pipelines.economic_pulse import run
                result = await run(start_date=chunk_start, end_date=chunk_end, dry_run=dry_run)
                log.info("chunk_done", records=result.records_loaded, status=result.status)

            elif pipeline == "housing":
                from candata_pipeline.pipelines.housing import run
                results = await run(year=chunk_start.year, dry_run=dry_run)
                total = sum(r.records_loaded for r in results.values())
                log.info("chunk_done", records=total)

            elif pipeline == "procurement":
                from candata_pipeline.pipelines.procurement import run
                results = await run(dry_run=dry_run)
                total = sum(r.records_loaded for r in results.values())
                log.info("chunk_done", records=total)

            elif pipeline == "trade":
                from candata_pipeline.pipelines.trade import run
                result = await run(start_date=chunk_start, end_date=chunk_end, dry_run=dry_run)
                log.info("chunk_done", records=result.records_loaded, status=result.status)

        except Exception as exc:
            log.error(
                "chunk_failed",
                pipeline=pipeline,
                chunk_start=str(chunk_start),
                error=str(exc),
                exc_info=True,
            )
            # Continue with next chunk to maximize data coverage

    log.info("backfill_complete", pipeline=pipeline, n_chunks=len(chunks))


async def main_async(args: argparse.Namespace) -> int:
    from candata_pipeline.utils.logging import configure_logging
    configure_logging(log_level=args.log_level)

    pipelines = (
        ["economic-pulse", "housing", "procurement", "trade"]
        if args.pipeline == "all"
        else [args.pipeline]
    )

    for pipeline in pipelines:
        await backfill_pipeline(
            pipeline=pipeline,
            start=args.start,
            end=args.end,
            chunk_size=args.chunk_size,
            dry_run=args.dry_run,
        )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="backfill",
        description="Historical data backfill for candata pipelines",
    )
    parser.add_argument(
        "pipeline",
        choices=["economic-pulse", "housing", "procurement", "trade", "all"],
    )
    parser.add_argument(
        "--from", dest="start",
        type=parse_date,
        required=True,
        metavar="YYYY or YYYY-MM-DD",
        help="Backfill start date",
    )
    parser.add_argument(
        "--to", dest="end",
        type=parse_date,
        default=date.today(),
        metavar="YYYY or YYYY-MM-DD",
        help="Backfill end date (default: today)",
    )
    parser.add_argument(
        "--chunk-size",
        choices=["year", "month"],
        default="year",
        help="How to split the date range (default: year)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Extract and transform but do not write to Supabase",
    )
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        default="INFO",
    )
    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(main_async(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
