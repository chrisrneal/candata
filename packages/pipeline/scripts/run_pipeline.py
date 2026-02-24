#!/usr/bin/env python3
"""
scripts/run_pipeline.py — CLI entry point for candata ETL pipelines.

Usage:
    python scripts/run_pipeline.py economic-pulse
    python scripts/run_pipeline.py housing --year 2023
    python scripts/run_pipeline.py procurement --datasets contracts tenders
    python scripts/run_pipeline.py trade --start-date 2020-01-01
    python scripts/run_pipeline.py all --dry-run
    python scripts/run_pipeline.py all --backfill --start-date 2015-01-01

Available pipelines:
    economic-pulse  — GDP, CPI, employment, interest rates
    housing         — CMHC vacancy rates, rents, housing starts
    procurement     — Federal contracts and tenders
    trade           — Import/export by HS code and province
    all             — Run all pipelines sequentially
"""

from __future__ import annotations

import argparse
import asyncio
import sys
from datetime import date


def parse_date(s: str) -> date:
    try:
        return date.fromisoformat(s)
    except ValueError:
        raise argparse.ArgumentTypeError(f"Invalid date: {s!r} — expected YYYY-MM-DD")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="candata ETL pipeline runner",
    )
    parser.add_argument(
        "pipeline",
        choices=["economic-pulse", "housing", "procurement", "trade", "all"],
        help="Pipeline to run",
    )
    parser.add_argument(
        "--start-date",
        type=parse_date,
        default=None,
        metavar="YYYY-MM-DD",
        help="Earliest reference date to fetch (default: source-defined)",
    )
    parser.add_argument(
        "--end-date",
        type=parse_date,
        default=None,
        metavar="YYYY-MM-DD",
        help="Latest reference date to fetch (default: today)",
    )
    parser.add_argument(
        "--year",
        type=int,
        default=None,
        help="Reference year (housing pipeline)",
    )
    parser.add_argument(
        "--datasets",
        nargs="+",
        choices=["contracts", "tenders"],
        default=None,
        help="Procurement datasets to run (default: both)",
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


async def run_pipeline(args: argparse.Namespace) -> int:
    """Dispatch to the appropriate pipeline and return exit code."""
    from candata_pipeline.utils.logging import configure_logging
    configure_logging(log_level=args.log_level)

    import structlog
    log = structlog.get_logger("run_pipeline")

    pipeline = args.pipeline
    log.info("pipeline_dispatch", pipeline=pipeline, dry_run=args.dry_run)

    try:
        if pipeline == "economic-pulse":
            from candata_pipeline.pipelines.economic_pulse import run
            result = await run(
                start_date=args.start_date,
                end_date=args.end_date,
                dry_run=args.dry_run,
            )
            log.info("done", records_loaded=result.records_loaded, status=result.status)

        elif pipeline == "housing":
            from candata_pipeline.pipelines.housing import run
            results = await run(
                year=args.year,
                start_date=args.start_date,
                dry_run=args.dry_run,
            )
            total = sum(r.records_loaded for r in results.values())
            log.info("done", total_records_loaded=total, tables=list(results.keys()))

        elif pipeline == "procurement":
            from candata_pipeline.pipelines.procurement import run
            results = await run(
                datasets=args.datasets,
                dry_run=args.dry_run,
            )
            total = sum(r.records_loaded for r in results.values())
            log.info("done", total_records_loaded=total)

        elif pipeline == "trade":
            from candata_pipeline.pipelines.trade import run
            result = await run(
                start_date=args.start_date,
                end_date=args.end_date,
                dry_run=args.dry_run,
            )
            log.info("done", records_loaded=result.records_loaded, status=result.status)

        elif pipeline == "all":
            await run_all(args)

    except Exception as exc:
        log.error("pipeline_failed", pipeline=pipeline, error=str(exc), exc_info=True)
        return 1

    return 0


async def run_all(args: argparse.Namespace) -> None:
    """Run all pipelines sequentially, continuing on non-fatal errors."""
    import structlog
    log = structlog.get_logger("run_all")

    from candata_pipeline.pipelines import economic_pulse, housing, procurement, trade

    pipelines = [
        ("economic-pulse", economic_pulse.run, {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "dry_run": args.dry_run,
        }),
        ("housing", housing.run, {
            "year": args.year,
            "start_date": args.start_date,
            "dry_run": args.dry_run,
        }),
        ("procurement", procurement.run, {
            "dry_run": args.dry_run,
        }),
        ("trade", trade.run, {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "dry_run": args.dry_run,
        }),
    ]

    for name, runner, kwargs in pipelines:
        log.info("starting_pipeline", pipeline=name)
        try:
            result = await runner(**kwargs)
            if isinstance(result, dict):
                total = sum(r.records_loaded for r in result.values())
                log.info("pipeline_done", pipeline=name, records_loaded=total)
            else:
                log.info("pipeline_done", pipeline=name, records_loaded=result.records_loaded)
        except Exception as exc:
            log.error("pipeline_error", pipeline=name, error=str(exc))
            # Continue with remaining pipelines


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    exit_code = asyncio.run(run_pipeline(args))
    sys.exit(exit_code)


if __name__ == "__main__":
    main()
