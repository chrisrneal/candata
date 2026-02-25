#!/usr/bin/env python3
"""
scripts/run_pipeline.py — CLI entry point for candata ETL pipelines.

Usage:
    python scripts/run_pipeline.py economic-pulse
    python scripts/run_pipeline.py housing
    python scripts/run_pipeline.py housing --cmas toronto,vancouver
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


def parse_tables(s: str) -> list[str]:
    return [t.strip() for t in s.split(",") if t.strip()]

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="run_pipeline",
        description="candata ETL pipeline runner",
    )
    parser.add_argument(
        "pipeline",
        choices=["economic-pulse", "housing", "procurement", "trade", "trade-hs6", "comtrade", "all"],
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
        "--fiscal-year",
        type=str,
        default=None,
        metavar="YYYY-YYYY",
        help="Filter procurement contracts to a fiscal year (e.g. 2024-2025)",
    )
    parser.add_argument(
        "--from-year",
        type=int,
        default=None,
        help="Start year for trade-hs6 pipeline (default: 2019)",
    )
    parser.add_argument(
        "--to-year",
        type=int,
        default=None,
        help="End year for trade-hs6 pipeline (default: current year)",
    )
    parser.add_argument(
        "--level",
        choices=["hs2", "hs6"],
        default="hs2",
        help="Product code level for comtrade pipeline (default: hs2)",
    )
    parser.add_argument(
        "--years",
        type=str,
        default=None,
        metavar="YEARS",
        help="Years for comtrade pipeline as range (2019-2023) or list (2019,2020)",
    )
    parser.add_argument(
        "--partners",
        type=str,
        default=None,
        metavar="CODES",
        help="Comma-separated ISO partner codes for comtrade pipeline",
    )
    parser.add_argument(
        "--province",
        type=str,
        default=None,
        metavar="NAME",
        help="Province filter for trade-hs6 pipeline (default: all)",
    )
    parser.add_argument(
        "--tables",
        type=parse_tables,
        default=None,
        metavar="TABLE[,TABLE...]",
        help="Comma-separated StatCan table aliases for economic-pulse (gdp,cpi,unemployment,retail)",
    )
    parser.add_argument(
        "--cmas",
        type=parse_tables,
        default=None,
        metavar="CMA[,CMA...]",
        help="Comma-separated CMA names for housing pipeline (e.g. toronto,vancouver)",
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
                tables=args.tables,
            )
            log.info("done", records_loaded=result.records_loaded, status=result.status)

        elif pipeline == "housing":
            from candata_pipeline.pipelines.housing import run
            results = await run(
                year=args.year,
                start_date=args.start_date,
                cmas=args.cmas,
                dry_run=args.dry_run,
            )
            total = sum(r.records_loaded for r in results.values())
            log.info("done", total_records_loaded=total, tables=list(results.keys()))

        elif pipeline == "procurement":
            from candata_pipeline.pipelines.procurement import run
            results = await run(
                datasets=args.datasets,
                fiscal_year=args.fiscal_year,
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

        elif pipeline == "trade-hs6":
            from candata_pipeline.pipelines.statcan_trade_hs6 import run
            result = await run(
                from_year=args.from_year or 2019,
                to_year=args.to_year,
                province=args.province,
                dry_run=args.dry_run,
            )
            log.info("done", records_loaded=result.records_loaded, status=result.status)

        elif pipeline == "comtrade":
            from candata_pipeline.pipelines.un_comtrade import run, _parse_int_list
            comtrade_years = _parse_int_list(args.years) if args.years else None
            comtrade_partners = (
                [int(p.strip()) for p in args.partners.split(",")]
                if args.partners else None
            )
            result = await run(
                level=args.level,
                partners=comtrade_partners,
                years=comtrade_years,
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
    from candata_pipeline.pipelines import statcan_trade_hs6

    pipelines = [
        ("economic-pulse", economic_pulse.run, {
            "start_date": args.start_date,
            "end_date": args.end_date,
            "dry_run": args.dry_run,
        }),
        ("housing", housing.run, {
            "year": args.year,
            "start_date": args.start_date,
            "cmas": args.cmas,
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
        ("trade-hs6", statcan_trade_hs6.run, {
            "from_year": args.from_year or 2019,
            "to_year": args.to_year,
            "province": args.province,
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
