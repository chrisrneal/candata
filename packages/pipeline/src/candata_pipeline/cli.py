"""
cli.py — Click CLI entrypoint for pipeline workers.

Usage:
    pipeline run statcan-cpi
    pipeline run all
    pipeline status
"""

from __future__ import annotations

import click
import structlog

from candata_shared.config import settings

log = structlog.get_logger(__name__)


@click.group()
@click.option(
    "--log-level",
    default=settings.log_level,
    type=click.Choice(["DEBUG", "INFO", "WARNING", "ERROR"]),
    help="Log level",
)
def main(log_level: str) -> None:
    """candata ETL pipeline workers."""
    import logging
    logging.basicConfig(level=getattr(logging, log_level))


@main.command()
@click.argument(
    "pipeline",
    type=click.Choice(
        ["statcan-cpi", "statcan-gdp", "statcan-labour", "boc-rates", "cmhc-housing",
         "procurement-contracts", "trade-flows", "all"],
        case_sensitive=False,
    ),
)
def run(pipeline: str) -> None:
    """Run a named pipeline or 'all' to run all pipelines."""
    import asyncio

    click.echo(f"Running pipeline: {pipeline}")
    log.info("pipeline_start", pipeline=pipeline)

    # Placeholder — individual pipeline modules go here
    click.echo(f"  [stub] Pipeline '{pipeline}' complete.")
    log.info("pipeline_complete", pipeline=pipeline)


@main.command()
def status() -> None:
    """Show the last run status of each pipeline."""
    from candata_shared.db import get_supabase_client

    click.echo("Pipeline status:")
    try:
        client = get_supabase_client(service_role=True)
        result = (
            client.table("pipeline_runs")
            .select("pipeline_name,status,started_at,records_loaded,duration_ms")
            .order("started_at", desc=True)
            .limit(20)
            .execute()
        )
        if not result.data:
            click.echo("  No pipeline runs found.")
            return
        for row in result.data:
            status_emoji = {"success": "✓", "failure": "✗", "running": "⟳", "partial_failure": "⚠"}.get(
                row["status"], "?"
            )
            click.echo(
                f"  {status_emoji} {row['pipeline_name']:30s} "
                f"{row['status']:16s} "
                f"{row.get('records_loaded', '?')} rows  "
                f"{row.get('started_at', '')[:19]}"
            )
    except Exception as exc:
        click.echo(f"  Error fetching status: {exc}", err=True)


if __name__ == "__main__":
    main()
