"""
candata_pipeline — ETL pipeline workers for the candata platform.

Architecture:
  sources/     — one module per external data provider (StatCan, BoC, CMHC, ...)
  transforms/  — geography normalization, time-series alignment, entity resolution
  loaders/     — idempotent Supabase upserts with batch handling
  pipelines/   — orchestrators that wire sources -> transforms -> loaders
  utils/       — structlog configuration, exponential-backoff retry decorator

Quick start:
    from candata_pipeline.pipelines.economic_pulse import run as run_econ
    import asyncio
    result = asyncio.run(run_econ(dry_run=True))

CLI:
    python scripts/run_pipeline.py economic-pulse --dry-run
    python scripts/run_pipeline.py all
    python scripts/backfill.py economic-pulse --from 2015-01-01

Shared code from candata_shared:
    from candata_shared.config import settings
    from candata_shared.db import get_supabase_client, get_duckdb_connection
    from candata_shared.models.indicators import Indicator, IndicatorValue
    from candata_shared.geo import normalize_statcan_geo
    from candata_shared.constants import PROVINCES, INDICATOR_IDS
"""

__version__ = "0.1.0"
