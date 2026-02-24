"""
utils/logging.py — structlog configuration for the pipeline.

Sets up structured logging with JSON or human-readable console output
controlled by settings.log_format. Call configure_logging() once at
process startup (done automatically by the CLI).

Usage:
    from candata_pipeline.utils.logging import configure_logging, get_logger

    configure_logging()
    log = get_logger("candata_pipeline.sources.statcan")
    log.info("extract_start", table_pid="3610043401", start_date="2020-01-01")

    # Bind pipeline-wide context for all subsequent log calls:
    log = log.bind(source_name="StatCan", pipeline_run_id=str(run_id))
    log.info("rows_extracted", count=1234)
"""

from __future__ import annotations

import logging
import sys
from typing import Any

import structlog

from candata_shared.config import settings

# ---------------------------------------------------------------------------
# Public helpers
# ---------------------------------------------------------------------------


def configure_logging(
    log_level: str | None = None,
    log_format: str | None = None,
) -> None:
    """
    Configure structlog for the pipeline process.

    Should be called once at startup. Idempotent.

    Args:
        log_level:  Override settings.log_level ("DEBUG", "INFO", …).
        log_format: Override settings.log_format ("json" | "console").
    """
    level = log_level or settings.log_level
    fmt = log_format or settings.log_format

    # Standard library logging integration
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=getattr(logging, level.upper(), logging.INFO),
    )

    # Shared processors used in both modes
    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.StackInfoRenderer(),
    ]

    if fmt == "json":
        renderer: Any = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.processors.format_exc_info,
            renderer,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


def get_logger(name: str, **initial_values: Any) -> structlog.BoundLogger:
    """
    Return a bound structlog logger with optional initial context values.

    Args:
        name:           Logger name (conventionally the module __name__).
        **initial_values: Key-value pairs merged into every log record.

    Returns:
        structlog.BoundLogger
    """
    logger = structlog.get_logger(name)
    if initial_values:
        logger = logger.bind(**initial_values)
    return logger  # type: ignore[return-value]
