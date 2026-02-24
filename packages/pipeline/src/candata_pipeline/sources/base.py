"""
sources/base.py — Abstract base class for all data source adapters.

Each concrete source must implement:
  extract()      — fetch raw data, return polars DataFrame
  transform()    — clean/normalize raw DataFrame into standard schema
  get_metadata() — return dict with source info for pipeline_runs table

The run() method orchestrates extract → transform → return and handles
timing/logging automatically. Pipelines call run() rather than the
individual methods.
"""

from __future__ import annotations

import time
from abc import ABC, abstractmethod
from typing import Any

import polars as pl
import structlog

log = structlog.get_logger(__name__)


class BaseSource(ABC):
    """Abstract base for all candata ETL data source adapters."""

    # Override in subclass — used for logging and pipeline_runs.source_name
    name: str = "unknown"

    def __init__(self) -> None:
        self._log = log.bind(source_name=self.name)

    # ------------------------------------------------------------------
    # Abstract interface — subclasses must implement all three
    # ------------------------------------------------------------------

    @abstractmethod
    async def extract(self, **kwargs: Any) -> pl.DataFrame:
        """
        Fetch raw data from the external source.

        Implementations should:
        - Make HTTP calls (via httpx, decorated with @with_retry)
        - Return a raw polars DataFrame with all original columns preserved
        - Cache raw data in DuckDB staging where appropriate

        Args:
            **kwargs: Source-specific parameters (start_date, end_date, etc.)

        Returns:
            Raw polars DataFrame.
        """
        ...

    @abstractmethod
    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        """
        Clean and normalize a raw DataFrame into the standard domain schema.

        Implementations should:
        - Drop metadata/footnote rows
        - Rename columns to snake_case
        - Parse REF_DATE → ISO date (YYYY-MM-DD)
        - Normalize GEO column → sgc_code
        - Handle suppressed values ('x', '..', 'F') → None
        - Return only columns needed by the loader

        Args:
            raw: DataFrame returned by extract().

        Returns:
            Normalized polars DataFrame ready for loading.
        """
        ...

    @abstractmethod
    async def get_metadata(self) -> dict[str, Any]:
        """
        Return source-level metadata for observability.

        Should include at minimum:
          source_name, last_updated, record_count, description

        Returns:
            dict suitable for logging / pipeline_runs.metadata column.
        """
        ...

    # ------------------------------------------------------------------
    # Orchestration — pipelines call this
    # ------------------------------------------------------------------

    async def run(self, **kwargs: Any) -> pl.DataFrame:
        """
        Extract + transform in sequence with timing and structured logging.

        Args:
            **kwargs: Forwarded to extract().

        Returns:
            Transformed polars DataFrame.

        Raises:
            Any exception from extract() or transform() after logging it.
        """
        run_log = self._log.bind(**{k: str(v) for k, v in kwargs.items()})
        run_log.info("source_run_start")

        t0 = time.monotonic()
        try:
            raw = await self.extract(**kwargs)
            extract_ms = int((time.monotonic() - t0) * 1000)
            run_log.info(
                "extract_complete",
                raw_rows=len(raw),
                raw_cols=raw.width,
                duration_ms=extract_ms,
            )

            t1 = time.monotonic()
            result = self.transform(raw)
            transform_ms = int((time.monotonic() - t1) * 1000)
            run_log.info(
                "transform_complete",
                result_rows=len(result),
                result_cols=result.width,
                duration_ms=transform_ms,
            )

            run_log.info(
                "source_run_complete",
                total_duration_ms=int((time.monotonic() - t0) * 1000),
                output_rows=len(result),
            )
            return result

        except Exception as exc:
            run_log.error(
                "source_run_failed",
                error=str(exc),
                duration_ms=int((time.monotonic() - t0) * 1000),
                exc_info=True,
            )
            raise

    # ------------------------------------------------------------------
    # Shared helpers available to all subclasses
    # ------------------------------------------------------------------

    @staticmethod
    def _null_suppressed(series: pl.Series) -> pl.Series:
        """Replace StatCan suppression markers with null."""
        SUPPRESSED = {"x", "..", "F", "E", "r", "p", "...", ""}
        return series.map_elements(
            lambda v: None if (isinstance(v, str) and v.strip() in SUPPRESSED) else v,
            return_dtype=pl.String,
        )

    @staticmethod
    def _to_snake_case(name: str) -> str:
        """Convert 'REF_DATE' or 'Ref Date' to 'ref_date'."""
        import re
        s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
        s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
        return s.lower().replace(" ", "_").replace("-", "_")

    @staticmethod
    def _normalize_columns(df: pl.DataFrame) -> pl.DataFrame:
        """Rename all columns to snake_case, strip whitespace from string cols."""
        import re

        def snake(name: str) -> str:
            s = re.sub(r"([A-Z]+)([A-Z][a-z])", r"\1_\2", name)
            s = re.sub(r"([a-z\d])([A-Z])", r"\1_\2", s)
            return s.lower().strip().replace(" ", "_").replace("-", "_")

        return df.rename({col: snake(col) for col in df.columns})
