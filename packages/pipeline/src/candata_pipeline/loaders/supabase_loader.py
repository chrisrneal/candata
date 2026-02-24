"""
loaders/supabase_loader.py — Generic idempotent upsert loader for Supabase.

All pipelines funnel their normalized DataFrames through this module to
write to Supabase. The loader:
  - Converts polars DataFrames to list[dict] (JSON-serialisable)
  - Batches rows to respect Supabase payload limits (~500 rows / 5 MB)
  - Performs upsert (INSERT … ON CONFLICT DO UPDATE) via conflict_columns
  - Handles partial failures: logs failed batches and continues
  - Returns a LoadResult with records_loaded and records_failed counts
  - Records pipeline_runs rows in Supabase for observability

Usage:
    from candata_pipeline.loaders.supabase_loader import SupabaseLoader, LoadResult

    loader = SupabaseLoader()

    result = await loader.upsert(
        table="indicator_values",
        df=df,
        conflict_columns=["indicator_id", "geography_id", "ref_date"],
    )
    print(result.records_loaded, result.records_failed)

    # Or: record a pipeline run
    run_id = await loader.start_pipeline_run("statcan_cpi", "StatCan")
    try:
        result = await loader.upsert(...)
        await loader.finish_pipeline_run(run_id, result, status="success")
    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise
"""

from __future__ import annotations

import math
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import polars as pl
import structlog

from candata_shared.db import get_supabase_client

log = structlog.get_logger(__name__)

BATCH_SIZE = 500     # rows per Supabase request
MAX_PAYLOAD_MB = 4   # stay under Supabase's 5 MB limit


@dataclass
class LoadResult:
    """Summary of a loader upsert operation."""

    table: str
    records_loaded: int = 0
    records_failed: int = 0
    batches_total: int = 0
    batches_failed: int = 0
    errors: list[str] = field(default_factory=list)
    duration_ms: int = 0

    @property
    def success(self) -> bool:
        return self.records_failed == 0

    @property
    def status(self) -> str:
        if self.records_failed == 0:
            return "success"
        if self.records_loaded > 0:
            return "partial_failure"
        return "failure"


class SupabaseLoader:
    """
    Handles all writes to Supabase from the pipeline.

    Uses the service role key so RLS is bypassed for ETL writes.
    """

    def __init__(self, batch_size: int = BATCH_SIZE) -> None:
        self._batch_size = batch_size
        self._client = get_supabase_client(service_role=True)

    # ------------------------------------------------------------------
    # Core upsert
    # ------------------------------------------------------------------

    async def upsert(
        self,
        table: str,
        df: pl.DataFrame,
        conflict_columns: list[str],
        *,
        ignore_columns: list[str] | None = None,
    ) -> LoadResult:
        """
        Upsert all rows from a polars DataFrame into a Supabase table.

        Rows are serialized to dicts with date/UUID values converted to
        strings (Supabase REST accepts ISO strings). Null values are
        omitted from the dict to allow DB-level defaults.

        Args:
            table:            Target table name.
            df:               Normalized polars DataFrame.
            conflict_columns: Columns that identify uniqueness for upsert.
            ignore_columns:   Columns to exclude from the insert dict.

        Returns:
            LoadResult with counts and error list.
        """
        result = LoadResult(table=table)
        t0 = time.monotonic()

        if df.is_empty():
            log.warning("upsert_empty_dataframe", table=table)
            return result

        loader_log = log.bind(table=table, total_rows=len(df))
        loader_log.info("upsert_start")

        # Convert DataFrame to JSON-serialisable dicts
        rows = self._to_dicts(df, ignore_columns=ignore_columns or [])

        # Split into batches
        n_batches = math.ceil(len(rows) / self._batch_size)
        result.batches_total = n_batches

        for batch_idx in range(n_batches):
            start = batch_idx * self._batch_size
            batch = rows[start : start + self._batch_size]

            try:
                self._client.table(table).upsert(
                    batch,
                    on_conflict=",".join(conflict_columns),
                ).execute()
                result.records_loaded += len(batch)
                loader_log.debug(
                    "batch_loaded",
                    batch=batch_idx + 1,
                    n_batches=n_batches,
                    batch_size=len(batch),
                )
            except Exception as exc:
                error_msg = f"Batch {batch_idx + 1}/{n_batches}: {exc}"
                log.error("batch_failed", table=table, batch=batch_idx + 1, error=str(exc))
                result.records_failed += len(batch)
                result.batches_failed += 1
                result.errors.append(error_msg)

        result.duration_ms = int((time.monotonic() - t0) * 1000)
        loader_log.info(
            "upsert_complete",
            records_loaded=result.records_loaded,
            records_failed=result.records_failed,
            duration_ms=result.duration_ms,
            status=result.status,
        )
        return result

    # ------------------------------------------------------------------
    # Pipeline run tracking
    # ------------------------------------------------------------------

    async def start_pipeline_run(
        self,
        pipeline_name: str,
        source_name: str,
        *,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Insert a 'running' pipeline_runs row and return its UUID.

        Args:
            pipeline_name: Logical name of the pipeline (e.g. "statcan_cpi").
            source_name:   Data source name (e.g. "StatCan").
            metadata:      Optional extra JSON metadata.

        Returns:
            pipeline_run_id as string UUID.
        """
        run_id = str(uuid.uuid4())
        self._client.table("pipeline_runs").insert(
            {
                "id": run_id,
                "pipeline_name": pipeline_name,
                "source_name": source_name,
                "status": "running",
                "started_at": datetime.now(timezone.utc).isoformat(),
                "metadata": metadata or {},
            }
        ).execute()
        log.info("pipeline_run_started", run_id=run_id, pipeline=pipeline_name)
        return run_id

    async def finish_pipeline_run(
        self,
        run_id: str,
        result: LoadResult,
        *,
        records_extracted: int | None = None,
        status: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        """
        Update a pipeline_runs row with success/partial_failure outcome.

        Args:
            run_id:             UUID from start_pipeline_run().
            result:             LoadResult from upsert().
            records_extracted:  How many rows were pulled from the source.
            status:             Override status string (default: result.status).
            metadata:           Additional metadata to merge.
        """
        run_status = status or result.status
        now = datetime.now(timezone.utc)

        update: dict[str, Any] = {
            "status": run_status,
            "records_loaded": result.records_loaded,
            "records_rejected": result.records_failed,
            "completed_at": now.isoformat(),
        }
        if records_extracted is not None:
            update["records_extracted"] = records_extracted
        if metadata:
            update["metadata"] = metadata

        self._client.table("pipeline_runs").update(update).eq("id", run_id).execute()
        log.info(
            "pipeline_run_finished",
            run_id=run_id,
            status=run_status,
            records_loaded=result.records_loaded,
        )

    async def fail_pipeline_run(self, run_id: str, error_message: str) -> None:
        """
        Mark a pipeline run as failed with an error message.

        Args:
            run_id:        UUID from start_pipeline_run().
            error_message: Error string to store.
        """
        self._client.table("pipeline_runs").update(
            {
                "status": "failure",
                "error_message": error_message[:2000],  # DB column limit
                "completed_at": datetime.now(timezone.utc).isoformat(),
            }
        ).eq("id", run_id).execute()
        log.error("pipeline_run_failed", run_id=run_id, error=error_message[:200])

    # ------------------------------------------------------------------
    # Geography ID cache lookup
    # ------------------------------------------------------------------

    async def build_geo_lookup(self) -> dict[str, str]:
        """
        Return a dict of {sgc_code → geography_id} from Supabase.

        Cached for the lifetime of this loader instance.
        """
        if not hasattr(self, "_geo_lookup"):
            result = self._client.table("geographies").select("id, sgc_code").execute()
            self._geo_lookup: dict[str, str] = {
                row["sgc_code"]: row["id"] for row in (result.data or [])
            }
            log.debug("geo_lookup_built", count=len(self._geo_lookup))
        return self._geo_lookup

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _to_dicts(
        df: pl.DataFrame,
        *,
        ignore_columns: list[str],
    ) -> list[dict[str, Any]]:
        """
        Convert a polars DataFrame to a JSON-serialisable list of dicts.

        - Date and datetime values → ISO string
        - None/null values omitted (use DB defaults)
        - UUID columns already stored as strings
        """
        ignore_set = set(ignore_columns)
        cols = [c for c in df.columns if c not in ignore_set]
        df_sub = df.select(cols)

        # Cast Date columns to string
        cast_exprs = []
        for col_name in df_sub.columns:
            dtype = df_sub[col_name].dtype
            if dtype == pl.Date:
                cast_exprs.append(pl.col(col_name).cast(pl.String).alias(col_name))
            elif dtype == pl.Datetime:
                cast_exprs.append(
                    pl.col(col_name).dt.strftime("%Y-%m-%dT%H:%M:%SZ").alias(col_name)
                )
        if cast_exprs:
            df_sub = df_sub.with_columns(cast_exprs)

        raw = df_sub.to_dicts()

        # Strip nulls from each dict
        return [{k: v for k, v in row.items() if v is not None} for row in raw]
