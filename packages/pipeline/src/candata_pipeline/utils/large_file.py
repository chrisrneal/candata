"""
utils/large_file.py — Robust large-file handling for memory-safe pipeline ingestion.

Provides:
  - download_with_resume()  — HTTP Range-based resumable downloads
  - stream_csv_chunks()     — Chunked CSV reading via polars (never loads full file)
  - upsert_chunk()          — Batched Supabase upserts with error tolerance
  - get_or_download()       — Cached downloads with age-based expiry
  - estimate_csv_rows()     — Row-count estimation without loading the file
  - check_available_memory() — RAM guard
  - monitor_memory()        — Decorator printing memory delta
"""

from __future__ import annotations

import functools
import json
import os
import time
from collections.abc import Generator
from pathlib import Path
from typing import Any

import httpx
import polars as pl
import psutil
from tqdm import tqdm

import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# download_with_resume
# ---------------------------------------------------------------------------


def download_with_resume(
    url: str,
    dest_path: str | Path,
    chunk_size: int = 8192,
) -> Path:
    """Download a file to disk using HTTP Range requests for resume support.

    If *dest_path* already exists, sends a ``Range`` header to resume from
    the current file size.  Falls back to a fresh download if the server
    returns 200 instead of 206.  Retries up to 5 times on network errors
    with a 10-second wait between attempts.

    Returns the final file path.
    """
    dest = Path(dest_path)
    dest.parent.mkdir(parents=True, exist_ok=True)

    max_retries = 5
    retry_wait = 10

    for attempt in range(1, max_retries + 1):
        try:
            existing_size = dest.stat().st_size if dest.exists() else 0
            headers: dict[str, str] = {}
            if existing_size > 0:
                headers["Range"] = f"bytes={existing_size}-"
                print(f"Resuming from {existing_size / 1_048_576:.1f}MB")
            else:
                print("Starting fresh download")

            with httpx.Client(timeout=300.0, follow_redirects=True) as client:
                with client.stream("GET", url, headers=headers) as resp:
                    if resp.status_code == 200:
                        # Server doesn't support Range — restart
                        if existing_size > 0:
                            dest.unlink(missing_ok=True)
                            existing_size = 0
                        mode = "wb"
                    elif resp.status_code == 206:
                        mode = "ab"
                    else:
                        resp.raise_for_status()
                        mode = "wb"

                    # Determine total size for the progress bar
                    content_length = resp.headers.get("content-length")
                    if content_length:
                        total = int(content_length) + existing_size
                    else:
                        total = None

                    with (
                        tqdm(
                            total=total,
                            initial=existing_size,
                            unit="B",
                            unit_scale=True,
                            unit_divisor=1_048_576,
                            desc=dest.name,
                        ) as pbar,
                        open(dest, mode) as f,
                    ):
                        for chunk in resp.iter_bytes(chunk_size=chunk_size):
                            f.write(chunk)
                            pbar.update(len(chunk))

            return dest

        except (httpx.HTTPError, OSError) as exc:
            if attempt < max_retries:
                log.warning(
                    "download_retry",
                    attempt=attempt,
                    max_retries=max_retries,
                    error=str(exc),
                )
                print(f"Download error (attempt {attempt}/{max_retries}): {exc}")
                time.sleep(retry_wait)
            else:
                raise


# ---------------------------------------------------------------------------
# stream_csv_chunks
# ---------------------------------------------------------------------------


# Map pandas-style dtype strings to polars types for backwards compatibility
_DTYPE_MAP: dict[str, pl.DataType] = {
    "float32": pl.Float32,
    "float64": pl.Float64,
    "int32": pl.Int32,
    "int64": pl.Int64,
    "str": pl.Utf8,
    "string": pl.Utf8,
    "object": pl.Utf8,
}


def stream_csv_chunks(
    filepath: str | Path,
    chunksize: int = 50_000,
    dtype: dict[str, str] | None = None,
    usecols: list[str] | None = None,
    skiprows: int | None = None,
    start_row: int = 0,
) -> Generator[pl.DataFrame, None, None]:
    """Yield polars DataFrames in chunks from a large CSV file.

    Uses ``pl.read_csv_batched`` so the full file is never loaded into
    memory at once.

    Args:
        filepath:   Path to the CSV file.
        chunksize:  Number of rows per chunk.
        dtype:      Column dtype overrides (pandas-style strings accepted).
        usecols:    Columns to load (reduces memory).
        skiprows:   Header rows to skip.
        start_row:  Skip chunks until this cumulative row index is reached
                    (for checkpoint-based resumption).

    Yields:
        One ``pl.DataFrame`` per chunk.
    """
    filepath = Path(filepath)
    total_rows = 0
    chunk_count = 0

    schema_overrides: dict[str, pl.DataType] | None = None
    if dtype:
        schema_overrides = {
            col: _DTYPE_MAP.get(dt, pl.Utf8) for col, dt in dtype.items()
        }

    try:
        reader = pl.read_csv_batched(
            filepath,
            batch_size=chunksize,
            schema_overrides=schema_overrides,
            columns=usecols,
            skip_rows=skiprows or 0,
            ignore_errors=True,
            low_memory=True,
            truncate_ragged_lines=True,
        )
    except Exception as exc:
        log.error("csv_open_failed", filepath=str(filepath), error=str(exc))
        raise

    while True:
        batches = reader.next_batches(1)
        if not batches:
            break
        for chunk in batches:
            total_rows += len(chunk)

            # Skip chunks that were already processed (checkpoint resume)
            if total_rows <= start_row:
                continue

            chunk_count += 1
            if chunk_count % 10 == 0:
                print(f"Processed {total_rows:,} rows...")

            yield chunk

    print(f"Finished reading CSV: {total_rows:,} total rows in {chunk_count} chunks")


# ---------------------------------------------------------------------------
# upsert_chunk
# ---------------------------------------------------------------------------


def upsert_chunk(
    supabase_client: Any,
    table_name: str,
    records: list[dict[str, Any]],
    conflict_columns: list[str],
    batch_size: int = 500,
) -> tuple[int, int]:
    """Upsert a list of dicts to Supabase in batches.

    Args:
        supabase_client: A Supabase client instance.
        table_name:      Target table name.
        records:         Rows to upsert (list of dicts).
        conflict_columns: Columns forming the unique constraint.
        batch_size:      Max rows per Supabase request (default 500).

    Returns:
        ``(inserted_count, error_count)`` tuple.
    """
    inserted = 0
    errors = 0
    on_conflict = ",".join(conflict_columns)

    for i in range(0, len(records), batch_size):
        batch = records[i : i + batch_size]
        try:
            supabase_client.table(table_name).upsert(
                batch, on_conflict=on_conflict
            ).execute()
            inserted += len(batch)
        except Exception as exc:
            errors += len(batch)
            first_record = batch[0] if batch else {}
            log.error(
                "upsert_batch_failed",
                table=table_name,
                batch_start=i,
                batch_size=len(batch),
                error=str(exc),
                first_record=first_record,
            )

    return inserted, errors


# ---------------------------------------------------------------------------
# get_or_download
# ---------------------------------------------------------------------------


def get_or_download(
    url: str,
    cache_dir: str | Path,
    filename: str,
    max_age_hours: float = 24,
) -> Path:
    """Return a cached file or download it via :func:`download_with_resume`.

    If the file exists in *cache_dir* and was modified less than
    *max_age_hours* ago the download is skipped.

    Returns the file path (always inside *cache_dir*).
    """
    cache = Path(cache_dir)
    cache.mkdir(parents=True, exist_ok=True)
    dest = cache / filename

    if dest.exists():
        age_seconds = time.time() - dest.stat().st_mtime
        age_hours = age_seconds / 3600
        if age_hours < max_age_hours:
            print(f"Using cached file: {filename} ({age_hours:.1f} hours old)")
            return dest

    return download_with_resume(url, dest)


# ---------------------------------------------------------------------------
# estimate_csv_rows
# ---------------------------------------------------------------------------


def estimate_csv_rows(filepath: str | Path) -> int:
    """Estimate the number of rows in a CSV without loading it.

    Reads the first 1 000 lines, computes the average bytes per line,
    and divides the total file size by that average.

    Returns the estimated row count (excluding the header).
    """
    filepath = Path(filepath)
    file_size = filepath.stat().st_size

    sample_bytes = 0
    sample_lines = 0
    with open(filepath, "rb") as f:
        for line in f:
            sample_lines += 1
            sample_bytes += len(line)
            if sample_lines >= 1001:  # 1 header + 1000 data lines
                break

    if sample_lines <= 1:
        print(f"Estimated 0 rows in {filepath.name}")
        return 0

    # Subtract header line
    avg_bytes_per_line = sample_bytes / sample_lines
    estimated = int(file_size / avg_bytes_per_line) - 1  # -1 for header

    print(f"Estimated {estimated:,} rows in {filepath.name}")
    return estimated


# ---------------------------------------------------------------------------
# Memory safety guards
# ---------------------------------------------------------------------------


def check_available_memory(required_gb: float = 1.0) -> None:
    """Check available RAM and warn or raise if too low.

    - Below *required_gb*: prints a warning.
    - Below 0.5 GB: raises ``MemoryError``.
    """
    mem = psutil.virtual_memory()
    available_gb = mem.available / (1024 ** 3)

    if available_gb < 0.5:
        raise MemoryError(
            f"Only {available_gb:.2f}GB RAM available — aborting to prevent OOM."
        )
    if available_gb < required_gb:
        print(
            f"WARNING: Only {available_gb:.2f}GB RAM available "
            f"(recommended: {required_gb:.1f}GB)"
        )


def monitor_memory(func):
    """Decorator that prints memory usage before and after a function call."""

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        proc = psutil.Process()
        mem_before = proc.memory_info().rss / (1024 ** 2)
        print(f"Memory before: {mem_before:.1f}MB")
        try:
            result = func(*args, **kwargs)
            return result
        finally:
            mem_after = proc.memory_info().rss / (1024 ** 2)
            delta = mem_after - mem_before
            print(
                f"Memory before: {mem_before:.1f}MB | "
                f"After: {mem_after:.1f}MB | "
                f"Delta: {delta:+.1f}MB"
            )

    @functools.wraps(func)
    async def async_wrapper(*args, **kwargs):
        proc = psutil.Process()
        mem_before = proc.memory_info().rss / (1024 ** 2)
        print(f"Memory before: {mem_before:.1f}MB")
        try:
            result = await func(*args, **kwargs)
            return result
        finally:
            mem_after = proc.memory_info().rss / (1024 ** 2)
            delta = mem_after - mem_before
            print(
                f"Memory before: {mem_before:.1f}MB | "
                f"After: {mem_after:.1f}MB | "
                f"Delta: {delta:+.1f}MB"
            )

    import asyncio

    if asyncio.iscoroutinefunction(func):
        return async_wrapper
    return wrapper
