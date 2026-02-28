"""
utils/checkpoint.py — Pipeline checkpoint system for crash recovery.

Persists the last-processed row for each pipeline so that interrupted
runs can resume from where they left off instead of re-processing from
the beginning.

Checkpoint file: ``candata/data/cache/checkpoints.json``
"""

from __future__ import annotations

import json
from pathlib import Path

from filelock import FileLock

import structlog

log = structlog.get_logger(__name__)

# Default checkpoint location (relative to the pipeline package root)
_CHECKPOINT_DIR = Path(__file__).resolve().parents[3] / "data" / "cache"
_CHECKPOINT_FILE = _CHECKPOINT_DIR / "checkpoints.json"
_LOCK_FILE = _CHECKPOINT_DIR / "checkpoints.json.lock"


def _ensure_dir() -> None:
    _CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)


def _read_all() -> dict[str, int]:
    if not _CHECKPOINT_FILE.exists():
        return {}
    try:
        return json.loads(_CHECKPOINT_FILE.read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _write_all(data: dict[str, int]) -> None:
    _ensure_dir()
    _CHECKPOINT_FILE.write_text(json.dumps(data, indent=2))


def save_checkpoint(pipeline_name: str, last_processed_row: int) -> None:
    """Persist the last-processed row count for *pipeline_name*.

    Uses a file lock so concurrent pipeline runs don't corrupt the JSON.
    """
    _ensure_dir()
    with FileLock(_LOCK_FILE):
        data = _read_all()
        data[pipeline_name] = last_processed_row
        _write_all(data)
    log.debug("checkpoint_saved", pipeline=pipeline_name, row=last_processed_row)


def load_checkpoint(pipeline_name: str) -> int:
    """Return the last-processed row for *pipeline_name*, or 0 if none."""
    _ensure_dir()
    with FileLock(_LOCK_FILE):
        data = _read_all()
    row = data.get(pipeline_name, 0)
    if row > 0:
        log.info("checkpoint_loaded", pipeline=pipeline_name, row=row)
    return row


def clear_checkpoint(pipeline_name: str) -> None:
    """Remove the checkpoint entry on successful pipeline completion."""
    _ensure_dir()
    with FileLock(_LOCK_FILE):
        data = _read_all()
        if pipeline_name in data:
            del data[pipeline_name]
            _write_all(data)
    log.info("checkpoint_cleared", pipeline=pipeline_name)
