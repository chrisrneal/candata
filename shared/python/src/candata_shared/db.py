"""
db.py — Supabase and DuckDB client singletons.

Usage:
    from candata_shared.db import get_supabase_client, get_duckdb_connection

    supabase = get_supabase_client()                    # anon key (API reads)
    supabase = get_supabase_client(service_role=True)   # service key (pipeline writes)
    duck = get_duckdb_connection()
"""

from __future__ import annotations

import threading
from typing import Optional

import duckdb
import structlog
from supabase import Client, create_client

from candata_shared.config import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Supabase — one client per role per process (thread-safe via lock)
# ---------------------------------------------------------------------------
_supabase_lock = threading.Lock()
_supabase_anon: Optional[Client] = None
_supabase_service: Optional[Client] = None


def get_supabase_client(*, service_role: bool = False) -> Client:
    """
    Return a singleton Supabase client.

    Args:
        service_role: If True, uses the service role key (full DB access).
                      If False (default), uses the anon key (RLS applies).

    Returns:
        supabase.Client instance.
    """
    global _supabase_anon, _supabase_service

    with _supabase_lock:
        if service_role:
            if _supabase_service is None:
                if not settings.supabase_service_key:
                    raise RuntimeError(
                        "SUPABASE_SERVICE_KEY is not set. "
                        "Set it in .env before using service_role=True."
                    )
                _supabase_service = create_client(
                    settings.supabase_url,
                    settings.supabase_service_key,
                )
                logger.info("supabase_client_created", role="service_role")
            return _supabase_service
        else:
            if _supabase_anon is None:
                if not settings.supabase_anon_key:
                    raise RuntimeError(
                        "SUPABASE_ANON_KEY is not set. Set it in .env."
                    )
                _supabase_anon = create_client(
                    settings.supabase_url,
                    settings.supabase_anon_key,
                )
                logger.info("supabase_client_created", role="anon")
            return _supabase_anon


def reset_supabase_clients() -> None:
    """Reset singleton clients (useful in tests)."""
    global _supabase_anon, _supabase_service
    with _supabase_lock:
        _supabase_anon = None
        _supabase_service = None


# ---------------------------------------------------------------------------
# DuckDB — single connection per process
# ---------------------------------------------------------------------------
_duckdb_lock = threading.Lock()
_duckdb_conn: Optional[duckdb.DuckDBPyConnection] = None


def get_duckdb_connection() -> duckdb.DuckDBPyConnection:
    """
    Return a singleton DuckDB connection to the staging database.

    The file path is read from settings.duckdb_path.
    Creates parent directories if they don't exist.

    Returns:
        duckdb.DuckDBPyConnection
    """
    global _duckdb_conn

    with _duckdb_lock:
        if _duckdb_conn is None:
            import os
            from pathlib import Path

            db_path = Path(settings.duckdb_path)
            db_path.parent.mkdir(parents=True, exist_ok=True)

            _duckdb_conn = duckdb.connect(str(db_path))

            # Enable HTTP filesystem for reading remote Parquet / CSV
            _duckdb_conn.execute("INSTALL httpfs; LOAD httpfs;")
            _duckdb_conn.execute("SET threads TO 4;")

            logger.info("duckdb_connected", path=str(db_path))

        return _duckdb_conn


def reset_duckdb_connection() -> None:
    """Reset the DuckDB singleton (useful in tests)."""
    global _duckdb_conn
    with _duckdb_lock:
        if _duckdb_conn is not None:
            _duckdb_conn.close()
            _duckdb_conn = None
