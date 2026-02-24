"""
tests/conftest.py — Shared pytest fixtures for the pipeline test suite.

Provides:
  fixture_path()   — resolves paths to tests/fixtures/
  mock_supabase()  — MagicMock of the Supabase client (prevents real DB calls)
  sample_*_df      — pre-loaded polars DataFrames from fixture files
  httpx_mock       — configured respx router for faking HTTP responses
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import polars as pl
import pytest
import respx

FIXTURES_DIR = Path(__file__).parent / "fixtures"


# ---------------------------------------------------------------------------
# Path helpers
# ---------------------------------------------------------------------------

@pytest.fixture
def fixture_path() -> Path:
    return FIXTURES_DIR


# ---------------------------------------------------------------------------
# Supabase client mock
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_supabase_client() -> MagicMock:
    """
    A MagicMock that simulates the supabase.Client interface.

    The .table().select().execute() chain returns empty data by default.
    Override in individual tests: mock_supabase_client.table.return_value...
    """
    client = MagicMock()

    # Default: .table().select().execute() → {"data": [], "count": 0}
    default_result = MagicMock()
    default_result.data = []
    default_result.count = 0

    (
        client.table.return_value
        .select.return_value
        .execute.return_value
    ) = default_result

    (
        client.table.return_value
        .upsert.return_value
        .execute.return_value
    ) = default_result

    (
        client.table.return_value
        .insert.return_value
        .execute.return_value
    ) = default_result

    (
        client.table.return_value
        .update.return_value
        .eq.return_value
        .execute.return_value
    ) = default_result

    return client


@pytest.fixture
def mock_supabase(mock_supabase_client: MagicMock):
    """
    Patch get_supabase_client() to return the mock client.
    Yields the mock so tests can inspect calls.
    """
    with patch(
        "candata_shared.db.get_supabase_client",
        return_value=mock_supabase_client,
    ) as patched:
        yield patched


# ---------------------------------------------------------------------------
# Sample DataFrames
# ---------------------------------------------------------------------------

@pytest.fixture
def statcan_gdp_df() -> pl.DataFrame:
    """Raw StatCan GDP CSV loaded as polars DataFrame."""
    return pl.read_csv(
        FIXTURES_DIR / "statcan_gdp_sample.csv",
        infer_schema_length=0,
        null_values=["", "..", "x", "F"],
    )


@pytest.fixture
def statcan_cpi_df() -> pl.DataFrame:
    """Raw StatCan CPI CSV loaded as polars DataFrame."""
    return pl.read_csv(
        FIXTURES_DIR / "statcan_cpi_sample.csv",
        infer_schema_length=0,
        null_values=["", "..", "x", "F"],
    )


@pytest.fixture
def boc_valet_payload() -> dict:
    """Parsed BoC Valet JSON payload."""
    return json.loads((FIXTURES_DIR / "boc_valet_sample.json").read_text())


@pytest.fixture
def cmhc_vacancy_df() -> pl.DataFrame:
    """CMHC vacancy rate CSV loaded as polars DataFrame."""
    return pl.read_csv(
        FIXTURES_DIR / "cmhc_vacancy_sample.csv",
        infer_schema_length=0,
    )


# ---------------------------------------------------------------------------
# respx HTTP mock router
# ---------------------------------------------------------------------------

@pytest.fixture
def mock_http():
    """
    Activate the respx mock router for all httpx requests.

    Usage in tests:
        def test_something(mock_http):
            mock_http.get("https://...").mock(return_value=httpx.Response(200, json={...}))
    """
    with respx.mock(assert_all_called=False) as router:
        yield router
