"""Shared test fixtures for candata-api."""

from __future__ import annotations

from unittest.mock import MagicMock, patch
from uuid import uuid4

import pytest
from fastapi.testclient import TestClient


def make_chain(data=None, count=0):
    """Create a chainable mock that returns given data on execute()."""
    chain = MagicMock()
    chain.execute.return_value = MagicMock(data=data or [], count=count)
    for method in (
        "select", "eq", "neq", "gt", "gte", "lt", "lte",
        "ilike", "order", "limit", "range",
    ):
        getattr(chain, method).return_value = chain
    return chain


def make_supabase(table_data=None):
    """Create a mock Supabase client.

    table_data: optional dict mapping table name -> (data, count).
    All unmapped tables return empty results.
    """
    client = MagicMock()
    td = table_data or {}

    def _table(name):
        data, count = td.get(name, ([], 0))
        chain = make_chain(data, count)
        table = MagicMock()
        for method in (
            "select", "eq", "neq", "gt", "gte", "lt", "lte",
            "ilike", "order", "limit", "range",
        ):
            getattr(table, method).return_value = chain
        return table

    client.table.side_effect = _table
    return client


@pytest.fixture(autouse=True)
def _clear_caches():
    """Clear all in-memory caches between tests."""
    from candata_api.utils.cache import (
        geography_cache, housing_cache, indicator_cache,
        procurement_cache, search_cache, trade_cache,
    )
    yield
    for cache in (indicator_cache, geography_cache, procurement_cache,
                  search_cache, housing_cache, trade_cache):
        cache.clear()


@pytest.fixture()
def _supabase_patch():
    """Patch get_supabase_client everywhere it's imported."""
    mock = make_supabase()
    patches = [
        patch("candata_shared.db.get_supabase_client", return_value=mock),
        patch("candata_api.middleware.auth.get_supabase_client", return_value=mock),
    ]
    for p in patches:
        p.start()
    yield mock
    for p in patches:
        p.stop()


@pytest.fixture()
def app(_supabase_patch):
    """Create test FastAPI app with mocked Supabase."""
    from candata_api.app import create_app
    return create_app()


@pytest.fixture()
def client(app):
    """HTTP test client."""
    return TestClient(app)


@pytest.fixture()
def sample_indicator():
    return {
        "id": "cpi_monthly",
        "name": "Consumer Price Index",
        "source": "StatCan",
        "frequency": "monthly",
        "unit": "index",
        "description": "CPI all-items",
    }


@pytest.fixture()
def sample_geography():
    return {
        "id": str(uuid4()),
        "level": "pr",
        "sgc_code": "35",
        "name": "Ontario",
    }


@pytest.fixture()
def sample_contract():
    return {
        "id": str(uuid4()),
        "contract_number": "C-2024-001",
        "vendor_name": "Test Corp",
        "department": "Public Works",
        "category": "Construction",
        "contract_value": "1500000.00",
        "award_date": "2024-06-15",
    }
