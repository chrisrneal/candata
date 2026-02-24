"""Tests for housing endpoints."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from tests.conftest import make_supabase


def test_vacancy_rates(client):
    """GET /v1/housing/vacancy-rates returns data."""
    row = {
        "id": str(uuid4()),
        "geography_id": str(uuid4()),
        "ref_date": "2024-10-01",
        "bedroom_type": "total",
        "vacancy_rate": "2.5",
    }
    mock = make_supabase({"vacancy_rates": ([row], 1)})
    with patch("candata_api.services.housing_service.get_supabase_client", return_value=mock):
        response = client.get("/v1/housing/vacancy-rates")

    assert response.status_code == 200
    body = response.json()
    assert "data" in body


def test_market_summary_not_found(client):
    """GET /v1/housing/market-summary/{geo} returns 404 for unknown geo."""
    response = client.get("/v1/housing/market-summary/XXXX")
    assert response.status_code == 404
