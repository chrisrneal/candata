"""Tests for procurement endpoints."""

from __future__ import annotations

from unittest.mock import patch

from tests.conftest import make_supabase


def test_list_contracts(client, sample_contract):
    """GET /v1/procurement/contracts returns data."""
    mock = make_supabase({"contracts": ([sample_contract], 1)})
    with patch("candata_api.services.procurement_service.get_supabase_client", return_value=mock):
        response = client.get("/v1/procurement/contracts")

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert body["meta"]["total_count"] == 1


def test_get_contract_not_found(client):
    """GET /v1/procurement/contracts/{id} returns 404."""
    response = client.get("/v1/procurement/contracts/nonexistent")
    assert response.status_code == 404


def test_procurement_stats(client):
    """GET /v1/procurement/stats returns aggregated data."""
    rows = [
        {"department": "DND", "category": "Defence", "contract_value": "1000000", "award_date": "2024-01-01"},
        {"department": "PSPC", "category": "IT", "contract_value": "500000", "award_date": "2024-06-01"},
    ]
    mock = make_supabase({"contracts": (rows, 2)})
    with patch("candata_api.services.procurement_service.get_supabase_client", return_value=mock):
        response = client.get("/v1/procurement/stats")

    assert response.status_code == 200
    body = response.json()
    assert body["data"]["contract_count"] == 2
