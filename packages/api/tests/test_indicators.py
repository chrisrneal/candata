"""Tests for indicator endpoints."""

from __future__ import annotations

from unittest.mock import patch

from tests.conftest import make_supabase


def test_list_indicators(client, sample_indicator):
    """GET /v1/indicators returns indicator list."""
    mock = make_supabase({"indicators": ([sample_indicator], 1)})
    with patch("candata_api.services.indicator_service.get_supabase_client", return_value=mock):
        response = client.get("/v1/indicators")

    assert response.status_code == 200
    body = response.json()
    assert "data" in body
    assert "meta" in body
    assert len(body["data"]) == 1


def test_get_indicator_not_found(client):
    """GET /v1/indicators/{id} returns 404 for unknown indicator."""
    response = client.get("/v1/indicators/nonexistent")
    assert response.status_code == 404


def test_health(client):
    """GET /health returns ok."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"
    assert response.json()["version"] == "0.1.0"


def test_ready(client):
    """GET /ready returns ready."""
    response = client.get("/ready")
    assert response.status_code == 200
    assert response.json()["status"] == "ready"
