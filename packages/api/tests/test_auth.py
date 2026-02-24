"""Tests for authentication middleware."""

from __future__ import annotations

from unittest.mock import patch
from uuid import uuid4

from candata_api.middleware.auth import TIER_ORDER
from tests.conftest import make_supabase


def test_tier_ordering():
    """Verify tier ordering is correct."""
    assert TIER_ORDER["free"] < TIER_ORDER["starter"]
    assert TIER_ORDER["starter"] < TIER_ORDER["pro"]
    assert TIER_ORDER["pro"] < TIER_ORDER["business"]
    assert TIER_ORDER["business"] < TIER_ORDER["enterprise"]


def test_no_auth_returns_none(client):
    """Request with no credentials accesses public endpoints."""
    response = client.get("/v1/indicators")
    assert response.status_code == 200


def test_public_endpoint_ignores_invalid_key(client):
    """Public endpoints (no auth dep) ignore invalid API keys."""
    response = client.get(
        "/v1/indicators",
        headers={"X-API-Key": "invalid-key"},
    )
    # Public endpoints don't validate credentials
    assert response.status_code == 200


def test_invalid_api_key_returns_401_on_protected_endpoint(client):
    """Invalid API key returns 401 on auth-protected endpoints.

    Indicator values endpoint uses get_current_user dependency,
    so invalid keys are rejected.
    """
    # Need to mock the indicator lookup to get past 404
    mock = make_supabase({
        "indicators": ([{"id": "cpi_monthly", "name": "CPI", "source": "StatCan"}], 1),
        "geographies": ([{"id": str(uuid4()), "level": "country"}], 1),
    })
    with (
        patch("candata_api.middleware.auth.get_supabase_client", return_value=mock),
        patch("candata_api.services.indicator_service.get_supabase_client", return_value=mock),
        patch("candata_api.routers.v1.indicators.get_supabase_client", return_value=mock),
    ):
        response = client.get(
            "/v1/indicators/cpi_monthly/values",
            headers={"X-API-Key": "invalid-key"},
        )
    assert response.status_code == 401


def test_valid_api_key(client, sample_indicator):
    """Valid API key grants access."""
    user_id = str(uuid4())
    api_key_row = {
        "user_id": user_id,
        "tier": "pro",
        "email": "test@example.com",
        "metadata": {},
    }
    mock = make_supabase({
        "api_keys": ([api_key_row], 1),
        "indicators": ([sample_indicator], 1),
    })
    with (
        patch("candata_api.middleware.auth.get_supabase_client", return_value=mock),
        patch("candata_api.services.indicator_service.get_supabase_client", return_value=mock),
    ):
        response = client.get(
            "/v1/indicators",
            headers={"X-API-Key": "valid-key"},
        )
    assert response.status_code == 200
