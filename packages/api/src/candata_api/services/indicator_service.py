"""Indicator data service."""

from __future__ import annotations

from datetime import date, datetime
from typing import Any
from uuid import UUID

from candata_shared.db import get_supabase_client

from candata_api.utils.cache import indicator_cache
from candata_api.utils.filtering import apply_cursor_filter, apply_date_filters


def list_indicators() -> list[dict[str, Any]]:
    """List all indicators with metadata."""
    cached = indicator_cache.get("indicators:all")
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    result = supabase.table("indicators").select("*").order("id").execute()
    indicator_cache.set("indicators:all", result.data)
    return result.data


def get_indicator(indicator_id: str) -> dict[str, Any] | None:
    """Get a single indicator by ID."""
    cache_key = f"indicator:{indicator_id}"
    cached = indicator_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    result = (
        supabase.table("indicators")
        .select("*")
        .eq("id", indicator_id)
        .limit(1)
        .execute()
    )
    if not result.data:
        return None
    indicator_cache.set(cache_key, result.data[0])
    return result.data[0]


def get_indicator_values(
    indicator_id: str,
    *,
    geography_id: UUID | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    frequency: str | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    """Get time-series values for an indicator.

    Returns (rows, total_count).
    """
    supabase = get_supabase_client()

    # Build query
    query = (
        supabase.table("indicator_values")
        .select("*", count="exact")
        .eq("indicator_id", indicator_id)
        .order("ref_date", desc=True)
    )

    if geography_id is not None:
        query = query.eq("geography_id", str(geography_id))

    query = apply_date_filters(query, "ref_date", start_date, end_date)
    query = apply_cursor_filter(query, last_id)
    query = query.limit(page_size)

    result = query.execute()
    return result.data, result.count
