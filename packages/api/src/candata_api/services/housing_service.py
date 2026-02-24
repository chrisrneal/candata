"""Housing data service."""

from __future__ import annotations

from datetime import date
from typing import Any
from uuid import UUID

from candata_shared.db import get_supabase_client

from candata_api.utils.cache import housing_cache
from candata_api.utils.filtering import apply_cursor_filter, apply_date_filters


def get_vacancy_rates(
    *,
    geography_id: UUID | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    bedroom_type: str | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = (
        supabase.table("vacancy_rates")
        .select("*", count="exact")
        .order("ref_date", desc=True)
    )
    if geography_id:
        query = query.eq("geography_id", str(geography_id))
    if bedroom_type:
        query = query.eq("bedroom_type", bedroom_type)
    query = apply_date_filters(query, "ref_date", start_date, end_date)
    query = apply_cursor_filter(query, last_id)
    result = query.limit(page_size).execute()
    return result.data, result.count


def get_average_rents(
    *,
    geography_id: UUID | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    bedroom_type: str | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = (
        supabase.table("average_rents")
        .select("*", count="exact")
        .order("ref_date", desc=True)
    )
    if geography_id:
        query = query.eq("geography_id", str(geography_id))
    if bedroom_type:
        query = query.eq("bedroom_type", bedroom_type)
    query = apply_date_filters(query, "ref_date", start_date, end_date)
    query = apply_cursor_filter(query, last_id)
    result = query.limit(page_size).execute()
    return result.data, result.count


def get_housing_starts(
    *,
    geography_id: UUID | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    dwelling_type: str | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = (
        supabase.table("housing_starts")
        .select("*", count="exact")
        .order("ref_date", desc=True)
    )
    if geography_id:
        query = query.eq("geography_id", str(geography_id))
    if dwelling_type:
        query = query.eq("dwelling_type", dwelling_type)
    query = apply_date_filters(query, "ref_date", start_date, end_date)
    query = apply_cursor_filter(query, last_id)
    result = query.limit(page_size).execute()
    return result.data, result.count


def get_market_summary(geography_id: UUID) -> dict[str, Any]:
    """Combined housing market view for a geography."""
    cache_key = f"market_summary:{geography_id}"
    cached = housing_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    geo_id = str(geography_id)

    # Latest vacancy rate
    vacancy = (
        supabase.table("vacancy_rates")
        .select("*")
        .eq("geography_id", geo_id)
        .eq("bedroom_type", "total")
        .order("ref_date", desc=True)
        .limit(1)
        .execute()
    )

    # Latest average rent
    rent = (
        supabase.table("average_rents")
        .select("*")
        .eq("geography_id", geo_id)
        .eq("bedroom_type", "total")
        .order("ref_date", desc=True)
        .limit(1)
        .execute()
    )

    # Latest housing starts
    starts = (
        supabase.table("housing_starts")
        .select("*")
        .eq("geography_id", geo_id)
        .eq("dwelling_type", "total")
        .order("ref_date", desc=True)
        .limit(1)
        .execute()
    )

    summary = {
        "geography_id": geo_id,
        "vacancy_rate": vacancy.data[0] if vacancy.data else None,
        "average_rent": rent.data[0] if rent.data else None,
        "housing_starts": starts.data[0] if starts.data else None,
    }
    housing_cache.set(cache_key, summary)
    return summary
