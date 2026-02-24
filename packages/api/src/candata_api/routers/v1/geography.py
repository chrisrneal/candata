"""Geography endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

from candata_shared.db import get_supabase_client

from candata_api.responses import wrap_response
from candata_api.utils.cache import geography_cache

router = APIRouter(prefix="/geo", tags=["geography"])


@router.get("/provinces")
async def list_provinces():
    """List all provinces and territories."""
    cached = geography_cache.get("provinces")
    if cached is not None:
        return wrap_response(cached, total_count=len(cached), source="candata")

    supabase = get_supabase_client()
    result = (
        supabase.table("geographies")
        .select("*")
        .eq("level", "pr")
        .order("sgc_code")
        .execute()
    )
    geography_cache.set("provinces", result.data)
    return wrap_response(result.data, total_count=len(result.data), source="candata")


@router.get("/cmas")
async def list_cmas():
    """List all Census Metropolitan Areas."""
    cached = geography_cache.get("cmas")
    if cached is not None:
        return wrap_response(cached, total_count=len(cached), source="candata")

    supabase = get_supabase_client()
    result = (
        supabase.table("geographies")
        .select("*")
        .eq("level", "cma")
        .order("name")
        .execute()
    )
    geography_cache.set("cmas", result.data)
    return wrap_response(result.data, total_count=len(result.data), source="candata")


@router.get("/{sgc_code}")
async def get_geography(sgc_code: str):
    """Get geography detail by SGC code."""
    cache_key = f"geo:{sgc_code}"
    cached = geography_cache.get(cache_key)
    if cached is not None:
        return wrap_response(cached, source="candata")

    supabase = get_supabase_client()
    result = (
        supabase.table("geographies")
        .select("*")
        .eq("sgc_code", sgc_code)
        .limit(1)
        .execute()
    )
    if not result.data:
        raise HTTPException(status_code=404, detail=f"Geography '{sgc_code}' not found")
    geography_cache.set(cache_key, result.data[0])
    return wrap_response(result.data[0], source="candata")
