"""Indicator endpoints."""

from __future__ import annotations

import io
from datetime import date

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from candata_api.dependencies import (
    AuthUser,
    PaginationParams,
    get_current_user,
    get_supabase_client,
    require_auth,
)
from candata_api.middleware.auth import TIER_ORDER
from candata_api.responses import error_response, wrap_response
from candata_api.services import indicator_service
from candata_api.utils.pagination import build_links, encode_cursor

router = APIRouter(prefix="/indicators", tags=["indicators"])


def _resolve_geography(geo: str) -> str | None:
    """Resolve a geo query param to a geography UUID."""
    supabase = get_supabase_client()
    if geo.lower() == "canada":
        result = (
            supabase.table("geographies")
            .select("id")
            .eq("level", "country")
            .limit(1)
            .execute()
        )
    else:
        result = (
            supabase.table("geographies")
            .select("id")
            .eq("sgc_code", geo)
            .limit(1)
            .execute()
        )
    if result.data:
        return result.data[0]["id"]
    return None


@router.get("")
async def list_indicators():
    """List all indicators with metadata. No auth required."""
    data = indicator_service.list_indicators()
    return wrap_response(data, total_count=len(data), source="candata")


@router.get("/{indicator_id}")
async def get_indicator(indicator_id: str):
    """Get a single indicator's metadata."""
    data = indicator_service.get_indicator(indicator_id)
    if data is None:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_id}' not found")
    return wrap_response(data, source=data.get("source"))


@router.get("/{indicator_id}/values")
async def get_indicator_values(
    indicator_id: str,
    pagination: PaginationParams = Depends(),
    user: AuthUser | None = Depends(get_current_user),
    geo: str = Query("canada", description="SGC code or 'canada'"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    frequency: str | None = Query(None),
    format: str = Query("json", description="Response format: json or csv"),
):
    """Get time-series values for an indicator."""
    # Verify indicator exists
    indicator = indicator_service.get_indicator(indicator_id)
    if indicator is None:
        raise HTTPException(status_code=404, detail=f"Indicator '{indicator_id}' not found")

    # Resolve geography
    geography_id = _resolve_geography(geo)
    if geography_id is None:
        raise HTTPException(
            status_code=400,
            detail=error_response(
                "INVALID_GEOGRAPHY",
                f"Geography code '{geo}' is not valid.",
                details={"valid_formats": ["SGC code", "'canada'"]},
            ),
        )

    # Tier-based geo access
    if geo.lower() != "canada" and user is not None:
        # Check if the geo is provincial, CMA, etc.
        supabase = get_supabase_client()
        geo_row = (
            supabase.table("geographies")
            .select("level")
            .eq("id", geography_id)
            .limit(1)
            .execute()
        )
        if geo_row.data:
            level = geo_row.data[0]["level"]
            user_level = TIER_ORDER.get(user.tier, 0)
            if level == "pr" and user_level < TIER_ORDER["starter"]:
                raise HTTPException(
                    status_code=403,
                    detail="Provincial data requires 'starter' tier or above.",
                )
            if level in ("cma", "cd", "csd") and user_level < TIER_ORDER["pro"]:
                raise HTTPException(
                    status_code=403,
                    detail="CMA/sub-provincial data requires 'pro' tier or above.",
                )

    data, total = indicator_service.get_indicator_values(
        indicator_id,
        geography_id=geography_id,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )

    if format == "csv":
        return _csv_response(data, f"{indicator_id}_values.csv")

    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        f"/v1/indicators/{indicator_id}/values",
        {"geo": geo, "start_date": start_date, "end_date": end_date},
        data,
        pagination.page_size,
    )

    return wrap_response(
        data,
        total_count=total,
        page_size=pagination.page_size,
        cursor=cursor,
        source=indicator.get("source"),
        links=links,
    )


def _csv_response(data: list[dict], filename: str) -> StreamingResponse:
    """Convert list of dicts to CSV streaming response."""
    import polars as pl

    if not data:
        content = ""
    else:
        df = pl.DataFrame(data)
        buf = io.BytesIO()
        df.write_csv(buf)
        content = buf.getvalue().decode()

    return StreamingResponse(
        iter([content]),
        media_type="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
