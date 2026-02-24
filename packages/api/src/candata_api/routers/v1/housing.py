"""Housing endpoints."""

from __future__ import annotations

import io
from datetime import date
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from fastapi.responses import StreamingResponse

from candata_api.dependencies import PaginationParams, get_current_user, get_supabase_client
from candata_api.responses import wrap_response
from candata_api.services import housing_service
from candata_api.utils.pagination import build_links, encode_cursor

router = APIRouter(prefix="/housing", tags=["housing"])


def _resolve_geo(geo: str | None) -> UUID | None:
    if geo is None:
        return None
    supabase = get_supabase_client()
    if geo.lower() == "canada":
        result = (
            supabase.table("geographies").select("id").eq("level", "country").limit(1).execute()
        )
    else:
        result = (
            supabase.table("geographies").select("id").eq("sgc_code", geo).limit(1).execute()
        )
    if result.data:
        return UUID(result.data[0]["id"])
    return None


def _csv_response(data: list[dict], filename: str) -> StreamingResponse:
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


@router.get("/vacancy-rates")
async def vacancy_rates(
    pagination: PaginationParams = Depends(),
    geo: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    bedroom_type: str | None = Query(None),
    format: str = Query("json"),
):
    geography_id = _resolve_geo(geo)
    data, total = housing_service.get_vacancy_rates(
        geography_id=geography_id,
        start_date=start_date,
        end_date=end_date,
        bedroom_type=bedroom_type,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    if format == "csv":
        return _csv_response(data, "vacancy_rates.csv")
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/housing/vacancy-rates",
        {"geo": geo, "start_date": start_date, "end_date": end_date, "bedroom_type": bedroom_type},
        data,
        pagination.page_size,
    )
    return wrap_response(data, total_count=total, page_size=pagination.page_size, cursor=cursor, source="CMHC", links=links)


@router.get("/rents")
async def rents(
    pagination: PaginationParams = Depends(),
    geo: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    bedroom_type: str | None = Query(None),
    format: str = Query("json"),
):
    geography_id = _resolve_geo(geo)
    data, total = housing_service.get_average_rents(
        geography_id=geography_id,
        start_date=start_date,
        end_date=end_date,
        bedroom_type=bedroom_type,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    if format == "csv":
        return _csv_response(data, "average_rents.csv")
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/housing/rents",
        {"geo": geo, "start_date": start_date, "end_date": end_date, "bedroom_type": bedroom_type},
        data,
        pagination.page_size,
    )
    return wrap_response(data, total_count=total, page_size=pagination.page_size, cursor=cursor, source="CMHC", links=links)


@router.get("/starts")
async def starts(
    pagination: PaginationParams = Depends(),
    geo: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    dwelling_type: str | None = Query(None),
    format: str = Query("json"),
):
    geography_id = _resolve_geo(geo)
    data, total = housing_service.get_housing_starts(
        geography_id=geography_id,
        start_date=start_date,
        end_date=end_date,
        dwelling_type=dwelling_type,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    if format == "csv":
        return _csv_response(data, "housing_starts.csv")
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/housing/starts",
        {"geo": geo, "start_date": start_date, "end_date": end_date, "dwelling_type": dwelling_type},
        data,
        pagination.page_size,
    )
    return wrap_response(data, total_count=total, page_size=pagination.page_size, cursor=cursor, source="CMHC", links=links)


@router.get("/market-summary/{geo}")
async def market_summary(geo: str):
    """Combined housing market view for a geography."""
    geography_id = _resolve_geo(geo)
    if geography_id is None:
        raise HTTPException(status_code=404, detail=f"Geography '{geo}' not found")
    data = housing_service.get_market_summary(geography_id)
    return wrap_response(data, source="CMHC")
