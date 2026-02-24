"""Trade endpoints."""

from __future__ import annotations

import io
from datetime import date

from fastapi import APIRouter, Depends, Query
from fastapi.responses import StreamingResponse

from candata_api.dependencies import PaginationParams
from candata_api.responses import wrap_response
from candata_api.services import trade_service
from candata_api.utils.pagination import build_links, encode_cursor

router = APIRouter(prefix="/trade", tags=["trade"])


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


@router.get("/exports")
async def exports(
    pagination: PaginationParams = Depends(),
    hs_code: str | None = Query(None),
    partner: str | None = Query(None, description="ISO 3166-1 alpha-3 country code"),
    province: str | None = Query(None, description="2-digit SGC province code"),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    format: str = Query("json"),
):
    data, total = trade_service.get_exports(
        hs_code=hs_code, partner=partner, province=province,
        start_date=start_date, end_date=end_date,
        page_size=pagination.page_size, last_id=pagination.last_id,
    )
    if format == "csv":
        return _csv_response(data, "exports.csv")
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/trade/exports",
        {"hs_code": hs_code, "partner": partner, "province": province},
        data, pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        source="StatCan", links=links,
    )


@router.get("/imports")
async def imports(
    pagination: PaginationParams = Depends(),
    hs_code: str | None = Query(None),
    partner: str | None = Query(None),
    province: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    format: str = Query("json"),
):
    data, total = trade_service.get_imports(
        hs_code=hs_code, partner=partner, province=province,
        start_date=start_date, end_date=end_date,
        page_size=pagination.page_size, last_id=pagination.last_id,
    )
    if format == "csv":
        return _csv_response(data, "imports.csv")
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/trade/imports",
        {"hs_code": hs_code, "partner": partner, "province": province},
        data, pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        source="StatCan", links=links,
    )


@router.get("/balance")
async def trade_balance(
    partner: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
):
    data = trade_service.get_trade_balance(
        partner=partner, start_date=start_date, end_date=end_date,
    )
    return wrap_response(data, source="StatCan")


@router.get("/top-commodities")
async def top_commodities(
    direction: str = Query("export"),
    partner: str | None = Query(None),
    year: int | None = Query(None),
    limit: int = Query(20, ge=1, le=100),
):
    data = trade_service.get_top_commodities(
        direction=direction, partner=partner, year=year, limit=limit,
    )
    return wrap_response(data, total_count=len(data), source="StatCan")
