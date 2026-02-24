"""Procurement endpoints."""

from __future__ import annotations

from datetime import date
from decimal import Decimal

from fastapi import APIRouter, Depends, HTTPException, Query

from candata_api.dependencies import PaginationParams
from candata_api.responses import wrap_response
from candata_api.services import procurement_service
from candata_api.utils.pagination import build_links, encode_cursor

router = APIRouter(prefix="/procurement", tags=["procurement"])


@router.get("/contracts")
async def list_contracts(
    pagination: PaginationParams = Depends(),
    department: str | None = Query(None),
    vendor: str | None = Query(None),
    min_value: Decimal | None = Query(None),
    max_value: Decimal | None = Query(None),
    category: str | None = Query(None),
    start_date: date | None = Query(None),
    end_date: date | None = Query(None),
    q: str | None = Query(None, description="Full-text search"),
    sort_by: str = Query("date", description="Sort by: value or date"),
):
    data, total = procurement_service.search_contracts(
        department=department,
        vendor=vendor,
        min_value=min_value,
        max_value=max_value,
        category=category,
        start_date=start_date,
        end_date=end_date,
        q=q,
        sort_by=sort_by,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
        last_sort_value=pagination.last_sort_value,
    )
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/procurement/contracts",
        {"department": department, "vendor": vendor, "q": q, "sort_by": sort_by},
        data,
        pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        source="CanadaBuys", links=links,
    )


@router.get("/contracts/{contract_id}")
async def get_contract(contract_id: str):
    data = procurement_service.get_contract(contract_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Contract not found")
    return wrap_response(data, source="CanadaBuys")


@router.get("/vendors/{vendor_name}")
async def get_vendor(
    vendor_name: str,
    pagination: PaginationParams = Depends(),
):
    data, total = procurement_service.get_vendor_contracts(
        vendor_name, page_size=pagination.page_size, last_id=pagination.last_id,
    )
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        f"/v1/procurement/vendors/{vendor_name}", {}, data, pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        source="CanadaBuys", links=links,
    )


@router.get("/stats")
async def procurement_stats(year: int | None = Query(None)):
    data = procurement_service.get_procurement_stats(year=year)
    return wrap_response(data, source="CanadaBuys")


@router.get("/tenders")
async def list_tenders(
    pagination: PaginationParams = Depends(),
    category: str | None = Query(None),
    region: str | None = Query(None),
    closing_after: date | None = Query(None),
):
    data, total = procurement_service.list_tenders(
        category=category,
        region=region,
        closing_after=closing_after,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/procurement/tenders",
        {"category": category, "region": region},
        data,
        pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        source="CanadaBuys", links=links,
    )
