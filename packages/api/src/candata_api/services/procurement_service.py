"""Procurement data service."""

from __future__ import annotations

from datetime import date
from decimal import Decimal
from typing import Any

from candata_shared.db import get_supabase_client

from candata_api.utils.cache import procurement_cache
from candata_api.utils.filtering import (
    apply_cursor_filter,
    apply_date_filters,
    apply_text_search,
)


def search_contracts(
    *,
    department: str | None = None,
    vendor: str | None = None,
    min_value: Decimal | None = None,
    max_value: Decimal | None = None,
    category: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    q: str | None = None,
    sort_by: str = "award_date",
    page_size: int = 50,
    last_id: str | None = None,
    last_sort_value: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = supabase.table("contracts").select("*", count="exact")

    if department:
        query = query.ilike("department", f"%{department}%")
    if vendor:
        query = query.ilike("vendor_name", f"%{vendor}%")
    if category:
        query = query.eq("category", category)
    if min_value is not None:
        query = query.gte("contract_value", str(min_value))
    if max_value is not None:
        query = query.lte("contract_value", str(max_value))
    query = apply_date_filters(query, "award_date", start_date, end_date)
    query = apply_text_search(query, "description", q)
    query = apply_cursor_filter(query, last_id)

    desc = sort_by == "value"
    order_col = "contract_value" if sort_by == "value" else "award_date"
    query = query.order(order_col, desc=desc).limit(page_size)

    result = query.execute()
    return result.data, result.count


def get_contract(contract_id: str) -> dict[str, Any] | None:
    supabase = get_supabase_client()
    result = (
        supabase.table("contracts")
        .select("*")
        .eq("id", contract_id)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


def get_vendor_contracts(
    vendor_name: str,
    *,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = (
        supabase.table("contracts")
        .select("*", count="exact")
        .ilike("vendor_name", f"%{vendor_name}%")
        .order("award_date", desc=True)
    )
    query = apply_cursor_filter(query, last_id)
    result = query.limit(page_size).execute()
    return result.data, result.count


def get_procurement_stats(
    *,
    year: int | None = None,
) -> dict[str, Any]:
    cache_key = f"procurement_stats:{year or 'all'}"
    cached = procurement_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    query = supabase.table("contracts").select("department, category, contract_value, award_date")
    if year:
        query = query.gte("award_date", f"{year}-01-01").lte("award_date", f"{year}-12-31")

    result = query.execute()
    rows = result.data

    # Aggregate in Python
    by_dept: dict[str, float] = {}
    by_cat: dict[str, float] = {}
    total = 0.0
    for row in rows:
        val = float(row.get("contract_value") or 0)
        total += val
        dept = row.get("department", "Unknown")
        cat = row.get("category", "Unknown")
        by_dept[dept] = by_dept.get(dept, 0) + val
        by_cat[cat] = by_cat.get(cat, 0) + val

    stats = {
        "total_value": total,
        "contract_count": len(rows),
        "by_department": dict(sorted(by_dept.items(), key=lambda x: -x[1])[:20]),
        "by_category": dict(sorted(by_cat.items(), key=lambda x: -x[1])[:20]),
        "year": year,
    }
    procurement_cache.set(cache_key, stats)
    return stats


def list_tenders(
    *,
    category: str | None = None,
    region: str | None = None,
    closing_after: date | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = supabase.table("tenders").select("*", count="exact")

    if category:
        query = query.eq("category", category)
    if region:
        query = query.ilike("region", f"%{region}%")
    if closing_after:
        query = query.gte("closing_date", closing_after.isoformat())

    query = apply_cursor_filter(query, last_id)
    query = query.order("closing_date").limit(page_size)

    result = query.execute()
    return result.data, result.count
