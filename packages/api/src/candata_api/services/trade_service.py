"""Trade data service."""

from __future__ import annotations

from datetime import date
from typing import Any

from candata_shared.db import get_supabase_client

from candata_api.utils.cache import trade_cache
from candata_api.utils.filtering import apply_cursor_filter, apply_date_filters


def _query_trade_flows(
    direction: str,
    *,
    hs_code: str | None = None,
    partner: str | None = None,
    province: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = (
        supabase.table("trade_flows")
        .select("*", count="exact")
        .eq("direction", direction)
        .order("ref_date", desc=True)
    )

    if hs_code:
        query = query.eq("hs_code", hs_code)
    if partner:
        query = query.eq("partner_country", partner)
    if province:
        query = query.eq("province", province)
    query = apply_date_filters(query, "ref_date", start_date, end_date)
    query = apply_cursor_filter(query, last_id)

    result = query.limit(page_size).execute()
    return result.data, result.count


def get_exports(**kwargs) -> tuple[list[dict[str, Any]], int | None]:
    return _query_trade_flows("export", **kwargs)


def get_imports(**kwargs) -> tuple[list[dict[str, Any]], int | None]:
    return _query_trade_flows("import", **kwargs)


def get_trade_balance(
    *,
    partner: str | None = None,
    start_date: date | None = None,
    end_date: date | None = None,
) -> dict[str, Any]:
    cache_key = f"balance:{partner or 'all'}:{start_date}:{end_date}"
    cached = trade_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()

    def _sum(direction: str) -> float:
        q = supabase.table("trade_flows").select("value_cad").eq("direction", direction)
        if partner:
            q = q.eq("partner_country", partner)
        q = apply_date_filters(q, "ref_date", start_date, end_date)
        result = q.execute()
        return sum(float(r.get("value_cad") or 0) for r in result.data)

    exports_total = _sum("export")
    imports_total = _sum("import")

    balance = {
        "exports": exports_total,
        "imports": imports_total,
        "balance": exports_total - imports_total,
        "partner": partner,
    }
    trade_cache.set(cache_key, balance)
    return balance


def get_top_commodities(
    *,
    direction: str = "export",
    partner: str | None = None,
    year: int | None = None,
    limit: int = 20,
) -> list[dict[str, Any]]:
    cache_key = f"top_commodities:{direction}:{partner}:{year}:{limit}"
    cached = trade_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    query = (
        supabase.table("trade_flows")
        .select("hs_code, hs_description, value_cad")
        .eq("direction", direction)
    )
    if partner:
        query = query.eq("partner_country", partner)
    if year:
        query = query.gte("ref_date", f"{year}-01-01").lte("ref_date", f"{year}-12-31")

    result = query.execute()

    # Aggregate by hs_code
    by_code: dict[str, dict[str, Any]] = {}
    for row in result.data:
        code = row.get("hs_code", "")
        if code not in by_code:
            by_code[code] = {
                "hs_code": code,
                "hs_description": row.get("hs_description"),
                "total_value": 0.0,
            }
        by_code[code]["total_value"] += float(row.get("value_cad") or 0)

    ranked = sorted(by_code.values(), key=lambda x: -x["total_value"])[:limit]
    trade_cache.set(cache_key, ranked)
    return ranked
