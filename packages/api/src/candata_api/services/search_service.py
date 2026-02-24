"""Cross-product search service."""

from __future__ import annotations

from typing import Any

from candata_shared.db import get_supabase_client

from candata_api.utils.cache import search_cache


def cross_search(q: str, *, limit: int = 20) -> list[dict[str, Any]]:
    """Search across multiple tables and return mixed results."""
    cache_key = f"search:{q}:{limit}"
    cached = search_cache.get(cache_key)
    if cached is not None:
        return cached

    supabase = get_supabase_client()
    results: list[dict[str, Any]] = []
    per_type = max(3, limit // 5)

    # Search indicators
    ind = (
        supabase.table("indicators")
        .select("id, name, source, description")
        .ilike("name", f"%{q}%")
        .limit(per_type)
        .execute()
    )
    for row in ind.data:
        results.append({"type": "indicator", "id": row["id"], "name": row["name"], **row})

    # Search geographies
    geo = (
        supabase.table("geographies")
        .select("id, name, level, sgc_code")
        .ilike("name", f"%{q}%")
        .limit(per_type)
        .execute()
    )
    for row in geo.data:
        results.append({"type": "geography", "id": str(row["id"]), "name": row["name"], **row})

    # Search contracts
    con = (
        supabase.table("contracts")
        .select("id, vendor_name, department, contract_value")
        .ilike("description", f"%{q}%")
        .limit(per_type)
        .execute()
    )
    for row in con.data:
        results.append({
            "type": "contract",
            "id": str(row["id"]),
            "name": row["vendor_name"],
            **row,
        })

    # Search entities
    ent = (
        supabase.table("entities")
        .select("id, name, entity_type")
        .ilike("name", f"%{q}%")
        .limit(per_type)
        .execute()
    )
    for row in ent.data:
        results.append({"type": "entity", "id": str(row["id"]), "name": row["name"], **row})

    results = results[:limit]
    search_cache.set(cache_key, results)
    return results
