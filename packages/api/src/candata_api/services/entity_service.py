"""Entity data service."""

from __future__ import annotations

from typing import Any

from candata_shared.db import get_supabase_client

from candata_api.utils.filtering import apply_cursor_filter, apply_text_search


def search_entities(
    *,
    entity_type: str | None = None,
    q: str | None = None,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()
    query = supabase.table("entities").select("*", count="exact").order("name")

    if entity_type:
        query = query.eq("entity_type", entity_type)
    query = apply_text_search(query, "name", q)
    query = apply_cursor_filter(query, last_id)

    result = query.limit(page_size).execute()
    return result.data, result.count


def get_entity(entity_id: str) -> dict[str, Any] | None:
    supabase = get_supabase_client()
    result = (
        supabase.table("entities")
        .select("*")
        .eq("id", entity_id)
        .limit(1)
        .execute()
    )
    return result.data[0] if result.data else None


def get_entity_relationships(
    entity_id: str,
    *,
    page_size: int = 50,
    last_id: str | None = None,
) -> tuple[list[dict[str, Any]], int | None]:
    supabase = get_supabase_client()

    # Get relationships where entity is source or target
    source_q = (
        supabase.table("entity_relationships")
        .select("*", count="exact")
        .eq("source_entity_id", entity_id)
    )
    source_q = apply_cursor_filter(source_q, last_id)
    source_result = source_q.limit(page_size).execute()

    target_q = (
        supabase.table("entity_relationships")
        .select("*")
        .eq("target_entity_id", entity_id)
    )
    target_result = target_q.limit(page_size).execute()

    combined = (source_result.data or []) + (target_result.data or [])
    combined.sort(key=lambda r: r.get("created_at", ""))

    return combined[:page_size], source_result.count
