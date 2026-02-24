"""Query parameter parsing and Supabase filter builders."""

from __future__ import annotations

from datetime import date
from typing import Any


def apply_date_filters(
    query: Any,
    column: str,
    start_date: date | None,
    end_date: date | None,
) -> Any:
    """Apply date range filters to a Supabase query builder."""
    if start_date is not None:
        query = query.gte(column, start_date.isoformat())
    if end_date is not None:
        query = query.lte(column, end_date.isoformat())
    return query


def apply_cursor_filter(
    query: Any,
    last_id: str | None,
    id_column: str = "id",
) -> Any:
    """Apply cursor-based pagination filter."""
    if last_id is not None:
        query = query.gt(id_column, last_id)
    return query


def apply_text_search(
    query: Any,
    column: str,
    search_term: str | None,
) -> Any:
    """Apply full-text search using ilike."""
    if search_term:
        query = query.ilike(column, f"%{search_term}%")
    return query
