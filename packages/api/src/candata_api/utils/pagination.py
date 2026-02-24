"""Cursor-based pagination helpers."""

from __future__ import annotations

import base64
import json
from typing import Any

from fastapi import Query


def encode_cursor(last_id: str, last_sort_value: str | None = None) -> str:
    payload = {"last_id": last_id}
    if last_sort_value is not None:
        payload["last_sort_value"] = last_sort_value
    return base64.urlsafe_b64encode(json.dumps(payload).encode()).decode()


def decode_cursor(cursor: str) -> dict[str, str]:
    try:
        raw = base64.urlsafe_b64decode(cursor.encode())
        return json.loads(raw)
    except Exception:
        return {}


class PaginationParams:
    """Dependency for extracting pagination query params."""

    def __init__(
        self,
        cursor: str | None = Query(None, description="Pagination cursor from previous response"),
        page_size: int = Query(50, ge=1, le=500, description="Number of results per page"),
    ) -> None:
        self.cursor = cursor
        self.page_size = page_size
        self._decoded: dict[str, str] | None = None

    @property
    def decoded_cursor(self) -> dict[str, str]:
        if self._decoded is None:
            self._decoded = decode_cursor(self.cursor) if self.cursor else {}
        return self._decoded

    @property
    def last_id(self) -> str | None:
        return self.decoded_cursor.get("last_id")

    @property
    def last_sort_value(self) -> str | None:
        return self.decoded_cursor.get("last_sort_value")


def build_links(
    path: str,
    params: dict[str, Any],
    items: list[dict[str, Any]],
    page_size: int,
    id_field: str = "id",
    sort_field: str | None = None,
) -> dict[str, str]:
    """Build self/next links for a paginated response."""
    query = "&".join(f"{k}={v}" for k, v in params.items() if v is not None)
    self_link = f"{path}?{query}" if query else path

    links: dict[str, str] = {"self": self_link}

    if len(items) >= page_size and items:
        last = items[-1]
        last_sort = str(last[sort_field]) if sort_field and sort_field in last else None
        next_cursor = encode_cursor(str(last[id_field]), last_sort)
        next_params = {**params, "cursor": next_cursor}
        next_query = "&".join(f"{k}={v}" for k, v in next_params.items() if v is not None)
        links["next"] = f"{path}?{next_query}"

    return links
