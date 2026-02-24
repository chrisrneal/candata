"""Standardized API response wrappers."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

T = TypeVar("T")


class ResponseMeta(BaseModel):
    total_count: int | None = None
    page_size: int = 50
    cursor: str | None = None
    source: str | None = None
    last_updated: datetime | None = None


class ApiResponse(BaseModel, Generic[T]):
    data: T
    meta: ResponseMeta = Field(default_factory=ResponseMeta)
    links: dict[str, str] = Field(default_factory=dict)


class ErrorDetail(BaseModel):
    code: str
    message: str
    details: dict[str, Any] = Field(default_factory=dict)
    docs_url: str | None = None


class ApiError(BaseModel):
    error: ErrorDetail


def wrap_response(
    data: Any,
    *,
    total_count: int | None = None,
    page_size: int = 50,
    cursor: str | None = None,
    source: str | None = None,
    last_updated: datetime | None = None,
    links: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Build a standardized API response dict."""
    meta = {
        "total_count": total_count,
        "page_size": page_size,
        "cursor": cursor,
        "source": source,
        "last_updated": last_updated.isoformat() if last_updated else None,
    }
    return {
        "data": data,
        "meta": {k: v for k, v in meta.items() if v is not None},
        "links": links or {},
    }


def error_response(
    code: str,
    message: str,
    *,
    details: dict[str, Any] | None = None,
    docs_url: str | None = None,
) -> dict[str, Any]:
    """Build a standardized error response dict."""
    err: dict[str, Any] = {"code": code, "message": message}
    if details:
        err["details"] = details
    if docs_url:
        err["docs_url"] = docs_url
    return {"error": err}
