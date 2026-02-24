"""Shared FastAPI dependencies."""

from __future__ import annotations

from candata_shared.db import get_supabase_client

from candata_api.middleware.auth import AuthUser, get_current_user, require_auth
from candata_api.utils.pagination import PaginationParams

__all__ = [
    "AuthUser",
    "PaginationParams",
    "get_current_user",
    "get_supabase_client",
    "require_auth",
]
