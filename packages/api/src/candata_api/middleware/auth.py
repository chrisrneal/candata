"""API key and JWT authentication middleware."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from fastapi import Depends, HTTPException, Request

from candata_shared.config import settings
from candata_shared.constants import Tier
from candata_shared.db import get_supabase_client

TIER_ORDER: dict[str, int] = {
    "free": 0,
    "starter": 1,
    "pro": 2,
    "business": 3,
    "enterprise": 4,
}


@dataclass
class AuthUser:
    user_id: str
    tier: Tier
    email: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


def _validate_jwt(token: str) -> dict[str, Any] | None:
    """Validate a Supabase JWT and return its claims."""
    try:
        from jose import jwt as jose_jwt

        claims = jose_jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=["HS256"],
            audience="authenticated",
        )
        return claims
    except Exception:
        return None


async def get_current_user(request: Request) -> AuthUser | None:
    """Extract and validate user from API key or JWT.

    Returns None if no credentials are provided (public access).
    Raises 401 if credentials are invalid.
    """
    # Check API key first
    api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
    if api_key:
        supabase = get_supabase_client(service_role=True)
        result = (
            supabase.table("api_keys")
            .select("user_id, tier, email, metadata")
            .eq("key", api_key)
            .eq("active", True)
            .limit(1)
            .execute()
        )
        if not result.data:
            raise HTTPException(status_code=401, detail="Invalid API key")
        row = result.data[0]
        return AuthUser(
            user_id=row["user_id"],
            tier=row.get("tier", "free"),
            email=row.get("email"),
            metadata=row.get("metadata", {}),
        )

    # Check JWT
    auth_header = request.headers.get("Authorization", "")
    if auth_header.startswith("Bearer "):
        token = auth_header[7:]
        claims = _validate_jwt(token)
        if claims is None:
            raise HTTPException(status_code=401, detail="Invalid or expired token")
        user_id = claims.get("sub", "")
        # Look up user profile for tier
        supabase = get_supabase_client(service_role=True)
        result = (
            supabase.table("profiles")
            .select("tier, email")
            .eq("id", user_id)
            .limit(1)
            .execute()
        )
        tier = "free"
        email = claims.get("email")
        if result.data:
            tier = result.data[0].get("tier", "free")
            email = result.data[0].get("email", email)
        return AuthUser(user_id=user_id, tier=tier, email=email)

    return None


def require_auth(min_tier: str = "free"):
    """Dependency factory that requires authentication at a minimum tier."""

    async def _dependency(
        user: AuthUser | None = Depends(get_current_user),
    ) -> AuthUser:
        if user is None:
            raise HTTPException(
                status_code=401,
                detail="Authentication required",
            )
        user_level = TIER_ORDER.get(user.tier, 0)
        required_level = TIER_ORDER.get(min_tier, 0)
        if user_level < required_level:
            raise HTTPException(
                status_code=403,
                detail=f"This endpoint requires '{min_tier}' tier or above. "
                f"Your current tier is '{user.tier}'.",
            )
        return user

    return _dependency
