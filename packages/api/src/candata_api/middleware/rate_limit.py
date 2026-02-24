"""Tier-based rate limiting middleware."""

from __future__ import annotations

import time
import threading
from dataclasses import dataclass, field

from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.responses import JSONResponse

from candata_api.responses import error_response

# Limits: (requests_per_period, period_seconds, burst_per_second)
TIER_LIMITS: dict[str, tuple[int, int, int]] = {
    "free": (100, 86400, 2),          # 100/day, 2/sec
    "starter": (5_000, 2_592_000, 5),  # 5k/month, 5/sec
    "pro": (50_000, 2_592_000, 20),    # 50k/month, 20/sec
    "business": (500_000, 2_592_000, 50),
    "enterprise": (5_000_000, 2_592_000, 100),
}

DEFAULT_LIMIT = TIER_LIMITS["free"]


@dataclass
class RateBucket:
    count: int = 0
    period_start: float = 0.0
    burst_count: int = 0
    burst_second: float = 0.0


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app):
        super().__init__(app)
        self._buckets: dict[str, RateBucket] = {}
        self._lock = threading.Lock()

    def _get_key(self, request: Request) -> str:
        api_key = request.headers.get("X-API-Key") or request.query_params.get("api_key")
        if api_key:
            return f"key:{api_key}"
        client = request.client
        ip = client.host if client else "unknown"
        return f"ip:{ip}"

    def _get_tier(self, request: Request) -> str:
        user = getattr(request.state, "user", None)
        if user and hasattr(user, "tier"):
            return user.tier
        return "free"

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        # Skip rate limiting for health checks
        if request.url.path in ("/health", "/ready"):
            return await call_next(request)

        key = self._get_key(request)
        tier = self._get_tier(request)
        max_requests, period_secs, burst_limit = TIER_LIMITS.get(tier, DEFAULT_LIMIT)

        now = time.monotonic()

        with self._lock:
            bucket = self._buckets.setdefault(key, RateBucket(period_start=now, burst_second=now))

            # Reset period if expired
            if now - bucket.period_start >= period_secs:
                bucket.count = 0
                bucket.period_start = now

            # Reset burst window
            if now - bucket.burst_second >= 1.0:
                bucket.burst_count = 0
                bucket.burst_second = now

            # Check limits
            if bucket.count >= max_requests:
                reset_at = bucket.period_start + period_secs
                retry_after = max(1, int(reset_at - now))
                return JSONResponse(
                    status_code=429,
                    content=error_response(
                        "RATE_LIMIT_EXCEEDED",
                        f"Rate limit exceeded. Limit: {max_requests} per period.",
                    ),
                    headers={
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(max_requests),
                        "X-RateLimit-Remaining": "0",
                        "X-RateLimit-Reset": str(int(reset_at)),
                    },
                )

            if bucket.burst_count >= burst_limit:
                return JSONResponse(
                    status_code=429,
                    content=error_response(
                        "BURST_LIMIT_EXCEEDED",
                        f"Burst limit exceeded. Max {burst_limit} requests/second.",
                    ),
                    headers={"Retry-After": "1"},
                )

            bucket.count += 1
            bucket.burst_count += 1
            remaining = max_requests - bucket.count
            reset_at = bucket.period_start + period_secs

        response = await call_next(request)
        response.headers["X-RateLimit-Limit"] = str(max_requests)
        response.headers["X-RateLimit-Remaining"] = str(remaining)
        response.headers["X-RateLimit-Reset"] = str(int(reset_at))
        return response
