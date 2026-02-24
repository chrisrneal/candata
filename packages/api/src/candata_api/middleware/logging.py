"""Structured request/response logging middleware."""

from __future__ import annotations

import time

import structlog
from fastapi import Request, Response
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

logger = structlog.get_logger()


class LoggingMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        start = time.monotonic()
        log = logger.bind(
            method=request.method,
            path=request.url.path,
            query=str(request.url.query),
            client_ip=request.client.host if request.client else None,
        )

        try:
            response = await call_next(request)
            elapsed_ms = round((time.monotonic() - start) * 1000, 2)
            log.info(
                "request_completed",
                status=response.status_code,
                duration_ms=elapsed_ms,
            )
            return response
        except Exception as exc:
            elapsed_ms = round((time.monotonic() - start) * 1000, 2)
            log.error(
                "request_failed",
                error=str(exc),
                duration_ms=elapsed_ms,
            )
            raise
