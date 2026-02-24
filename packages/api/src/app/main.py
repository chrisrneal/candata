"""
main.py — FastAPI application factory for the candata API.

Start with:
    uvicorn app.main:app --reload --port 8000

Endpoints (stubs, implemented in routers/):
    GET  /health
    GET  /v1/indicators
    GET  /v1/indicators/{id}/values
    GET  /v1/housing/vacancy-rates
    GET  /v1/housing/rents
    GET  /v1/housing/starts
    GET  /v1/procurement/contracts
    GET  /v1/procurement/tenders
    GET  /v1/trade/flows
    POST /v1/auth/register
    POST /v1/auth/login
    GET  /v1/me/api-keys
    POST /v1/me/api-keys
"""

from __future__ import annotations

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from candata_shared.config import settings

log = structlog.get_logger(__name__)


def create_app() -> FastAPI:
    app = FastAPI(
        title="candata API",
        description="Canadian data intelligence platform API",
        version="0.1.0",
        docs_url="/docs",
        redoc_url="/redoc",
        openapi_url="/openapi.json",
    )

    # CORS
    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins_list,
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Health check
    @app.get("/health", tags=["meta"])
    async def health() -> dict[str, str]:
        return {"status": "ok", "version": "0.1.0"}

    # TODO: mount routers
    # from app.routers import indicators, housing, procurement, trade, auth, me
    # app.include_router(indicators.router, prefix="/v1/indicators", tags=["indicators"])
    # app.include_router(housing.router, prefix="/v1/housing", tags=["housing"])
    # app.include_router(procurement.router, prefix="/v1/procurement", tags=["procurement"])
    # app.include_router(trade.router, prefix="/v1/trade", tags=["trade"])
    # app.include_router(auth.router, prefix="/v1/auth", tags=["auth"])
    # app.include_router(me.router, prefix="/v1/me", tags=["me"])

    log.info("app_created", cors_origins=settings.cors_origins_list)
    return app


app = create_app()
