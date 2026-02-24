"""FastAPI application factory."""

from __future__ import annotations

import structlog
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from candata_shared.config import settings

from candata_api.middleware.logging import LoggingMiddleware
from candata_api.middleware.rate_limit import RateLimitMiddleware
from candata_api.routers.health import router as health_router
from candata_api.routers.v1 import v1_router

logger = structlog.get_logger()


def create_app() -> FastAPI:
    app = FastAPI(
        title="CanData API",
        description="Canadian public data intelligence API",
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

    # Custom middleware (order matters: last added = first executed)
    app.add_middleware(RateLimitMiddleware)
    app.add_middleware(LoggingMiddleware)

    # Routers
    app.include_router(health_router)
    app.include_router(v1_router)

    logger.info("app_created", cors_origins=settings.cors_origins_list)
    return app


app = create_app()
