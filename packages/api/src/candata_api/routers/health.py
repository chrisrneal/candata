"""Health check endpoints."""

from __future__ import annotations

from fastapi import APIRouter

router = APIRouter(tags=["health"])


@router.get("/health")
async def health() -> dict:
    return {"status": "ok", "version": "0.1.0"}


@router.get("/ready")
async def ready() -> dict:
    return {"status": "ready"}
