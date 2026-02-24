"""Cross-product search endpoint."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query

from candata_api.responses import wrap_response
from candata_api.services import search_service

router = APIRouter(tags=["search"])


@router.get("/search")
async def search(
    q: str = Query(..., min_length=1, description="Search query"),
    limit: int = Query(20, ge=1, le=100),
):
    """Search across indicators, geographies, contracts, and entities."""
    results = search_service.cross_search(q, limit=limit)
    return wrap_response(results, total_count=len(results))
