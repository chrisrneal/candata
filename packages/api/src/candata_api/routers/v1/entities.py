"""Entity endpoints."""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query

from candata_api.dependencies import PaginationParams
from candata_api.responses import wrap_response
from candata_api.services import entity_service
from candata_api.utils.pagination import build_links, encode_cursor

router = APIRouter(prefix="/entities", tags=["entities"])


@router.get("")
async def list_entities(
    pagination: PaginationParams = Depends(),
    type: str | None = Query(None, description="Entity type filter"),
    q: str | None = Query(None, description="Search by name"),
):
    data, total = entity_service.search_entities(
        entity_type=type,
        q=q,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        "/v1/entities", {"type": type, "q": q}, data, pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        links=links,
    )


@router.get("/{entity_id}")
async def get_entity(entity_id: str):
    data = entity_service.get_entity(entity_id)
    if data is None:
        raise HTTPException(status_code=404, detail="Entity not found")
    return wrap_response(data)


@router.get("/{entity_id}/relationships")
async def get_entity_relationships(
    entity_id: str,
    pagination: PaginationParams = Depends(),
):
    # Verify entity exists
    entity = entity_service.get_entity(entity_id)
    if entity is None:
        raise HTTPException(status_code=404, detail="Entity not found")

    data, total = entity_service.get_entity_relationships(
        entity_id,
        page_size=pagination.page_size,
        last_id=pagination.last_id,
    )
    cursor = encode_cursor(str(data[-1]["id"])) if data else None
    links = build_links(
        f"/v1/entities/{entity_id}/relationships", {}, data, pagination.page_size,
    )
    return wrap_response(
        data, total_count=total, page_size=pagination.page_size, cursor=cursor,
        links=links,
    )
