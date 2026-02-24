from fastapi import APIRouter

from candata_api.routers.v1 import (
    entities,
    geography,
    housing,
    indicators,
    procurement,
    search,
    trade,
)

v1_router = APIRouter(prefix="/v1")

v1_router.include_router(indicators.router)
v1_router.include_router(housing.router)
v1_router.include_router(procurement.router)
v1_router.include_router(trade.router)
v1_router.include_router(entities.router)
v1_router.include_router(geography.router)
v1_router.include_router(search.router)
