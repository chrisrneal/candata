"""
Utility / metadata endpoints.

Serves /meta/cmas and /meta/data-freshness.
"""

from __future__ import annotations

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from candata_shared.db import get_supabase_client

router = APIRouter(prefix="/meta", tags=["meta"])

_CACHE = {"Cache-Control": "max-age=3600"}


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CmaListItem(BaseModel):
    cma_name: str
    cma_geoid: str
    latest_date: str
    record_count: int


class DataFreshness(BaseModel):
    cmhc_housing: str | None
    nhpi: str | None
    building_permits: str | None
    trade_flows: str | None
    comtrade_flows: str | None


# ---------------------------------------------------------------------------
# GET /meta/cmas
# ---------------------------------------------------------------------------


@router.get(
    "/cmas",
    response_model=list[CmaListItem],
    summary="List all CMAs with metadata",
)
async def list_cmas() -> JSONResponse:
    """
    Full list of CMAs found in `cmhc_housing`, with their geoUIDs, latest
    available data month, and total row count.

    Useful for populating CMA selector dropdowns in front-end applications
    and for validating geoUID values before calling other housing endpoints.
    """
    supabase = get_supabase_client()

    rows = (
        supabase.table("cmhc_housing")
        .select("cma_name,cma_geoid,year,month")
        .order("cma_name")
        .limit(200_000)
        .execute()
    ).data

    stats: dict[str, dict] = {}
    for row in rows:
        key = row["cma_geoid"]
        if key not in stats:
            stats[key] = {
                "cma_name": row["cma_name"],
                "cma_geoid": key,
                "latest_year": row["year"],
                "latest_month": row["month"],
                "record_count": 0,
            }
        stats[key]["record_count"] += 1
        if (row["year"], row["month"]) > (
            stats[key]["latest_year"],
            stats[key]["latest_month"],
        ):
            stats[key]["latest_year"] = row["year"]
            stats[key]["latest_month"] = row["month"]

    output = [
        {
            "cma_name": info["cma_name"],
            "cma_geoid": info["cma_geoid"],
            "latest_date": f"{info['latest_year']}-{info['latest_month']:02d}",
            "record_count": info["record_count"],
        }
        for info in sorted(stats.values(), key=lambda x: x["cma_name"])
    ]
    return JSONResponse(content=output, headers=_CACHE)


# ---------------------------------------------------------------------------
# GET /meta/data-freshness
# ---------------------------------------------------------------------------


@router.get(
    "/data-freshness",
    response_model=DataFreshness,
    summary="Latest available date in each data table",
)
async def data_freshness() -> JSONResponse:
    """
    Reports the most recent data point available in each pipeline table.

    Useful for monitoring data staleness and scheduling pipeline runs.
    Returns `null` for tables that are empty or unavailable.

    Tables covered: `cmhc_housing`, `nhpi`, `building_permits`,
    `trade_flows`, `comtrade_flows`.
    """
    supabase = get_supabase_client()
    results: dict[str, str | None] = {}

    def _latest_year_month(table: str) -> str | None:
        try:
            r = (
                supabase.table(table)
                .select("year,month")
                .order("year", desc=True)
                .order("month", desc=True)
                .limit(1)
                .execute()
            )
            if r.data:
                y, m = r.data[0]["year"], r.data[0]["month"]
                return f"{y}-{m:02d}"
        except Exception:
            pass
        return None

    results["cmhc_housing"] = _latest_year_month("cmhc_housing")
    results["nhpi"] = _latest_year_month("nhpi")
    results["building_permits"] = _latest_year_month("building_permits")

    # trade_flows uses ref_date (date column)
    try:
        r = (
            supabase.table("trade_flows")
            .select("ref_date")
            .order("ref_date", desc=True)
            .limit(1)
            .execute()
        )
        results["trade_flows"] = str(r.data[0]["ref_date"])[:10] if r.data else None
    except Exception:
        results["trade_flows"] = None

    # comtrade_flows uses period_year (integer)
    try:
        r = (
            supabase.table("comtrade_flows")
            .select("period_year")
            .order("period_year", desc=True)
            .limit(1)
            .execute()
        )
        results["comtrade_flows"] = str(r.data[0]["period_year"]) if r.data else None
    except Exception:
        results["comtrade_flows"] = None

    return JSONResponse(content=results, headers=_CACHE)
