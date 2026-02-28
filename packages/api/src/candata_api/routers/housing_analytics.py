"""
CMHC housing comparison and analytics endpoints.

Serves /housing/cma/{geoid}/summary, /housing/compare, and
/housing/affordability/{cma_name}.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from candata_shared.db import get_supabase_client

router = APIRouter(prefix="/housing", tags=["housing-analytics"])

_CACHE = {"Cache-Control": "max-age=3600"}


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class CmaSummary(BaseModel):
    cma_name: str
    cma_geoid: str
    latest_month: str
    starts_latest: int | None
    completions_latest: int | None
    under_construction_latest: int | None
    starts_12mo: int | None
    completions_12mo: int | None


class CmaDataPoint(BaseModel):
    year: int
    month: int
    value: int | None


class CmaTimeSeries(BaseModel):
    cma_name: str
    cma_geoid: str
    data: list[CmaDataPoint]


class AffordabilityPoint(BaseModel):
    year: int
    month: int
    nhpi_composite: float | None
    nhpi_land: float | None
    nhpi_building: float | None
    new_starts_total: int | None


# ---------------------------------------------------------------------------
# GET /housing/cma/{cma_geoid}/summary
# ---------------------------------------------------------------------------


@router.get(
    "/cma/{cma_geoid}/summary",
    response_model=CmaSummary,
    summary="CMA housing summary",
)
async def cma_summary(cma_geoid: str) -> JSONResponse:
    """
    Latest month's housing starts, completions, and units under construction
    for a single CMA (Total dwelling type, Total intended market), plus
    12-month rolling totals.

    **cma_geoid** is the Statistics Canada CMA geo UID (e.g. `535` for Toronto).
    """
    supabase = get_supabase_client()

    result = (
        supabase.table("cmhc_housing")
        .select("cma_name,cma_geoid,year,month,data_type,value")
        .eq("cma_geoid", cma_geoid)
        .eq("dwelling_type", "Total")
        .eq("intended_market", "Total")
        .in_("data_type", ["Starts", "Completions", "UnderConstruction"])
        .order("year", desc=True)
        .order("month", desc=True)
        .limit(500)
        .execute()
    )
    rows = result.data
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No data found for CMA geoUID '{cma_geoid}'. "
            "Use GET /meta/cmas to see valid geoUIDs.",
        )

    cma_name = rows[0]["cma_name"]

    starts_rows = sorted(
        [r for r in rows if r["data_type"] == "Starts"],
        key=lambda r: (r["year"], r["month"]),
        reverse=True,
    )
    if not starts_rows:
        raise HTTPException(
            status_code=404, detail=f"No starts data found for CMA '{cma_geoid}'"
        )

    latest_year = starts_rows[0]["year"]
    latest_month = starts_rows[0]["month"]
    latest_month_str = f"{latest_year}-{latest_month:02d}"

    def _latest(data_type: str) -> int | None:
        matches = [
            r
            for r in rows
            if r["data_type"] == data_type
            and r["year"] == latest_year
            and r["month"] == latest_month
        ]
        return matches[0]["value"] if matches else None

    def _rolling_12(data_type: str) -> int | None:
        typed = sorted(
            [r for r in rows if r["data_type"] == data_type],
            key=lambda r: (r["year"], r["month"]),
            reverse=True,
        )[:12]
        values = [r["value"] for r in typed if r["value"] is not None]
        return sum(values) if values else None

    payload = {
        "cma_name": cma_name,
        "cma_geoid": cma_geoid,
        "latest_month": latest_month_str,
        "starts_latest": _latest("Starts"),
        "completions_latest": _latest("Completions"),
        "under_construction_latest": _latest("UnderConstruction"),
        "starts_12mo": _rolling_12("Starts"),
        "completions_12mo": _rolling_12("Completions"),
    }
    return JSONResponse(content=payload, headers=_CACHE)


# ---------------------------------------------------------------------------
# GET /housing/compare
# ---------------------------------------------------------------------------


@router.get(
    "/compare",
    response_model=list[CmaTimeSeries],
    summary="Compare a housing metric across CMAs",
)
async def compare_cmas(
    cmas: str = Query(..., description="Comma-separated CMA geoUIDs, e.g. 535,505,462"),
    metric: str = Query(..., description="starts | completions | under_construction"),
    dwelling_type: str = Query("Total", description="Single | Semi | Row | Apartment | Total"),
    intended_market: str = Query("Total", description="Freehold | Condo | Rental | Total"),
    from_: str | None = Query(None, alias="from", description="Start month YYYY-MM"),
    to: str | None = Query(None, description="End month YYYY-MM"),
) -> JSONResponse:
    """
    Primary charting endpoint — returns one time series per requested CMA in a
    single response.

    All CMAs are returned together to minimise round-trips for multi-CMA charts.
    An empty `data` array is returned for any CMA that has no matching rows.
    """
    metric_map = {
        "starts": "Starts",
        "completions": "Completions",
        "under_construction": "UnderConstruction",
    }
    if metric not in metric_map:
        raise HTTPException(
            status_code=422,
            detail=f"Invalid metric '{metric}'. Must be one of: {', '.join(metric_map)}",
        )

    cma_list = [c.strip() for c in cmas.split(",") if c.strip()]
    if not cma_list:
        raise HTTPException(status_code=422, detail="'cmas' must contain at least one geoUID")

    # Parse optional date filters
    from_year: int | None = None
    to_year: int | None = None
    if from_:
        try:
            from_year = int(from_[:4])
        except (ValueError, IndexError):
            raise HTTPException(status_code=422, detail="'from' must be in YYYY-MM format")
    if to:
        try:
            to_year = int(to[:4])
        except (ValueError, IndexError):
            raise HTTPException(status_code=422, detail="'to' must be in YYYY-MM format")

    supabase = get_supabase_client()
    query = (
        supabase.table("cmhc_housing")
        .select("cma_name,cma_geoid,year,month,value")
        .in_("cma_geoid", cma_list)
        .eq("data_type", metric_map[metric])
        .eq("dwelling_type", dwelling_type)
        .eq("intended_market", intended_market)
        .order("year", desc=False)
        .order("month", desc=False)
    )
    if from_year is not None:
        query = query.gte("year", from_year)
    if to_year is not None:
        query = query.lte("year", to_year)

    rows = query.limit(5000).execute().data

    grouped: dict[str, dict] = {}
    for row in rows:
        geoid = row["cma_geoid"]
        if geoid not in grouped:
            grouped[geoid] = {"cma_name": row["cma_name"], "cma_geoid": geoid, "data": []}
        grouped[geoid]["data"].append(
            {"year": row["year"], "month": row["month"], "value": row["value"]}
        )

    output = [
        grouped.get(geoid, {"cma_name": geoid, "cma_geoid": geoid, "data": []})
        for geoid in cma_list
    ]
    return JSONResponse(content=output, headers=_CACHE)


# ---------------------------------------------------------------------------
# GET /housing/affordability/{cma_name}
# ---------------------------------------------------------------------------


@router.get(
    "/affordability/{cma_name}",
    response_model=list[AffordabilityPoint],
    summary="Housing affordability trend for a CMA",
)
async def affordability(cma_name: str) -> JSONResponse:
    """
    Combined NHPI (New Housing Price Index) and housing starts time series.

    Joins `nhpi` (price index) with `cmhc_housing` (supply) to produce a
    unified series for affordability trend analysis.

    Returned fields per month:
    - **nhpi_composite** — Total house price index
    - **nhpi_land** — Land component
    - **nhpi_building** — Building component
    - **new_starts_total** — New housing starts (Total dwelling / Total market)
    """
    supabase = get_supabase_client()

    nhpi_res = (
        supabase.table("nhpi")
        .select("year,month,index_component,index_value")
        .eq("cma_name", cma_name)
        .eq("house_type", "Total")
        .in_("index_component", ["Total", "Land", "Building"])
        .order("year")
        .order("month")
        .limit(2000)
        .execute()
    )

    starts_res = (
        supabase.table("cmhc_housing")
        .select("year,month,value")
        .eq("data_type", "Starts")
        .eq("dwelling_type", "Total")
        .eq("intended_market", "Total")
        .ilike("cma_name", f"%{cma_name}%")
        .order("year")
        .order("month")
        .limit(2000)
        .execute()
    )

    if not nhpi_res.data and not starts_res.data:
        raise HTTPException(
            status_code=404,
            detail=f"No affordability data found for CMA '{cma_name}'. "
            "The cma_name must match the NHPI table exactly (e.g. 'Toronto').",
        )

    nhpi_lookup: dict[tuple[int, int], dict[str, float | None]] = {}
    for row in nhpi_res.data:
        key = (row["year"], row["month"])
        nhpi_lookup.setdefault(key, {})[row["index_component"]] = row["index_value"]

    starts_lookup: dict[tuple[int, int], int | None] = {
        (r["year"], r["month"]): r["value"] for r in starts_res.data
    }

    all_dates = sorted(set(nhpi_lookup) | set(starts_lookup))
    output = [
        {
            "year": year,
            "month": month,
            "nhpi_composite": nhpi_lookup.get((year, month), {}).get("Total"),
            "nhpi_land": nhpi_lookup.get((year, month), {}).get("Land"),
            "nhpi_building": nhpi_lookup.get((year, month), {}).get("Building"),
            "new_starts_total": starts_lookup.get((year, month)),
        }
        for year, month in all_dates
    ]
    return JSONResponse(content=output, headers=_CACHE)
