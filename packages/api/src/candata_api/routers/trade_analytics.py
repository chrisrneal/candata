"""
Trade analytics endpoints.

Serves /trade/top-products, /trade/timeseries, and /trade/province-breakdown.
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from candata_shared.db import get_supabase_client

router = APIRouter(prefix="/trade", tags=["trade-analytics"])

_CACHE = {"Cache-Control": "max-age=3600"}


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class ProductRank(BaseModel):
    hs2_code: str
    hs2_description: str | None
    value: float
    prior_value: float | None
    yoy_change_pct: float | None


class TradeDataPoint(BaseModel):
    year: int
    partner_name: str | None
    value_usd: float


class ProvinceRow(BaseModel):
    province: str
    total_value: float


# ---------------------------------------------------------------------------
# GET /trade/top-products
# ---------------------------------------------------------------------------


@router.get(
    "/top-products",
    response_model=list[ProductRank],
    summary="Top N product categories by trade value",
)
async def top_products(
    flow: str = Query(..., description="Import or Export"),
    year: int = Query(..., description="Reference year, e.g. 2023"),
    n: int = Query(20, ge=1, le=100, description="Number of products to return"),
    source: str = Query(
        "comtrade",
        description="Data source: comtrade (HS2, annual) or statcan (NAPCS, monthly→annual)",
    ),
) -> JSONResponse:
    """
    Top N product categories by trade value for a given year and flow.

    Includes year-over-year change percentage where prior-year data is available.

    - **source=comtrade** (default): HS2 chapters from UN Comtrade, annual totals.
    - **source=statcan**: NAPCS codes from Statistics Canada, summed to annual totals.
    """
    if flow not in ("Import", "Export"):
        raise HTTPException(status_code=422, detail="'flow' must be 'Import' or 'Export'")
    if source not in ("comtrade", "statcan"):
        raise HTTPException(
            status_code=422, detail="'source' must be 'comtrade' or 'statcan'"
        )

    supabase = get_supabase_client()

    if source == "comtrade":
        rows = (
            supabase.table("comtrade_flows")
            .select("hs2_code,hs2_description,period_year,value_usd")
            .eq("flow", flow)
            .in_("period_year", [year, year - 1])
            .eq("hs6_code", "")  # summary rows only
            .limit(10_000)
            .execute()
        ).data

        current: dict[str, dict] = {}
        prior: dict[str, float] = {}
        for row in rows:
            code = row["hs2_code"]
            val = row.get("value_usd") or 0.0
            if row["period_year"] == year:
                if code not in current:
                    current[code] = {
                        "hs2_description": row.get("hs2_description"),
                        "value": 0.0,
                    }
                current[code]["value"] += val
            elif row["period_year"] == year - 1:
                prior[code] = prior.get(code, 0.0) + val

        ranked = sorted(current.items(), key=lambda x: x[1]["value"], reverse=True)[:n]
        output = []
        for code, info in ranked:
            pv = prior.get(code)
            yoy = round((info["value"] - pv) / pv * 100, 2) if pv else None
            output.append(
                {
                    "hs2_code": code,
                    "hs2_description": info["hs2_description"],
                    "value": round(info["value"], 2),
                    "prior_value": round(pv, 2) if pv is not None else None,
                    "yoy_change_pct": yoy,
                }
            )
        return JSONResponse(content=output, headers=_CACHE)

    # statcan source: trade_flows_hs6 (NAPCS-level, monthly)
    rows = (
        supabase.table("trade_flows_hs6")
        .select("napcs_code,napcs_description,ref_year,value_cad_millions")
        .eq("trade_flow", flow)
        .in_("ref_year", [year, year - 1])
        .limit(50_000)
        .execute()
    ).data

    current_s: dict[str, dict] = {}
    prior_s: dict[str, float] = {}
    for row in rows:
        code = row["napcs_code"]
        val = row.get("value_cad_millions") or 0.0
        if row["ref_year"] == year:
            if code not in current_s:
                current_s[code] = {
                    "hs2_description": row.get("napcs_description"),
                    "value": 0.0,
                }
            current_s[code]["value"] += val
        elif row["ref_year"] == year - 1:
            prior_s[code] = prior_s.get(code, 0.0) + val

    ranked_s = sorted(current_s.items(), key=lambda x: x[1]["value"], reverse=True)[:n]
    output_s = []
    for code, info in ranked_s:
        pv = prior_s.get(code)
        yoy = round((info["value"] - pv) / pv * 100, 2) if pv else None
        output_s.append(
            {
                "hs2_code": code,
                "hs2_description": info["hs2_description"],
                "value": round(info["value"], 2),
                "prior_value": round(pv, 2) if pv is not None else None,
                "yoy_change_pct": yoy,
            }
        )
    return JSONResponse(content=output_s, headers=_CACHE)


# ---------------------------------------------------------------------------
# GET /trade/timeseries
# ---------------------------------------------------------------------------


@router.get(
    "/timeseries",
    response_model=list[TradeDataPoint],
    summary="Annual trade time series for an HS2 chapter",
)
async def trade_timeseries(
    hs2: str = Query(..., description="2-digit HS chapter code, e.g. '87' (vehicles)"),
    flow: str = Query(..., description="Import or Export"),
    from_year: int = Query(2019, description="Start year (inclusive)"),
    to_year: int = Query(2023, description="End year (inclusive)"),
    partners: str | None = Query(
        None,
        description="Comma-separated partner names; omit for all partners aggregated",
    ),
) -> JSONResponse:
    """
    Annual time series for a specific HS2 product chapter from UN Comtrade.

    When **partners** is omitted, all partners are returned as individual series
    rows — useful for stacking or further aggregation by the client.

    Use `hs2=87` for vehicles, `hs2=84` for machinery, `hs2=27` for energy, etc.
    """
    if flow not in ("Import", "Export"):
        raise HTTPException(status_code=422, detail="'flow' must be 'Import' or 'Export'")
    if from_year > to_year:
        raise HTTPException(
            status_code=422, detail="'from_year' must be less than or equal to 'to_year'"
        )

    supabase = get_supabase_client()
    query = (
        supabase.table("comtrade_flows")
        .select("period_year,partner_name,value_usd")
        .eq("hs2_code", hs2)
        .eq("flow", flow)
        .gte("period_year", from_year)
        .lte("period_year", to_year)
        .eq("hs6_code", "")
        .limit(10_000)
    )
    if partners:
        partner_list = [p.strip() for p in partners.split(",") if p.strip()]
        query = query.in_("partner_name", partner_list)

    rows = query.execute().data
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No Comtrade data found for HS2='{hs2}', flow='{flow}', "
            f"years {from_year}–{to_year}.",
        )

    # Aggregate by (year, partner)
    agg: dict[tuple[int, str], float] = {}
    for row in rows:
        key = (row["period_year"], row.get("partner_name") or "Unknown")
        agg[key] = agg.get(key, 0.0) + (row.get("value_usd") or 0.0)

    output = [
        {"year": yr, "partner_name": partner, "value_usd": round(val, 2)}
        for (yr, partner), val in sorted(agg.items())
    ]
    return JSONResponse(content=output, headers=_CACHE)


# ---------------------------------------------------------------------------
# GET /trade/province-breakdown
# ---------------------------------------------------------------------------


@router.get(
    "/province-breakdown",
    response_model=list[ProvinceRow],
    summary="Trade value by province for a year",
)
async def province_breakdown(
    year: int = Query(..., description="Reference year, e.g. 2023"),
    flow: str = Query(..., description="Import or Export"),
    napcs_code: str | None = Query(None, description="Optional NAPCS product code filter"),
) -> JSONResponse:
    """
    Provincial breakdown of trade value using Statistics Canada HS6/NAPCS data.

    Returns provinces ranked by total trade value. Optionally filter to a
    single NAPCS product code.

    Province codes follow Statistics Canada SGC conventions (e.g. `'35'` for
    Ontario, `'24'` for Quebec).
    """
    if flow not in ("Import", "Export"):
        raise HTTPException(status_code=422, detail="'flow' must be 'Import' or 'Export'")

    supabase = get_supabase_client()
    query = (
        supabase.table("trade_flows_hs6")
        .select("province,value_cad_millions")
        .eq("trade_flow", flow)
        .eq("ref_year", year)
        .limit(50_000)
    )
    if napcs_code:
        query = query.eq("napcs_code", napcs_code)

    rows = query.execute().data
    if not rows:
        raise HTTPException(
            status_code=404,
            detail=f"No province trade data found for year={year}, flow='{flow}'.",
        )

    totals: dict[str, float] = {}
    for row in rows:
        prov = row["province"]
        totals[prov] = totals.get(prov, 0.0) + (row.get("value_cad_millions") or 0.0)

    output = [
        {"province": prov, "total_value": round(val, 2)}
        for prov, val in sorted(totals.items(), key=lambda x: x[1], reverse=True)
    ]
    return JSONResponse(content=output, headers=_CACHE)
