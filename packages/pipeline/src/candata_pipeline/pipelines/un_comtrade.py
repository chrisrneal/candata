"""
pipelines/un_comtrade.py — UN Comtrade bilateral trade flows pipeline.

Pulls Canadian import/export trade data from the UN Comtrade API v1 at HS2
or HS6 product-code level, transforms to a clean schema, and upserts to
the comtrade_flows table in Supabase.

The free tier allows 1 req/sec and 500 req/hour. This pipeline includes a
rate limiter that enforces both constraints and logs when throttling.

Usage (as module):
    from candata_pipeline.pipelines.un_comtrade import run
    result = await run(level="hs2", years=[2022, 2023], dry_run=True)

CLI (via run_pipeline.py):
    python scripts/run_pipeline.py comtrade --level hs2 --dry-run
    python scripts/run_pipeline.py comtrade --level hs6 --years 2023

Standalone:
    python -m candata_pipeline.pipelines.un_comtrade --dry-run
"""

from __future__ import annotations

import asyncio
import os
import time
from collections import deque
from datetime import datetime
from typing import Any

import httpx
import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="comtrade")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

COMTRADE_BASE = "https://comtradeapi.un.org/data/v1/get/C/A/HS"
COMTRADE_PUBLIC = "https://comtradeapi.un.org/public/v1/preview/C/A/HS"

CANADA_CODE = 124  # ISO 3166-1 numeric

# Key trading partners: code → name (fallback if API doesn't return name)
KEY_PARTNERS: dict[int, str] = {
    0:   "World",
    840: "United States",
    156: "China",
    276: "Germany",
    392: "Japan",
    410: "South Korea",
    826: "United Kingdom",
    484: "Mexico",
    36:  "Australia",
    528: "Netherlands",
    380: "Italy",
}

# HS2 chapters 01–99 (zero-padded)
HS2_CHAPTERS = [f"{i:02d}" for i in range(1, 100)]

TABLE_NAME = "comtrade_flows"

CONFLICT_COLUMNS = [
    "period_year", "reporter_code", "partner_code",
    "hs2_code", "flow", "hs6_code",
]

# HS2 chapter descriptions (WCO Harmonized System)
HS2_DESCRIPTIONS: dict[str, str] = {
    "01": "Live animals",
    "02": "Meat and edible meat offal",
    "03": "Fish and crustaceans",
    "04": "Dairy produce; eggs; honey",
    "05": "Other animal products",
    "06": "Live trees and other plants",
    "07": "Edible vegetables",
    "08": "Edible fruit and nuts",
    "09": "Coffee, tea, mate and spices",
    "10": "Cereals",
    "11": "Milling industry products",
    "12": "Oil seeds and oleaginous fruits",
    "13": "Lac; gums, resins",
    "14": "Vegetable plaiting materials",
    "15": "Animal or vegetable fats and oils",
    "16": "Preparations of meat or fish",
    "17": "Sugars and sugar confectionery",
    "18": "Cocoa and cocoa preparations",
    "19": "Preparations of cereals, flour, starch",
    "20": "Preparations of vegetables, fruit, nuts",
    "21": "Miscellaneous edible preparations",
    "22": "Beverages, spirits and vinegar",
    "23": "Food industry residues and waste; animal feed",
    "24": "Tobacco",
    "25": "Salt; sulphur; earths and stone",
    "26": "Ores, slag and ash",
    "27": "Mineral fuels, oils and products",
    "28": "Inorganic chemicals",
    "29": "Organic chemicals",
    "30": "Pharmaceutical products",
    "31": "Fertilisers",
    "32": "Tanning or dyeing extracts",
    "33": "Essential oils and cosmetics",
    "34": "Soap, washing preparations",
    "35": "Albuminoidal substances; glues",
    "36": "Explosives; pyrotechnics",
    "37": "Photographic or cinematographic goods",
    "38": "Miscellaneous chemical products",
    "39": "Plastics and articles thereof",
    "40": "Rubber and articles thereof",
    "41": "Raw hides and skins; leather",
    "42": "Articles of leather",
    "43": "Furskins and artificial fur",
    "44": "Wood and articles of wood",
    "45": "Cork and articles of cork",
    "46": "Manufactures of straw; basketware",
    "47": "Pulp of wood; recovered paper",
    "48": "Paper and paperboard",
    "49": "Printed books, newspapers",
    "50": "Silk",
    "51": "Wool, fine or coarse animal hair",
    "52": "Cotton",
    "53": "Other vegetable textile fibres",
    "54": "Man-made filaments",
    "55": "Man-made staple fibres",
    "56": "Wadding, felt; twine, cordage",
    "57": "Carpets and other textile floor coverings",
    "58": "Special woven fabrics",
    "59": "Impregnated textile fabrics",
    "60": "Knitted or crocheted fabrics",
    "61": "Apparel, knitted or crocheted",
    "62": "Apparel, not knitted",
    "63": "Other made up textile articles",
    "64": "Footwear",
    "65": "Headgear",
    "66": "Umbrellas, walking-sticks",
    "67": "Prepared feathers; artificial flowers",
    "68": "Articles of stone, plaster, cement",
    "69": "Ceramic products",
    "70": "Glass and glassware",
    "71": "Precious stones and metals; jewellery",
    "72": "Iron and steel",
    "73": "Articles of iron or steel",
    "74": "Copper and articles thereof",
    "75": "Nickel and articles thereof",
    "76": "Aluminium and articles thereof",
    "78": "Lead and articles thereof",
    "79": "Zinc and articles thereof",
    "80": "Tin and articles thereof",
    "81": "Other base metals; cermets",
    "82": "Tools, implements, cutlery",
    "83": "Miscellaneous articles of base metal",
    "84": "Nuclear reactors, boilers, machinery",
    "85": "Electrical machinery and equipment",
    "86": "Railway locomotives; track fixtures",
    "87": "Vehicles other than railway",
    "88": "Aircraft, spacecraft",
    "89": "Ships, boats",
    "90": "Optical, photographic, medical instruments",
    "91": "Clocks and watches",
    "92": "Musical instruments",
    "93": "Arms and ammunition",
    "94": "Furniture; bedding, mattresses",
    "95": "Toys, games and sports equipment",
    "96": "Miscellaneous manufactured articles",
    "97": "Works of art, antiques",
    "98": "Special classification provisions",
    "99": "Special transactions and commodities",
}

DEFAULT_YEARS = [2019, 2020, 2021, 2022, 2023]


# ---------------------------------------------------------------------------
# Rate limiter
# ---------------------------------------------------------------------------

class RateLimiter:
    """Async rate limiter: max N calls per second, M calls per hour."""

    def __init__(
        self,
        per_second: int = 1,
        per_hour: int = 500,
    ) -> None:
        self._per_second = per_second
        self._per_hour = per_hour
        self._second_timestamps: deque[float] = deque()
        self._hour_timestamps: deque[float] = deque()
        self._total_calls = 0

    async def acquire(self) -> None:
        """Wait until a request slot is available."""
        while True:
            now = time.monotonic()

            # Prune old timestamps
            while self._second_timestamps and now - self._second_timestamps[0] > 1.0:
                self._second_timestamps.popleft()
            while self._hour_timestamps and now - self._hour_timestamps[0] > 3600.0:
                self._hour_timestamps.popleft()

            # Check per-second limit
            if len(self._second_timestamps) >= self._per_second:
                wait = 1.0 - (now - self._second_timestamps[0]) + 0.05
                if wait > 0:
                    log.debug("rate_limit_second", wait_s=round(wait, 2))
                    await asyncio.sleep(wait)
                    continue

            # Check per-hour limit
            if len(self._hour_timestamps) >= self._per_hour:
                wait = 3600.0 - (now - self._hour_timestamps[0]) + 1.0
                log.warning(
                    "rate_limit_hourly",
                    wait_s=round(wait, 1),
                    calls_this_hour=len(self._hour_timestamps),
                )
                await asyncio.sleep(wait)
                continue

            # Slot available — record and return
            self._second_timestamps.append(now)
            self._hour_timestamps.append(now)
            self._total_calls += 1
            return

    @property
    def total_calls(self) -> int:
        return self._total_calls


# ---------------------------------------------------------------------------
# API client
# ---------------------------------------------------------------------------

def _get_api_key() -> str:
    """Read the Comtrade API key from env."""
    key = os.environ.get("COMTRADE_API_KEY", "")
    if not key:
        log.info(
            "comtrade_api_key_missing",
            msg="No COMTRADE_API_KEY — using public preview endpoint",
        )
    return key


async def _fetch_comtrade(
    client: httpx.AsyncClient,
    limiter: RateLimiter,
    *,
    reporter: int = CANADA_CODE,
    period: str,
    partner_code: int,
    flow_code: str,
    cmd_code: str = "TOTAL",
    api_key: str = "",
    max_retries: int = 3,
) -> list[dict[str, Any]]:
    """Fetch one page from the Comtrade API with rate limiting + retries.

    Uses the authenticated endpoint if an API key is available, otherwise
    falls back to the public preview endpoint (limited to 500 rows per call
    and no descriptions).

    Args:
        client:       Shared httpx async client.
        limiter:      Shared RateLimiter instance.
        reporter:     ISO numeric reporter code (124 = Canada).
        period:       Comma-separated years, e.g. "2022,2023".
        partner_code: ISO numeric partner code (0 = World).
        flow_code:    "M" (imports) or "X" (exports).
        cmd_code:     HS code filter ("TOTAL", "AG2", "01", etc.).
        api_key:      Comtrade subscription key.
        max_retries:  Retries on transient errors.

    Returns:
        List of data records from the response.
    """
    params: dict[str, Any] = {
        "reporterCode": reporter,
        "period": period,
        "partnerCode": partner_code,
        "flowCode": flow_code,
        "cmdCode": cmd_code,
    }

    # Choose endpoint and auth
    if api_key:
        url = COMTRADE_BASE
        headers: dict[str, str] = {"Ocp-Apim-Subscription-Key": api_key}
    else:
        url = COMTRADE_PUBLIC
        headers = {}

    for attempt in range(1, max_retries + 1):
        await limiter.acquire()
        try:
            resp = await client.get(url, params=params, headers=headers)

            # Handle 401 — no point retrying without a valid key
            if resp.status_code == 401:
                log.warning(
                    "comtrade_401",
                    msg="Authentication required. Set COMTRADE_API_KEY in .env",
                )
                return []

            # Handle 403 — quota or subscription issue
            if resp.status_code == 403:
                log.warning(
                    "comtrade_403",
                    msg="Access forbidden. Check your API key subscription.",
                )
                return []

            # Handle 429 Too Many Requests
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", "5"))
                log.warning(
                    "comtrade_429",
                    retry_after=retry_after,
                    attempt=attempt,
                )
                await asyncio.sleep(retry_after)
                continue

            # Handle 5xx
            if resp.status_code >= 500:
                log.warning(
                    "comtrade_server_error",
                    status=resp.status_code,
                    attempt=attempt,
                )
                await asyncio.sleep(2 ** attempt)
                continue

            resp.raise_for_status()
            body = resp.json()

            # Comtrade v1 wraps data in {"data": [...]}
            data = body.get("data", [])
            if not isinstance(data, list):
                log.warning("comtrade_unexpected_response", body_keys=list(body.keys()))
                return []

            return data

        except httpx.HTTPStatusError as exc:
            log.warning(
                "comtrade_http_error",
                status=exc.response.status_code,
                attempt=attempt,
                error=str(exc),
            )
            if attempt == max_retries:
                raise
            await asyncio.sleep(2 ** attempt)

        except (httpx.ConnectError, httpx.ReadTimeout) as exc:
            log.warning(
                "comtrade_connection_error",
                attempt=attempt,
                error=str(exc),
            )
            if attempt == max_retries:
                raise
            await asyncio.sleep(2 ** attempt)

    return []


# ---------------------------------------------------------------------------
# Transform
# ---------------------------------------------------------------------------

def _records_to_dataframe(
    records: list[dict[str, Any]],
) -> pl.DataFrame:
    """Convert raw Comtrade JSON records to a polars DataFrame.

    Maps Comtrade field names to our schema:
        period       → period_year
        reporterCode → reporter_code
        partnerCode  → partner_code
        partnerDesc  → partner_name
        cmdCode      → hs2_code / hs6_code
        cmdDesc      → hs2_description / hs6_description
        flowCode     → flow (M→Import, X→Export)
        primaryValue → value_usd
    """
    if not records:
        return pl.DataFrame(schema={
            "period_year": pl.Int32,
            "reporter_code": pl.Int32,
            "partner_code": pl.Int32,
            "partner_name": pl.String,
            "hs2_code": pl.String,
            "hs2_description": pl.String,
            "hs6_code": pl.String,
            "hs6_description": pl.String,
            "flow": pl.String,
            "value_usd": pl.Float64,
        })

    rows: list[dict[str, Any]] = []
    for r in records:
        cmd = str(r.get("cmdCode", "") or "")
        cmd_desc = str(r.get("cmdDesc", "") or "")

        # Determine HS2 vs HS6
        if len(cmd) <= 2:
            hs2 = cmd.zfill(2) if cmd else "00"
            hs2_desc = cmd_desc if cmd_desc else HS2_DESCRIPTIONS.get(hs2)
            hs6 = None
            hs6_desc = None
        else:
            hs2 = cmd[:2]
            hs2_desc = HS2_DESCRIPTIONS.get(hs2)
            hs6 = cmd
            hs6_desc = cmd_desc if cmd_desc else None

        flow_raw = str(r.get("flowCode", "") or "")
        flow = "Import" if flow_raw == "M" else "Export" if flow_raw == "X" else flow_raw

        # Use refYear if available (actual field name in API), fall back to period
        year_val = r.get("refYear") or r.get("period", 0)

        # Partner name: use partnerDesc if available, otherwise look up
        partner_desc = r.get("partnerDesc") or ""
        partner_code_val = int(r.get("partnerCode", 0))
        if not partner_desc:
            partner_desc = KEY_PARTNERS.get(partner_code_val, str(partner_code_val))

        rows.append({
            "period_year": int(year_val),
            "reporter_code": int(r.get("reporterCode", 0)),
            "partner_code": partner_code_val,
            "partner_name": partner_desc,
            "hs2_code": hs2,
            "hs2_description": hs2_desc,
            "hs6_code": hs6,
            "hs6_description": hs6_desc,
            "flow": flow,
            "value_usd": float(r.get("primaryValue", 0) or 0),
        })

    return pl.DataFrame(rows, schema={
        "period_year": pl.Int32,
        "reporter_code": pl.Int32,
        "partner_code": pl.Int32,
        "partner_name": pl.String,
        "hs2_code": pl.String,
        "hs2_description": pl.String,
        "hs6_code": pl.String,
        "hs6_description": pl.String,
        "flow": pl.String,
        "value_usd": pl.Float64,
    })


# ---------------------------------------------------------------------------
# Top products helper
# ---------------------------------------------------------------------------

def get_top_products(
    df: pl.DataFrame,
    flow: str = "Import",
    n: int = 20,
    year: int = 2023,
) -> pl.DataFrame:
    """Return top N HS2 chapters by trade value for a given flow and year.

    Args:
        df:   DataFrame with comtrade_flows schema.
        flow: "Import" or "Export".
        n:    Number of products to return.
        year: Year to filter to.

    Returns:
        DataFrame with columns: hs2_code, hs2_description, value_usd
        sorted by value_usd descending.
    """
    result = (
        df.filter(
            (pl.col("flow") == flow)
            & (pl.col("period_year") == year)
            & ((pl.col("hs6_code").is_null()) | (pl.col("hs6_code") == ""))
        )
        .group_by(["hs2_code", "hs2_description"])
        .agg(pl.col("value_usd").sum().alias("value_usd"))
        .sort("value_usd", descending=True)
        .head(n)
    )
    return result


def _print_top_products(df: pl.DataFrame, flow: str, year: int, n: int = 20) -> None:
    """Print a formatted table of top products."""
    top_df = get_top_products(df, flow=flow, n=n, year=year)
    if top_df.is_empty():
        print(f"  No {flow.lower()} data for {year}")
        return

    print(f"\n{'='*80}")
    print(f"  Top {n} HS2 Chapters — Canada {flow}s, {year}")
    print(f"{'='*80}")
    print(f"  {'HS2':<6} {'Description':<50} {'Value (USD)':>18}")
    print(f"  {'---':<6} {'-'*50:<50} {'-'*18:>18}")

    for row in top_df.iter_rows(named=True):
        desc = (row["hs2_description"] or "")[:50]
        val = row["value_usd"]
        if val >= 1e9:
            val_str = f"${val/1e9:,.1f}B"
        elif val >= 1e6:
            val_str = f"${val/1e6:,.1f}M"
        else:
            val_str = f"${val:,.0f}"
        print(f"  {row['hs2_code']:<6} {desc:<50} {val_str:>18}")
    print()


# ---------------------------------------------------------------------------
# Pipeline runner
# ---------------------------------------------------------------------------

async def run(
    *,
    level: str = "hs2",
    partners: list[int] | None = None,
    years: list[int] | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the UN Comtrade pipeline.

    Downloads Canadian trade data from the Comtrade API, transforms to
    the comtrade_flows schema, and upserts to Supabase.

    Args:
        level:    "hs2" for chapter-level, "hs6" for 6-digit detail.
        partners: List of ISO partner codes (default: KEY_PARTNERS).
        years:    List of years to fetch (default: 2019-2023).
        dry_run:  Transform and print sample but do not write to DB.

    Returns:
        LoadResult with record counts.
    """
    configure_logging()

    if years is None:
        years = DEFAULT_YEARS.copy()
    if partners is None:
        partners = list(KEY_PARTNERS.keys())

    api_key = _get_api_key()
    limiter = RateLimiter(per_second=1, per_hour=500)
    all_records: list[dict[str, Any]] = []

    period_str = ",".join(str(y) for y in years)

    log.info(
        "comtrade_pipeline_start",
        level=level,
        years=years,
        partners=partners,
        dry_run=dry_run,
    )

    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        for partner_code in partners:
            partner_name = KEY_PARTNERS.get(partner_code, str(partner_code))

            if level == "hs2":
                # Two calls per partner: imports + exports at HS2 level
                for flow_code, flow_label in [("M", "Import"), ("X", "Export")]:
                    log.info(
                        "comtrade_fetch",
                        partner=partner_name,
                        flow=flow_label,
                        level="hs2",
                    )
                    data = await _fetch_comtrade(
                        client, limiter,
                        period=period_str,
                        partner_code=partner_code,
                        flow_code=flow_code,
                        cmd_code="AG2",   # All HS2-digit chapters
                        api_key=api_key,
                    )
                    all_records.extend(data)
                    log.info(
                        "comtrade_fetched",
                        partner=partner_name,
                        flow=flow_label,
                        records=len(data),
                    )

            elif level == "hs6":
                # Loop over each HS2 chapter for HS6 detail
                for chapter in HS2_CHAPTERS:
                    for flow_code, flow_label in [("M", "Import"), ("X", "Export")]:
                        log.info(
                            "comtrade_fetch",
                            partner=partner_name,
                            flow=flow_label,
                            chapter=chapter,
                            level="hs6",
                        )
                        data = await _fetch_comtrade(
                            client, limiter,
                            period=period_str,
                            partner_code=partner_code,
                            flow_code=flow_code,
                            cmd_code=chapter,
                            api_key=api_key,
                        )
                        all_records.extend(data)

    log.info(
        "comtrade_fetch_complete",
        total_records=len(all_records),
        api_calls=limiter.total_calls,
    )

    if not all_records:
        log.warning("comtrade_no_data")
        return LoadResult(table=TABLE_NAME)

    # Transform
    df = _records_to_dataframe(all_records)

    # Fill empty hs6_code with empty string for the unique constraint
    df = df.with_columns(
        pl.col("hs6_code").fill_null("").alias("hs6_code"),
    )

    # Deduplicate
    df = df.unique(subset=CONFLICT_COLUMNS, keep="last")

    log.info(
        "comtrade_transform_complete",
        rows=len(df),
        unique_hs2=df["hs2_code"].n_unique(),
        year_range=f"{df['period_year'].min()}-{df['period_year'].max()}"
        if not df.is_empty() else "empty",
    )

    # Dry-run output
    if dry_run:
        print("\n=== DRY RUN — first 20 rows ===")
        with pl.Config(tbl_cols=-1, tbl_rows=20, fmt_str_lengths=60):
            print(df.head(20))

        # Summary
        total_import = df.filter(pl.col("flow") == "Import")["value_usd"].sum()
        total_export = df.filter(pl.col("flow") == "Export")["value_usd"].sum()
        print(f"\nTotal records: {len(df)}")
        print(f"Years: {sorted(df['period_year'].unique().to_list())}")
        print(f"Partners: {df['partner_name'].n_unique()} unique")
        print(f"Total import value: ${total_import/1e9:,.1f}B USD")
        print(f"Total export value: ${total_export/1e9:,.1f}B USD")

        # Top products tables
        for year in sorted(years, reverse=True):
            if year in df["period_year"].unique().to_list():
                _print_top_products(df, "Import", year)
                _print_top_products(df, "Export", year)
                break  # Only print most recent year with data

        return LoadResult(table=TABLE_NAME, records_loaded=len(df))

    # Upsert to Supabase
    loader = SupabaseLoader()
    run_id = await loader.start_pipeline_run(
        "un_comtrade",
        "UN-Comtrade",
        metadata={
            "level": level,
            "years": years,
            "partners": partners,
        },
    )

    try:
        result = await loader.upsert(
            TABLE_NAME, df, conflict_columns=CONFLICT_COLUMNS,
        )
        await loader.finish_pipeline_run(
            run_id, result,
            metadata={
                "rows": result.records_loaded,
                "api_calls": limiter.total_calls,
            },
        )
    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise

    # Print summary
    total_import = df.filter(pl.col("flow") == "Import")["value_usd"].sum()
    total_export = df.filter(pl.col("flow") == "Export")["value_usd"].sum()
    print(f"\nLoaded {result.records_loaded} records to {TABLE_NAME}.")
    print(f"Total import value: ${total_import/1e9:,.1f}B USD")
    print(f"Total export value: ${total_export/1e9:,.1f}B USD")

    log.info(
        "comtrade_pipeline_complete",
        records_loaded=result.records_loaded,
        records_failed=result.records_failed,
        status=result.status,
    )
    return result


# ---------------------------------------------------------------------------
# Standalone CLI
# ---------------------------------------------------------------------------

def _parse_int_list(s: str) -> list[int]:
    """Parse '2019,2020,2021' or '2019-2023' into a list of ints."""
    if "-" in s and "," not in s:
        parts = s.split("-")
        if len(parts) == 2:
            return list(range(int(parts[0]), int(parts[1]) + 1))
    return [int(x.strip()) for x in s.split(",")]


def main() -> None:
    """Standalone CLI entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="UN Comtrade Canadian trade pipeline",
    )
    parser.add_argument(
        "--level", choices=["hs2", "hs6"], default="hs2",
        help="Product code level (default: hs2)",
    )
    parser.add_argument(
        "--partners", type=str, default=None,
        help="Comma-separated ISO partner codes (default: key partners)",
    )
    parser.add_argument(
        "--years", type=str, default="2019-2023",
        help="Years as range (2019-2023) or list (2019,2020). Default: 2019-2023",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print results, do not write to DB",
    )
    args = parser.parse_args()

    years = _parse_int_list(args.years)
    partners = (
        [int(p.strip()) for p in args.partners.split(",")]
        if args.partners
        else None
    )

    asyncio.run(run(
        level=args.level,
        partners=partners,
        years=years,
        dry_run=args.dry_run,
    ))


if __name__ == "__main__":
    main()
