"""
pipelines/trade.py — International merchandise trade pipeline.

Source: Statistics Canada Table 12-10-0011-01
  "International merchandise trade by commodity, province/territory, and
   trading partner (x 1,000,000)"

Downloads the full table CSV via StatCanSource, pivots to trade_flows
schema, and upserts into the trade_flows table.

trade_flows schema:
  direction       TEXT  — "import" | "export"
  hs_code         TEXT  — HS chapter or heading (2–4 digit)
  hs_description  TEXT
  partner_country TEXT  — ISO 3166-1 alpha-3
  province        TEXT  — 2-digit SGC code
  ref_date        DATE
  value_cad       NUMERIC

StatCan column mapping:
  REF_DATE  → ref_date (YYYY-MM → first of month)
  GEO       → province (SGC code via normalize_statcan_geo)
  Trade     → direction ("Import" → "import", "Export" → "export")
  Principal trading partners → partner_country (mapped to ISO-3 code)
  Harmonized System section  → hs_code
  VALUE     → value_cad (table is in $1,000 — multiply by 1000)

Usage:
    from candata_pipeline.pipelines.trade import run
    result = await run(start_date=date(2020, 1, 1))
"""

from __future__ import annotations

from datetime import date

import polars as pl
import structlog

from candata_pipeline.loaders.supabase_loader import LoadResult, SupabaseLoader
from candata_pipeline.sources.statcan import StatCanSource
from candata_pipeline.transforms.time_series import deduplicate_series
from candata_pipeline.utils.logging import configure_logging, get_logger

log = get_logger(__name__, pipeline="trade")

TRADE_TABLE_PID = "1210001101"

# Common StatCan partner country strings → ISO 3166-1 alpha-3
COUNTRY_MAP: dict[str, str] = {
    "united states": "USA",
    "united states of america": "USA",
    "u.s.a.": "USA",
    "u.s.": "USA",
    "china": "CHN",
    "china, people's republic of": "CHN",
    "united kingdom": "GBR",
    "u.k.": "GBR",
    "japan": "JPN",
    "germany": "DEU",
    "south korea": "KOR",
    "korea, republic of": "KOR",
    "mexico": "MEX",
    "france": "FRA",
    "italy": "ITA",
    "netherlands": "NLD",
    "belgium": "BEL",
    "spain": "ESP",
    "switzerland": "CHE",
    "india": "IND",
    "brazil": "BRA",
    "australia": "AUS",
    "norway": "NOR",
    "sweden": "SWE",
    "denmark": "DNK",
    "hong kong": "HKG",
    "taiwan": "TWN",
    "singapore": "SGP",
    "all countries": "ALL",
    "total": "ALL",
}


def normalize_country(name: str | None) -> str | None:
    if not name:
        return None
    return COUNTRY_MAP.get(name.strip().lower(), name.strip().upper()[:3])


def normalize_direction(raw: str | None) -> str | None:
    if not raw:
        return None
    lower = raw.strip().lower()
    if "import" in lower:
        return "import"
    if "export" in lower:
        return "export"
    return None


async def run(
    *,
    start_date: date | None = None,
    end_date: date | None = None,
    dry_run: bool = False,
) -> LoadResult:
    """
    Run the trade flows pipeline.

    Args:
        start_date: Earliest ref_date to include.
        end_date:   Latest ref_date to include.
        dry_run:    Transform but do not write to Supabase.

    Returns:
        LoadResult.
    """
    configure_logging()
    log.info("trade_pipeline_start", start_date=str(start_date), dry_run=dry_run)

    loader = SupabaseLoader()
    run_id = await loader.start_pipeline_run(
        "trade", "StatCan", metadata={"start_date": str(start_date), "dry_run": dry_run}
    )

    try:
        source = StatCanSource()
        raw = await source.extract(table_pid=TRADE_TABLE_PID)
        df = source.transform(raw, start_date=start_date)

        if df.is_empty():
            log.warning("trade_empty_after_transform")
            dummy = LoadResult(table="trade_flows")
            await loader.finish_pipeline_run(run_id, dummy)
            return dummy

        # Detect direction column — StatCan labels it "Trade"
        direction_col = next(
            (c for c in df.columns if "trade" in c.lower() and c != "ref_date"), None
        )
        partner_col = next(
            (c for c in df.columns
             if "partner" in c.lower() or "country" in c.lower()), None
        )
        hs_col = next(
            (c for c in df.columns if "hs" in c.lower() or "harmonized" in c.lower()), None
        )

        exprs: list[pl.Expr] = [
            pl.col("ref_date"),
            pl.col("sgc_code").alias("province"),
            pl.col("value").alias("value_cad"),
        ]

        if direction_col:
            exprs.append(
                pl.col(direction_col)
                .map_elements(normalize_direction, return_dtype=pl.String)
                .alias("direction")
            )
        else:
            exprs.append(pl.lit("export").alias("direction"))

        if partner_col:
            exprs.append(
                pl.col(partner_col)
                .map_elements(normalize_country, return_dtype=pl.String)
                .alias("partner_country")
            )
        else:
            exprs.append(pl.lit("USA").alias("partner_country"))

        if hs_col:
            exprs.append(pl.col(hs_col).alias("hs_code"))
        else:
            exprs.append(pl.lit("00").alias("hs_code"))

        df = df.with_columns(exprs)

        # Filter to valid rows
        df = df.filter(
            pl.col("province").is_not_null()
            & pl.col("direction").is_not_null()
            & pl.col("value_cad").is_not_null()
        )

        # Scale: StatCan trade table is in thousands of dollars
        df = df.with_columns(
            (pl.col("value_cad") * 1_000).alias("value_cad")
        )

        # Add UUIDs
        import uuid as uuid_module
        df = df.with_columns(
            pl.Series("id", [str(uuid_module.uuid4()) for _ in range(len(df))], dtype=pl.String)
        )

        output_cols = [c for c in [
            "id", "direction", "hs_code", "partner_country", "province",
            "ref_date", "value_cad"
        ] if c in df.columns]
        df = df.select(output_cols)

        df = deduplicate_series(
            df,
            ["direction", "hs_code", "partner_country", "province", "ref_date"],
        )

        log.info("trade_flows_ready", rows=len(df))

        if dry_run:
            dummy = LoadResult(table="trade_flows", records_loaded=len(df))
            await loader.finish_pipeline_run(run_id, dummy, records_extracted=len(df))
            return dummy

        result = await loader.upsert(
            "trade_flows",
            df,
            conflict_columns=["direction", "hs_code", "partner_country", "province", "ref_date"],
        )
        await loader.finish_pipeline_run(run_id, result, records_extracted=len(df))
        return result

    except Exception as exc:
        await loader.fail_pipeline_run(run_id, str(exc))
        raise
