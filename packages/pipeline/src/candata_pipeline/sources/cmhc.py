"""
sources/cmhc.py — CMHC Housing Market Information Portal source adapter.

CMHC publishes housing data through Statistics Canada as downloadable CSV
tables and through the CMHC Housing Market Data API.

StatCan tables (vacancy rates, rents, starts by dwelling type):
  34-10-0127 — Vacancy rates, CMA level (total only)
  34-10-0133 — Average rents, CMA level (by bedroom type + structure type)
  34-10-0148 — Housing starts, CMA level (by dwelling type, monthly)

CMHC API (starts, completions, under-construction by dwelling + market):
  https://www.cmhc-schl.gc.ca/api/v2/housing-market-data
  Covers all 35 CMAs, broken down by dwelling type and intended market.

We pull four datasets:
  1. Vacancy Rates by CMA            -> vacancy_rates table
  2. Average Rents by bedroom type   -> average_rents table
  3. Housing Starts by dwelling type  -> housing_starts table
  4. CMHC housing (all CMAs)         -> cmhc_housing table

Output schemas:
  Vacancy rates:  sgc_code, ref_date, bedroom_type, vacancy_rate, universe
  Average rents:  sgc_code, ref_date, bedroom_type, average_rent
  Housing starts: sgc_code, ref_date, dwelling_type, units
  CMHC housing:   cma_name, cma_geoid, year, month, dwelling_type,
                   data_type, intended_market, value

Usage:
    source = CMHCSource()
    df = await source.extract(dataset="vacancy_rates")
    df = await source.extract(dataset="housing_starts", cmhc_geo_ids=[2270])
    df = await source.extract_cmhc_api()
    df = await source.extract_cmhc_api(cma_names=["Toronto", "Vancouver"])
"""

from __future__ import annotations

import asyncio
import io
import logging
import zipfile
from datetime import date
from typing import Any, Literal

import httpx
import polars as pl
import structlog

from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

Dataset = Literal["vacancy_rates", "average_rents", "housing_starts"]

# ---------------------------------------------------------------------------
# CMHC internal geography IDs -> StatCan SGC codes for top CMAs
# ---------------------------------------------------------------------------
CMHC_GEO_TO_SGC: dict[int, str] = {
    2270: "535",   # Toronto
    2480: "462",   # Montreal
    2410: "933",   # Vancouver
    140:  "825",   # Calgary
    160:  "835",   # Edmonton
    1680: "505",   # Ottawa-Gatineau
    1900: "602",   # Winnipeg
    2020: "421",   # Quebec City
    520:  "537",   # Hamilton
    580:  "205",   # Halifax
    1020: "555",   # London
    3340: "935",   # Victoria
    1140: "568",   # Kitchener-Cambridge-Waterloo
    780:  "559",   # Windsor
    1780: "725",   # Saskatoon
    1760: "705",   # Regina
    260:  "305",   # Moncton
    2600: "408",   # Sherbrooke
    200:  "001",   # St. John's
    640:  "595",   # Guelph
    420:  "570",   # St. Catharines-Niagara
    1000: "543",   # Oshawa
    460:  "596",   # Barrie
    2120: "442",   # Saguenay
    2040: "433",   # Trois-Rivieres
    480:  "590",   # Brantford
    540:  "541",   # Peterborough
    360:  "310",   # Saint John
    1380: "996",   # Kelowna
    960:  "620",   # Greater Sudbury
}

# Reverse lookup: SGC code -> CMHC geo ID
SGC_TO_CMHC_GEO: dict[str, int] = {v: k for k, v in CMHC_GEO_TO_SGC.items()}

# Human-friendly CMA names for each CMHC geo ID
CMHC_GEO_NAMES: dict[int, str] = {
    2270: "Toronto",
    2480: "Montréal",
    2410: "Vancouver",
    140:  "Calgary",
    160:  "Edmonton",
    1680: "Ottawa-Gatineau",
    1900: "Winnipeg",
    2020: "Québec",
    520:  "Hamilton",
    580:  "Halifax",
    1020: "London",
    3340: "Victoria",
    1140: "Kitchener-Cambridge-Waterloo",
    780:  "Windsor",
    1780: "Saskatoon",
    1760: "Regina",
    260:  "Moncton",
    2600: "Sherbrooke",
    200:  "St. John's",
    640:  "Guelph",
    420:  "St. Catharines-Niagara",
    1000: "Oshawa",
    460:  "Barrie",
    2120: "Saguenay",
    2040: "Trois-Rivières",
    480:  "Brantford",
    540:  "Peterborough",
    360:  "Saint John",
    1380: "Kelowna",
    960:  "Greater Sudbury",
}

# CMA name (lowercased) -> CMHC geo ID for --cmas filtering
CMA_NAME_TO_CMHC: dict[str, int] = {
    name.lower(): geo_id for geo_id, name in CMHC_GEO_NAMES.items()
}
# Add common short aliases
CMA_NAME_TO_CMHC.update({
    "montreal": 2480,
    "quebec": 2020,
    "quebec city": 2020,
    "ottawa": 1680,
    "kitchener": 1140,
    "st catharines": 420,
    "st. catharines": 420,
    "trois-rivieres": 2040,
    "trois rivieres": 2040,
    "sudbury": 960,
    "st johns": 200,
    "st. johns": 200,
    "st. john's": 200,
})

# ---------------------------------------------------------------------------
# All 36 Canadian CMAs — name -> census geoUID (user-facing key)
# ---------------------------------------------------------------------------
CMA_GEOUIDS: dict[str, str] = {
    "St. John's": "001",
    "Halifax": "205",
    "Moncton": "305",
    "Saint John": "310",
    "Saguenay": "408",
    "Quebec City": "421",
    "Sherbrooke": "433",
    "Trois-Rivières": "442",
    "Montréal": "462",
    "Ottawa-Gatineau": "505",
    "Kingston": "521",
    "Peterborough": "529",
    "Oshawa": "532",
    "Toronto": "535",
    "Hamilton": "537",
    "St. Catharines-Niagara": "539",
    "Kitchener-Cambridge-Waterloo": "541",
    "Brantford": "543",
    "Guelph": "550",
    "London": "555",
    "Windsor": "559",
    "Barrie": "568",
    "Greater Sudbury": "580",
    "Thunder Bay": "595",
    "Winnipeg": "602",
    "Regina": "705",
    "Saskatoon": "725",
    "Lethbridge": "810",
    "Calgary": "825",
    "Edmonton": "835",
    "Kamloops": "920",
    "Kelowna": "915",
    "Abbotsford-Mission": "932",
    "Vancouver": "933",
    "Victoria": "935",
    "Nanaimo": "938",
}

# CMA name -> CMHC HMIP METCODE (internal geo ID used by the ExportTable API).
# Derived from mountainmath/cmhc R package cmhc_cma_translation_data.
CMA_METCODES: dict[str, str] = {
    "St. John's": "1640",
    "Halifax": "0580",
    "Moncton": "1040",
    "Saint John": "1600",
    "Saguenay": "0180",
    "Quebec City": "1400",
    "Sherbrooke": "1800",
    "Trois-Rivières": "2320",
    "Montréal": "1060",
    "Ottawa-Gatineau": "1265",
    "Kingston": "0700",
    "Peterborough": "1320",
    "Oshawa": "1250",
    "Toronto": "2270",
    "Hamilton": "0610",
    "St. Catharines-Niagara": "1160",
    "Kitchener-Cambridge-Waterloo": "0850",
    "Brantford": "0125",
    "Guelph": "0460",
    "London": "0950",
    "Windsor": "2640",
    "Barrie": "0120",
    "Greater Sudbury": "2000",
    "Thunder Bay": "2240",
    "Winnipeg": "2680",
    "Regina": "1490",
    "Saskatoon": "1700",
    "Lethbridge": "0870",
    "Calgary": "0140",
    "Edmonton": "0340",
    "Kamloops": "0650",
    "Kelowna": "0670",
    "Abbotsford-Mission": "0110",
    "Vancouver": "2410",
    "Victoria": "2440",
    "Nanaimo": "1100",
}

# CMA name (lowercased) -> canonical CMA name for --cma filtering
CMA_NAME_TO_GEOUID: dict[str, str] = {
    name.lower(): name for name in CMA_GEOUIDS
}

# ---------------------------------------------------------------------------
# CMHC HMIP ExportTable API (reverse-engineered, same as mountainmath/cmhc R)
# ---------------------------------------------------------------------------
_CMHC_EXPORT_URL = "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart/ExportTable"

_CMHC_COOKIE = (
    "ORDERDESKSID=jFINZMyDxkcEQBY3IJL4p2tWB0kFbPOXLJC7Fv4uVCdYBCNcqIUgi7N53swo1Qty; "
    "DoNotShowIntro=true"
)

# HMIP table IDs for historical CMA-level data by dwelling type
_HMIP_TABLE_IDS: dict[str, str] = {
    "Starts": "1.2.1",
    "Completions": "1.2.2",
    "UnderConstruction": "1.2.3",
}

CMHC_DATA_TYPES: list[str] = ["Starts", "Completions", "UnderConstruction"]

_CMHC_API_SLEEP = 0.15
_CMHC_API_MAX_RETRIES = 3

# File logger for CMHC API errors
_cmhc_error_logger = logging.getLogger("cmhc_errors")


def _setup_cmhc_error_log() -> None:
    """Configure a file handler for cmhc_errors.log (idempotent)."""
    if not _cmhc_error_logger.handlers:
        handler = logging.FileHandler("cmhc_errors.log", mode="a")
        handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)s %(message)s")
        )
        _cmhc_error_logger.addHandler(handler)
        _cmhc_error_logger.setLevel(logging.ERROR)


# ---------------------------------------------------------------------------
# StatCan table IDs for CMHC data (primary source)
# ---------------------------------------------------------------------------
_STATCAN_TABLES: dict[Dataset, str] = {
    "vacancy_rates": "34100127",
    "average_rents": "34100133",
    "housing_starts": "34100148",
}

_STATCAN_CSV_URL = "https://www150.statcan.gc.ca/n1/tbl/csv/{table}-eng.zip"

# ---------------------------------------------------------------------------
# Standard bedroom type normalisation
# ---------------------------------------------------------------------------
_BEDROOM_NORMALIZE: dict[str, str] = {
    "bachelor": "bachelor",
    "bach.": "bachelor",
    "studio": "bachelor",
    "bachelor units": "bachelor",
    "0": "bachelor",
    "1 bedroom": "1br",
    "1-bedroom": "1br",
    "1br": "1br",
    "1": "1br",
    "one bedroom units": "1br",
    "2 bedrooms": "2br",
    "2-bedroom": "2br",
    "2br": "2br",
    "2": "2br",
    "two bedroom units": "2br",
    "3 bedrooms +": "3br+",
    "3 bedrooms+": "3br+",
    "3-bedroom+": "3br+",
    "3br+": "3br+",
    "3+": "3br+",
    "3": "3br+",
    "three bedroom units and over": "3br+",
    "total": "total",
    "all": "total",
    "all bedroom types": "total",
    "total units": "total",
}

# Standard dwelling type normalisation
_DWELLING_NORMALIZE: dict[str, str] = {
    "single-detached": "single",
    "single detached": "single",
    "single": "single",
    "semi-detached": "semi",
    "semi detached": "semi",
    "semi": "semi",
    "row": "row",
    "row housing": "row",
    "townhouse": "row",
    "apartment": "apartment",
    "apartment and other": "apartment",
    "apt": "apartment",
    "all types": "total",
    "total": "total",
    "all": "total",
    "total units": "total",
}

# StatCan GEO name -> SGC code mapping (built from CMA names)
# The StatCan CSV GEO column is "Toronto, Ontario" style.
_GEO_NAME_TO_SGC: dict[str, str] = {}


def _build_geo_name_lookup() -> dict[str, str]:
    """Build a lookup from normalized CMA name to SGC code."""
    if _GEO_NAME_TO_SGC:
        return _GEO_NAME_TO_SGC

    # Province names for matching "City, Province" format
    _provinces = {
        "newfoundland and labrador", "prince edward island", "nova scotia",
        "new brunswick", "quebec", "ontario", "manitoba", "saskatchewan",
        "alberta", "british columbia", "yukon", "northwest territories", "nunavut",
    }

    for geo_id, name in CMHC_GEO_NAMES.items():
        sgc = CMHC_GEO_TO_SGC[geo_id]
        # Normalize: lowercase, strip accents for matching
        key = name.lower().replace("é", "e").replace("è", "e").replace("ê", "e")
        _GEO_NAME_TO_SGC[key] = sgc
        # Also add with common province suffixes expected in StatCan data
        _GEO_NAME_TO_SGC[name.lower()] = sgc

    return _GEO_NAME_TO_SGC


def _extract_sgc_from_geo(geo_name: str) -> str | None:
    """
    Extract SGC code from a StatCan GEO column value like "Toronto, Ontario".

    Falls back to substring matching against known CMA names.
    """
    lookup = _build_geo_name_lookup()

    # Try the city part (before comma)
    city = geo_name.split(",")[0].strip().lower()
    city_normalized = city.replace("é", "e").replace("è", "e").replace("ê", "e")

    if city_normalized in lookup:
        return lookup[city_normalized]
    if city in lookup:
        return lookup[city]

    # Try full string
    full = geo_name.strip().lower()
    full_normalized = full.replace("é", "e").replace("è", "e").replace("ê", "e")
    if full_normalized in lookup:
        return lookup[full_normalized]

    # Fuzzy: check if any known name is a substring
    for key, sgc in lookup.items():
        if key in city_normalized or city_normalized in key:
            return sgc

    return None


def normalize_bedroom(raw: Any) -> str | None:
    """Normalize a bedroom type string to our enum."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return _BEDROOM_NORMALIZE.get(s)


def normalize_dwelling(raw: Any) -> str | None:
    """Normalize a dwelling type string to our enum."""
    if raw is None:
        return None
    s = str(raw).strip().lower()
    return _DWELLING_NORMALIZE.get(s)


class CMHCSource(BaseSource):
    """
    Pulls CMHC rental market and housing starts data.

    Primary: StatCan CSV ZIP downloads.
    Fallback: CMHC HMIP internal API (currently broken, kept for future).
    """

    name = "CMHC"

    _HMIP_API_URL = "https://www03.cmhc-schl.gc.ca/hmip-pimh/api/HMIPService"

    def __init__(self, timeout: float = 60.0) -> None:
        super().__init__()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Primary: StatCan CSV ZIP downloads
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_statcan_zip(self, table_id: str) -> bytes:
        """Download a StatCan CSV ZIP file and return raw bytes."""
        url = _STATCAN_CSV_URL.format(table=table_id)
        self._log.info("statcan_download", url=url, table_id=table_id)
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    def _extract_csv_from_zip(self, zip_bytes: bytes, table_id: str) -> pl.DataFrame:
        """
        Extract the data CSV from a StatCan ZIP file.

        StatCan ZIPs contain two files: {table_id}.csv and {table_id}_MetaData.csv.
        We want the data CSV, not the metadata.
        """
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            # Find the data CSV (not metadata)
            data_file = None
            for name in zf.namelist():
                if name.endswith(".csv") and "MetaData" not in name:
                    data_file = name
                    break

            if not data_file:
                self._log.warning("zip_no_csv", table_id=table_id, files=zf.namelist())
                return pl.DataFrame()

            with zf.open(data_file) as f:
                csv_text = f.read().decode("utf-8-sig")
                return pl.read_csv(
                    io.StringIO(csv_text),
                    infer_schema_length=10000,
                    truncate_ragged_lines=True,
                )

    def _filter_cma_rows(
        self,
        df: pl.DataFrame,
        cmhc_geo_ids: list[int] | None,
    ) -> pl.DataFrame:
        """
        Filter a StatCan DataFrame to only include rows for our mapped CMAs.

        Adds an 'sgc_code' column based on the GEO name.
        """
        if "GEO" not in df.columns:
            return pl.DataFrame()

        # Map GEO names to SGC codes
        df = df.with_columns(
            pl.col("GEO").cast(pl.String).map_elements(
                _extract_sgc_from_geo, return_dtype=pl.String
            ).alias("sgc_code")
        )

        # Drop rows we can't map
        df = df.filter(pl.col("sgc_code").is_not_null())

        # Filter to specific CMAs if requested
        if cmhc_geo_ids is not None:
            target_sgc = {CMHC_GEO_TO_SGC[gid] for gid in cmhc_geo_ids if gid in CMHC_GEO_TO_SGC}
            df = df.filter(pl.col("sgc_code").is_in(list(target_sgc)))

        return df

    def _parse_ref_date(self, raw: str) -> date | None:
        """Parse StatCan REF_DATE strings like '2025' or '2025-06' to date."""
        raw = raw.strip()
        try:
            if len(raw) == 4:  # Year only: "2025"
                return date(int(raw), 10, 1)  # Default to October (vacancy survey month)
            elif len(raw) == 7:  # Year-month: "2025-06"
                parts = raw.split("-")
                return date(int(parts[0]), int(parts[1]), 1)
            else:
                return None
        except (ValueError, IndexError):
            return None

    def _transform_statcan_vacancy(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform StatCan table 34-10-0127 (vacancy rates) into our schema."""
        if df.is_empty():
            return pl.DataFrame()

        # Filter to valid rows with values
        df = df.filter(pl.col("VALUE").is_not_null())

        result = df.with_columns([
            pl.col("REF_DATE").cast(pl.String).map_elements(self._parse_ref_date, return_dtype=pl.Date).alias("ref_date"),
            pl.col("VALUE").cast(pl.Float64, strict=False).alias("vacancy_rate"),
            pl.lit("total").alias("bedroom_type"),  # This table has totals only
        ])

        output_cols = [c for c in ["sgc_code", "ref_date", "bedroom_type", "vacancy_rate"]
                       if c in result.columns]
        return result.select(output_cols).filter(
            pl.col("ref_date").is_not_null() & pl.col("vacancy_rate").is_not_null()
        )

    def _transform_statcan_rents(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform StatCan table 34-10-0133 (average rents) into our schema."""
        if df.is_empty():
            return pl.DataFrame()

        # Filter to valid rows with values
        df = df.filter(pl.col("VALUE").is_not_null())

        # Filter to apartment structures only (most relevant)
        if "Type of structure" in df.columns:
            df = df.filter(
                pl.col("Type of structure").str.contains("(?i)apartment|row and apartment")
            )

        # Map bedroom types from "Type of unit" column
        bedroom_col = "Type of unit" if "Type of unit" in df.columns else None

        result = df.with_columns([
            pl.col("REF_DATE").cast(pl.String).map_elements(self._parse_ref_date, return_dtype=pl.Date).alias("ref_date"),
            pl.col("VALUE").cast(pl.Float64, strict=False).alias("average_rent"),
        ])

        if bedroom_col:
            result = result.with_columns(
                pl.col(bedroom_col).map_elements(
                    normalize_bedroom, return_dtype=pl.String
                ).alias("bedroom_type")
            )
            result = result.filter(pl.col("bedroom_type").is_not_null())

        output_cols = [c for c in ["sgc_code", "ref_date", "bedroom_type", "average_rent"]
                       if c in result.columns]
        return result.select(output_cols).filter(
            pl.col("ref_date").is_not_null() & pl.col("average_rent").is_not_null()
        )

    def _transform_statcan_starts(self, df: pl.DataFrame) -> pl.DataFrame:
        """Transform StatCan table 34-10-0148 (housing starts) into our schema."""
        if df.is_empty():
            return pl.DataFrame()

        # Filter to valid rows with values
        df = df.filter(pl.col("VALUE").is_not_null())

        # Filter to actual starts (not completions, absorptions, etc.)
        if "Housing estimates" in df.columns:
            df = df.filter(
                pl.col("Housing estimates").str.contains("(?i)starts")
            )

        # Map dwelling types
        dwelling_col = "Type of dwelling unit" if "Type of dwelling unit" in df.columns else None

        result = df.with_columns([
            pl.col("REF_DATE").cast(pl.String).map_elements(self._parse_ref_date, return_dtype=pl.Date).alias("ref_date"),
            pl.col("VALUE").cast(pl.Float64, strict=False).alias("units_float"),
        ]).with_columns(
            pl.col("units_float").cast(pl.Int64, strict=False).alias("units")
        )

        if dwelling_col:
            result = result.with_columns(
                pl.col(dwelling_col).map_elements(
                    normalize_dwelling, return_dtype=pl.String
                ).alias("dwelling_type")
            )
            result = result.filter(pl.col("dwelling_type").is_not_null())

        # Aggregate across market types (Homeowner, Rental, Condo, etc.)
        # to get total starts per dwelling type per CMA per date
        group_cols = [c for c in ["sgc_code", "ref_date", "dwelling_type"] if c in result.columns]
        if group_cols and "units" in result.columns:
            result = result.group_by(group_cols).agg(pl.col("units").sum())

        output_cols = [c for c in ["sgc_code", "ref_date", "dwelling_type", "units"]
                       if c in result.columns]
        return result.select(output_cols).filter(
            pl.col("ref_date").is_not_null() & pl.col("units").is_not_null()
        )

    # ------------------------------------------------------------------
    # CMHC HMIP ExportTable — starts / completions / under-construction
    # ------------------------------------------------------------------

    async def _fetch_hmip_table(
        self,
        client: httpx.AsyncClient,
        cma_name: str,
        metcode: str,
        data_type: str,
    ) -> list[dict[str, Any]]:
        """
        Fetch one CMA × DataType CSV from the CMHC ExportTable endpoint.

        Returns flat record dicts for the cmhc_housing schema, or [] on
        failure (after retries).
        """
        table_id = _HMIP_TABLE_IDS[data_type]
        payload = {
            "TableId": table_id,
            "GeographyId": metcode,
            "GeographyTypeId": "3",
            "exportType": "csv",
        }
        last_exc: Exception | None = None
        for attempt in range(_CMHC_API_MAX_RETRIES):
            try:
                resp = await client.post(
                    _CMHC_EXPORT_URL,
                    data=payload,
                    headers={"Cookie": _CMHC_COOKIE},
                )
                resp.raise_for_status()
                csv_text = resp.content.decode("latin-1")
                return self._parse_hmip_csv(cma_name, data_type, csv_text)
            except Exception as exc:
                last_exc = exc
                if attempt < _CMHC_API_MAX_RETRIES - 1:
                    wait = 2 ** attempt
                    self._log.warning(
                        "hmip_retry",
                        cma=cma_name,
                        data_type=data_type,
                        attempt=attempt + 1,
                        wait_s=wait,
                        error=str(exc),
                    )
                    await asyncio.sleep(wait)

        msg = f"FAILED {cma_name} ({metcode}) {data_type}: {last_exc}"
        _cmhc_error_logger.error(msg)
        self._log.error("hmip_failed", cma=cma_name, data_type=data_type, error=str(last_exc))
        return []

    @staticmethod
    def _parse_hmip_csv(
        cma_name: str,
        data_type: str,
        csv_text: str,
    ) -> list[dict[str, Any]]:
        """
        Parse the CMHC ExportTable CSV into flat record dicts.

        The CSV has a header like:
            ,Single,Semi-Detached,Row,Apartment,All,
        and data rows like:
            Jan 2020,152,4,171,"1,985","2,312",
        """
        import re

        geo_id = CMA_GEOUIDS.get(cma_name, "")
        records: list[dict[str, Any]] = []
        lines = csv_text.strip().split("\n")

        # Find the header row (starts with a comma: ",Single,Semi-...")
        header_idx: int | None = None
        for i, line in enumerate(lines):
            stripped = line.strip()
            if stripped.startswith(",") and "Single" in stripped:
                header_idx = i
                break

        if header_idx is None:
            return records

        # Parse column names from header
        header_parts = lines[header_idx].strip().split(",")
        # header_parts[0] is empty (date column), rest are dwelling types
        col_names = [p.strip() for p in header_parts[1:] if p.strip()]

        # Map CMHC column names to our dwelling types
        dwelling_map = {
            "Single": "Single",
            "Semi-Detached": "Semi",
            "Row": "Row",
            "Apartment": "Apartment",
            "All": "Total",
        }

        # Month abbreviation -> month number
        month_map = {
            "Jan": 1, "Feb": 2, "Mar": 3, "Apr": 4, "May": 5, "Jun": 6,
            "Jul": 7, "Aug": 8, "Sep": 9, "Oct": 10, "Nov": 11, "Dec": 12,
        }

        # Parse data rows (start after header)
        for line in lines[header_idx + 1:]:
            stripped = line.strip()
            if not stripped or stripped.startswith("Notes") or stripped.startswith("Source") or stripped.startswith('"'):
                break
            # Skip empty/separator rows
            if stripped == "," or all(c in ", " for c in stripped):
                break

            # Split respecting quoted values like "1,985"
            parts: list[str] = []
            current = ""
            in_quotes = False
            for ch in stripped:
                if ch == '"':
                    in_quotes = not in_quotes
                elif ch == "," and not in_quotes:
                    parts.append(current)
                    current = ""
                else:
                    current += ch
            parts.append(current)

            date_str = parts[0].strip()
            if not date_str:
                continue

            # Parse "Jan 2020" -> year, month
            match = re.match(r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{4})", date_str)
            if not match:
                continue

            month_int = month_map[match.group(1)]
            year_int = int(match.group(2))

            # Filter to 2015+
            if year_int < 2015:
                continue

            # Parse values for each dwelling type column
            for col_idx, col_name in enumerate(col_names):
                dwelling = dwelling_map.get(col_name)
                if not dwelling:
                    continue
                val_idx = col_idx + 1  # offset by 1 (date column)
                if val_idx >= len(parts):
                    continue
                val_str = parts[val_idx].strip().replace(",", "")
                if not val_str or val_str == "**":
                    continue
                try:
                    value_int = int(float(val_str))
                except (ValueError, TypeError):
                    continue

                records.append({
                    "cma_name": cma_name,
                    "cma_geoid": geo_id,
                    "year": year_int,
                    "month": month_int,
                    "dwelling_type": dwelling,
                    "data_type": data_type,
                    "intended_market": "Total",
                    "value": value_int,
                })

        return records

    async def extract_cmhc_api(
        self,
        *,
        cma_names: list[str] | None = None,
    ) -> tuple[pl.DataFrame, int]:
        """
        Fetch starts/completions/under-construction from the CMHC HMIP
        ExportTable endpoint for all 36 CMAs (or a filtered subset).

        Makes 3 requests per CMA (one per data type). Each returns a CSV
        with columns for Single/Semi-Detached/Row/Apartment/All.

        Args:
            cma_names: Optional list of CMA names to filter (case-insensitive).

        Returns:
            (DataFrame with cmhc_housing schema, error_count).
        """
        _setup_cmhc_error_log()

        if cma_names:
            targets: dict[str, str] = {}
            for name in cma_names:
                canonical = CMA_NAME_TO_GEOUID.get(name.strip().lower())
                if canonical:
                    targets[canonical] = CMA_METCODES[canonical]
                else:
                    self._log.warning("cmhc_api_unknown_cma", cma=name)
            if not targets:
                return pl.DataFrame(), 0
        else:
            targets = CMA_METCODES

        self._log.info("cmhc_api_start", n_cmas=len(targets))

        all_records: list[dict[str, Any]] = []
        errors = 0

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            for cma_name, metcode in targets.items():
                self._log.info("cmhc_api_cma_start", cma=cma_name, metcode=metcode)
                for data_type in CMHC_DATA_TYPES:
                    rows = await self._fetch_hmip_table(
                        client, cma_name, metcode, data_type
                    )
                    if rows:
                        all_records.extend(rows)
                    else:
                        errors += 1
                    await asyncio.sleep(_CMHC_API_SLEEP)

                self._log.info("cmhc_api_cma_done", cma=cma_name)

        if not all_records:
            return pl.DataFrame(), errors

        df = pl.from_dicts(all_records).with_columns([
            pl.col("year").cast(pl.Int32),
            pl.col("month").cast(pl.Int32),
            pl.col("value").cast(pl.Int64),
        ])

        self._log.info("cmhc_api_complete", rows=len(df), errors=errors)
        return df, errors

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        dataset: Dataset = "vacancy_rates",
        year: int | None = None,
        start_date: date | None = None,
        cmhc_geo_ids: list[int] | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download CMHC data for the given dataset across all (or selected) CMAs.

        Primary approach: download StatCan CSV ZIP for the whole table,
        then filter to relevant CMAs.

        Args:
            dataset:      One of "vacancy_rates", "average_rents", "housing_starts".
            year:         Reference year (unused, kept for compat).
            start_date:   If set, filter to data from this date onwards.
            cmhc_geo_ids: List of CMHC geo IDs to keep. Defaults to all mapped CMAs.

        Returns:
            Combined polars DataFrame for all requested CMAs.
        """
        self._log.info(
            "cmhc_extract_start",
            dataset=dataset,
            n_cmas=len(cmhc_geo_ids) if cmhc_geo_ids else len(CMHC_GEO_TO_SGC),
        )

        table_id = _STATCAN_TABLES[dataset]

        try:
            zip_bytes = await self._download_statcan_zip(table_id)
            raw_df = self._extract_csv_from_zip(zip_bytes, table_id)

            if raw_df.is_empty():
                self._log.warning("statcan_csv_empty", table_id=table_id)
                return pl.DataFrame()

            # Filter to our CMAs and add sgc_code
            df = self._filter_cma_rows(raw_df, cmhc_geo_ids)

            if df.is_empty():
                self._log.warning("statcan_no_matching_cmas", table_id=table_id)
                return pl.DataFrame()

            # Apply dataset-specific transformation
            if dataset == "vacancy_rates":
                result = self._transform_statcan_vacancy(df)
            elif dataset == "average_rents":
                result = self._transform_statcan_rents(df)
            else:
                result = self._transform_statcan_starts(df)

            # Filter by start_date if specified
            if start_date and "ref_date" in result.columns:
                result = result.filter(pl.col("ref_date") >= start_date)

            self._log.info(
                "cmhc_extract_complete",
                dataset=dataset,
                total_rows=len(result),
            )
            return result

        except Exception as exc:
            self._log.error("statcan_download_failed", table_id=table_id, error=str(exc))
            return pl.DataFrame()

    def transform(self, raw: pl.DataFrame, *, dataset: Dataset = "vacancy_rates") -> pl.DataFrame:
        """
        Normalize CMHC raw data to domain schema.

        Data is already transformed during extraction via StatCan CSVs.
        This method is kept for the BaseSource interface.
        """
        return raw

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "statcan_tables": _STATCAN_TABLES,
            "description": "CMHC Housing Market data via Statistics Canada",
            "datasets": ["vacancy_rates", "average_rents", "housing_starts"],
            "n_cmas": len(CMHC_GEO_TO_SGC),
        }
