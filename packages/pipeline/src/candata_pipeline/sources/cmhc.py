"""
sources/cmhc.py — CMHC Housing Market Information Portal source adapter.

CMHC publishes housing data through their HMIP portal. This adapter
targets the underlying API used by the portal (similar approach to the
mountainmath/cmhc R package).

HMIP API base: https://www03.cmhc-schl.gc.ca/hmip-pimh

Key endpoints:
  /en/TableMapChart/TableMatchingCriteria  — table discovery
  /en/TableMapChart/GetTableData           — data download as JSON/CSV

We pull three datasets:
  1. Vacancy Rates by bedroom type by CMA
  2. Average Rents by bedroom type by CMA
  3. Housing Starts by dwelling type by CMA/province

Fallback: if HMIP API is unavailable, CMHC also publishes quarterly
data releases on their website as CSV files.

Output schemas:
  Vacancy rates:  geography (cma_name), ref_date, bedroom_type, vacancy_rate, universe
  Average rents:  geography (cma_name), ref_date, bedroom_type, average_rent
  Housing starts: geography (province/cma), ref_date, dwelling_type, units

Usage:
    source = CMHCSource()
    vacancy_df  = await source.run(dataset="vacancy_rates", year=2023)
    rents_df    = await source.run(dataset="average_rents", year=2023)
    starts_df   = await source.run(dataset="housing_starts", year=2023)
"""

from __future__ import annotations

import io
from datetime import date
from typing import Any, Literal

import httpx
import polars as pl
import structlog

from candata_shared.geo import cma_name_to_code, province_name_to_code
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

Dataset = Literal["vacancy_rates", "average_rents", "housing_starts"]

# HMIP survey type codes
_SURVEY_CODES: dict[Dataset, str] = {
    "vacancy_rates": "2",   # Rental Market Survey
    "average_rents": "2",   # Rental Market Survey
    "housing_starts": "1",  # Starts and Completions Survey
}

# Standard bedroom type normalisation
_BEDROOM_NORMALIZE: dict[str, str] = {
    "bachelor": "bachelor",
    "bach.": "bachelor",
    "studio": "bachelor",
    "1 bedroom": "1br",
    "1-bedroom": "1br",
    "1br": "1br",
    "2 bedrooms": "2br",
    "2-bedroom": "2br",
    "2br": "2br",
    "3 bedrooms +": "3br+",
    "3 bedrooms+": "3br+",
    "3-bedroom+": "3br+",
    "3br+": "3br+",
    "total": "total",
    "all": "total",
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
    "apartment": "apartment",
    "apt": "apartment",
    "all types": "total",
    "total": "total",
}

# CMHC Open Data CSV URLs (fallback when HMIP API is blocked)
_OPEN_DATA_URLS: dict[Dataset, str] = {
    "vacancy_rates": (
        "https://www.cmhc-schl.gc.ca/en/professionals/housing-markets-data-and-research/"
        "housing-data/data-tables/rental-market/rental-market-report-data-tables"
    ),
    "average_rents": (
        "https://www.cmhc-schl.gc.ca/en/professionals/housing-markets-data-and-research/"
        "housing-data/data-tables/rental-market/rental-market-report-data-tables"
    ),
    "housing_starts": (
        "https://www03.cmhc-schl.gc.ca/hmip-pimh/en/TableMapChart/GetTableData"
    ),
}


class CMHCSource(BaseSource):
    """Pulls CMHC rental market and housing starts data from HMIP."""

    name = "CMHC"

    _HMIP_BASE = "https://www03.cmhc-schl.gc.ca/hmip-pimh"

    def __init__(self, timeout: float = 60.0) -> None:
        super().__init__()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # HMIP API fetch
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _fetch_hmip(
        self,
        dataset: Dataset,
        year: int,
        geography_type_id: str = "3",  # 3 = CMA
    ) -> dict[str, Any]:
        """
        POST to the HMIP TableMatchingCriteria endpoint.

        Returns JSON payload with 'Headers' and 'Data' arrays.
        """
        survey_code = _SURVEY_CODES[dataset]

        params = {
            "GeographyType": geography_type_id,
            "GeographyId": "35",          # Ontario as default; real impl iterates all
            "DisplayAs": "Table",
            "ForDate": f"{year}-10-01",   # October = fall survey
            "Frequency": "2",             # Semi-annual
            "SurveyTypeId": survey_code,
        }
        url = f"{self._HMIP_BASE}/en/TableMapChart/GetTableData"
        self._log.info("hmip_fetch", url=url, dataset=dataset, year=year)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _fetch_starts_csv(self, year: int) -> bytes:
        """
        Download housing starts CSV from CMHC for a given year.
        Uses the HMIP table download endpoint.
        """
        url = (
            f"{self._HMIP_BASE}/en/TableMapChart/DownloadTbl"
            f"?TableId=2.1.31.3&GeographyId=1&GeographyTypeId=1"
            f"&DisplayAs=Table&ForDate={year}-10-01&Frequency=1"
        )
        self._log.info("hmip_starts_download", url=url, year=year)
        async with httpx.AsyncClient(timeout=self._timeout, follow_redirects=True) as client:
            r = await client.get(url)
            r.raise_for_status()
            return r.content

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        dataset: Dataset = "vacancy_rates",
        year: int | None = None,
        start_date: date | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download CMHC data for the given dataset and year.

        Args:
            dataset:    One of "vacancy_rates", "average_rents", "housing_starts".
            year:       Reference year (defaults to current year - 1).
            start_date: If set, fetch data from this year onwards (overrides year).

        Returns:
            Raw polars DataFrame (schema varies by dataset).
        """
        import datetime
        if year is None:
            year = datetime.date.today().year - 1
        if start_date:
            year = start_date.year

        if dataset == "housing_starts":
            csv_bytes = await self._fetch_starts_csv(year)
            return pl.read_csv(
                io.BytesIO(csv_bytes),
                infer_schema_length=0,
                truncate_ragged_lines=True,
            )

        # Vacancy / rents via HMIP JSON
        payload = await self._fetch_hmip(dataset, year)
        headers: list[str] = payload.get("Headers", [])
        data_rows: list[list[Any]] = payload.get("Data", [])
        if not data_rows:
            return pl.DataFrame({h: [] for h in (headers or ["geography", "value"])})
        return pl.DataFrame(data_rows, schema=headers if headers else None, orient="row")

    def transform(self, raw: pl.DataFrame, *, dataset: Dataset = "vacancy_rates") -> pl.DataFrame:
        """
        Normalize CMHC raw data to domain schema.

        Output columns vary by dataset — see module docstring.
        """
        if raw.is_empty():
            return raw

        df = self._normalize_columns(raw)

        if dataset == "vacancy_rates":
            return self._transform_vacancy(df)
        elif dataset == "average_rents":
            return self._transform_rents(df)
        else:
            return self._transform_starts(df)

    # ------------------------------------------------------------------
    # Dataset-specific transforms
    # ------------------------------------------------------------------

    def _normalize_bedroom(self, raw: str | None) -> str | None:
        if not raw:
            return None
        return _BEDROOM_NORMALIZE.get(raw.strip().lower())

    def _normalize_dwelling(self, raw: str | None) -> str | None:
        if not raw:
            return None
        return _DWELLING_NORMALIZE.get(raw.strip().lower())

    def _transform_vacancy(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize HMIP rental market survey → vacancy_rates schema."""
        # Expected columns (HMIP may vary): geography, date, bedroom_type,
        #   vacancy_rate, universe
        # Try to find sensible columns by pattern
        col_map: dict[str, str] = {}
        for col in df.columns:
            lower = col.lower()
            if "geograph" in lower or lower == "geography":
                col_map["geography"] = col
            elif "date" in lower or "period" in lower:
                col_map["date"] = col
            elif "bedroom" in lower or "type" in lower:
                col_map["bedroom_type"] = col
            elif "vacancy" in lower or "rate" in lower:
                col_map["vacancy_rate"] = col
            elif "universe" in lower or "total" in lower:
                col_map["universe"] = col

        result_cols: dict[str, pl.Expr] = {}

        # Geography → CMA code
        if "geography" in col_map:
            result_cols["cma_name"] = pl.col(col_map["geography"])
            result_cols["sgc_code"] = pl.col(col_map["geography"]).map_elements(
                lambda g: cma_name_to_code(g) or province_name_to_code(g),
                return_dtype=pl.String,
            )

        # Reference date
        if "date" in col_map:
            result_cols["ref_date"] = pl.col(col_map["date"]).str.to_date(strict=False)

        # Bedroom type
        if "bedroom_type" in col_map:
            result_cols["bedroom_type"] = pl.col(col_map["bedroom_type"]).map_elements(
                self._normalize_bedroom, return_dtype=pl.String
            )

        # Vacancy rate
        if "vacancy_rate" in col_map:
            result_cols["vacancy_rate"] = pl.col(col_map["vacancy_rate"]).cast(
                pl.Float64, strict=False
            )

        # Universe
        if "universe" in col_map:
            result_cols["universe"] = pl.col(col_map["universe"]).cast(
                pl.Int64, strict=False
            )

        return df.with_columns(**result_cols).select(list(result_cols.keys()))

    def _transform_rents(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize HMIP rental market survey → average_rents schema."""
        col_map: dict[str, str] = {}
        for col in df.columns:
            lower = col.lower()
            if "geograph" in lower:
                col_map["geography"] = col
            elif "date" in lower or "period" in lower:
                col_map["date"] = col
            elif "bedroom" in lower or "type" in lower:
                col_map["bedroom_type"] = col
            elif "rent" in lower or "average" in lower:
                col_map["average_rent"] = col

        result_cols: dict[str, pl.Expr] = {}
        if "geography" in col_map:
            result_cols["cma_name"] = pl.col(col_map["geography"])
            result_cols["sgc_code"] = pl.col(col_map["geography"]).map_elements(
                lambda g: cma_name_to_code(g) or province_name_to_code(g),
                return_dtype=pl.String,
            )
        if "date" in col_map:
            result_cols["ref_date"] = pl.col(col_map["date"]).str.to_date(strict=False)
        if "bedroom_type" in col_map:
            result_cols["bedroom_type"] = pl.col(col_map["bedroom_type"]).map_elements(
                self._normalize_bedroom, return_dtype=pl.String
            )
        if "average_rent" in col_map:
            result_cols["average_rent"] = pl.col(col_map["average_rent"]).cast(
                pl.Float64, strict=False
            )
        return df.with_columns(**result_cols).select(list(result_cols.keys()))

    def _transform_starts(self, df: pl.DataFrame) -> pl.DataFrame:
        """Normalize housing starts CSV → housing_starts schema."""
        col_map: dict[str, str] = {}
        for col in df.columns:
            lower = col.lower()
            if "geograph" in lower or "area" in lower or "cma" in lower:
                col_map["geography"] = col
            elif "date" in lower or "ref" in lower or "period" in lower:
                col_map["date"] = col
            elif "type" in lower or "dwelling" in lower:
                col_map["dwelling_type"] = col
            elif "unit" in lower or "start" in lower or "total" in lower:
                col_map["units"] = col

        result_cols: dict[str, pl.Expr] = {}
        if "geography" in col_map:
            result_cols["cma_name"] = pl.col(col_map["geography"])
            result_cols["sgc_code"] = pl.col(col_map["geography"]).map_elements(
                lambda g: cma_name_to_code(g) or province_name_to_code(g),
                return_dtype=pl.String,
            )
        if "date" in col_map:
            from candata_shared.time_utils import parse_statcan_date
            result_cols["ref_date"] = pl.col(col_map["date"]).map_elements(
                parse_statcan_date, return_dtype=pl.Date
            )
        if "dwelling_type" in col_map:
            result_cols["dwelling_type"] = pl.col(col_map["dwelling_type"]).map_elements(
                self._normalize_dwelling, return_dtype=pl.String
            )
        if "units" in col_map:
            result_cols["units"] = pl.col(col_map["units"]).cast(pl.Int64, strict=False)

        return df.with_columns(**result_cols).select(list(result_cols.keys()))

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "base_url": self._HMIP_BASE,
            "description": "CMHC Housing Market Information Portal",
            "datasets": list(_SURVEY_CODES.keys()),
        }
