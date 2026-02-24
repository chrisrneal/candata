"""
sources/bankofcanada.py — Bank of Canada Valet API source adapter.

The Valet API serves JSON observations for BoC series (interest rates, FX).

Endpoints:
  GET /observations/{series}/json?start_date=YYYY-MM-DD&end_date=YYYY-MM-DD
  GET /observations/group/{group}/json
  GET /series/{series}/json  (metadata)

Observation response shape:
  {
    "seriesDetail": { "FXUSDCAD": { "label": "...", "description": "..." } },
    "observations": [
      { "d": "2024-01-02", "FXUSDCAD": { "v": "1.3245" } },
      ...
    ]
  }

Series we pull:
  FXUSDCAD       — USD/CAD noon spot rate (daily)
  V39079         — Bank of Canada overnight rate (daily)
  V122530        — Prime business loan rate (daily)
  V80691338      — Conventional 5-year fixed mortgage rate (weekly)

Usage:
    source = BankOfCanadaSource()
    df = await source.run(series=["FXUSDCAD", "V39079"])
    # columns: ref_date, series_code, indicator_id, value
"""

from __future__ import annotations

from datetime import date
from typing import Any

import httpx
import polars as pl
import structlog

from candata_shared.config import settings
from candata_shared.constants import INDICATOR_IDS
from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

# Mapping: BoC series code → candata indicator_id
SERIES_INDICATOR_MAP: dict[str, str] = {
    "FXUSDCAD": "usdcad",
    "V39079": "overnight_rate",
    "V122530": "prime_rate",
    "V80691338": "mortgage_5yr_fixed",
}

# All series we pull in a single request via the "rates" group
DEFAULT_SERIES: list[str] = list(SERIES_INDICATOR_MAP.keys())


class BankOfCanadaSource(BaseSource):
    """Pulls interest rate and FX observations from the BoC Valet API."""

    name = "BoC"

    def __init__(self, timeout: float = 30.0) -> None:
        super().__init__()
        self._base_url = settings.boc_valet_url.rstrip("/")
        self._timeout = timeout

    # ------------------------------------------------------------------
    # HTTP
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=1.0, retry_on=(httpx.HTTPError,))
    async def _fetch_observations(
        self,
        series: list[str],
        start_date: date | None = None,
        end_date: date | None = None,
    ) -> dict[str, Any]:
        """
        Fetch JSON observations for one or more series in a single request.

        When len(series) > 1, uses the comma-joined multi-series endpoint.
        """
        series_str = ",".join(series)
        url = f"{self._base_url}/observations/{series_str}/json"
        params: dict[str, str] = {}
        if start_date:
            params["start_date"] = start_date.isoformat()
        if end_date:
            params["end_date"] = end_date.isoformat()

        self._log.info("boc_fetch", url=url, series=series_str, params=params)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url, params=params)
            response.raise_for_status()
            return response.json()

    # ------------------------------------------------------------------
    # BaseSource interface
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        series: list[str] | None = None,
        start_date: date | None = None,
        end_date: date | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Download observations for the given series list.

        Args:
            series:     BoC series codes. Defaults to all DEFAULT_SERIES.
            start_date: Earliest observation date (inclusive).
            end_date:   Latest observation date (inclusive).

        Returns:
            Raw polars DataFrame with columns:
              d (date str), {series_code} ({"v": "..."}) per series
        """
        series = series or DEFAULT_SERIES
        payload = await self._fetch_observations(series, start_date, end_date)
        observations = payload.get("observations", [])

        if not observations:
            self._log.warning("boc_no_observations", series=series)
            return pl.DataFrame({"d": [], "series_code": [], "raw_value": []})

        # Flatten: one row per (date, series_code)
        rows: list[dict[str, str | None]] = []
        for obs in observations:
            obs_date = obs.get("d", "")
            for code in series:
                if code in obs:
                    raw_v = obs[code].get("v") if isinstance(obs[code], dict) else None
                    rows.append({"d": obs_date, "series_code": code, "raw_value": raw_v})

        return pl.DataFrame(rows, schema={"d": pl.String, "series_code": pl.String, "raw_value": pl.String})

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        """
        Normalize BoC observations to standard indicator_values schema.

        Output columns:
            ref_date     Date     — observation date
            series_code  String   — BoC series code (e.g. "FXUSDCAD")
            indicator_id String   — candata indicator_id
            value        Float64  — numeric observation value
        """
        if raw.is_empty():
            return pl.DataFrame(
                schema={
                    "ref_date": pl.Date,
                    "series_code": pl.String,
                    "indicator_id": pl.String,
                    "value": pl.Float64,
                }
            )

        # Parse date string "YYYY-MM-DD"
        df = raw.with_columns(
            pl.col("d")
            .str.to_date(format="%Y-%m-%d", strict=False)
            .alias("ref_date"),
            pl.col("raw_value")
            .cast(pl.Float64, strict=False)
            .alias("value"),
        )

        # Map series_code → indicator_id
        series_map = SERIES_INDICATOR_MAP
        df = df.with_columns(
            pl.col("series_code")
            .map_elements(lambda s: series_map.get(s), return_dtype=pl.String)
            .alias("indicator_id")
        )

        # Drop unknown series and unparseable dates
        df = df.filter(
            pl.col("ref_date").is_not_null() & pl.col("indicator_id").is_not_null()
        )

        return df.select(["ref_date", "series_code", "indicator_id", "value"])

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "base_url": self._base_url,
            "description": "Bank of Canada Valet API — interest rates and exchange rates",
            "series": DEFAULT_SERIES,
        }
