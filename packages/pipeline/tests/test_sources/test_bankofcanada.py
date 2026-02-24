"""
tests/test_sources/test_bankofcanada.py — Unit tests for BankOfCanadaSource.

HTTP is mocked with respx; fixture JSON mirrors actual BoC Valet response.
"""

from __future__ import annotations

import json
from datetime import date
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from candata_pipeline.sources.bankofcanada import BankOfCanadaSource, SERIES_INDICATOR_MAP

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def boc_payload() -> dict:
    return json.loads((FIXTURES / "boc_valet_sample.json").read_text())


# ---------------------------------------------------------------------------
# extract() tests
# ---------------------------------------------------------------------------

class TestBoCExtract:
    @pytest.mark.asyncio
    async def test_extract_returns_dataframe(self, boc_payload: dict):
        source = BankOfCanadaSource()
        with respx.mock() as router:
            router.get(url__regex=r".*valet/observations.*").mock(
                return_value=httpx.Response(200, json=boc_payload)
            )
            df = await source.extract(series=["FXUSDCAD", "V39079", "V80691338"])

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "d" in df.columns
        assert "series_code" in df.columns
        assert "raw_value" in df.columns

    @pytest.mark.asyncio
    async def test_extract_flattens_multiple_series(self, boc_payload: dict):
        source = BankOfCanadaSource()
        with respx.mock() as router:
            router.get(url__regex=r".*valet.*").mock(
                return_value=httpx.Response(200, json=boc_payload)
            )
            df = await source.extract(series=["FXUSDCAD", "V39079"])

        # Each observation date should produce one row per series
        series_codes = df["series_code"].unique().to_list()
        assert "FXUSDCAD" in series_codes
        assert "V39079" in series_codes

    @pytest.mark.asyncio
    async def test_extract_with_date_range(self, boc_payload: dict):
        source = BankOfCanadaSource()
        with respx.mock() as router:
            route = router.get(url__regex=r".*valet.*").mock(
                return_value=httpx.Response(200, json=boc_payload)
            )
            await source.extract(
                series=["FXUSDCAD"],
                start_date=date(2023, 1, 1),
                end_date=date(2023, 6, 30),
            )
            # Verify date params were passed
            assert route.called
            request = route.calls[0].request
            assert "start_date" in str(request.url)

    @pytest.mark.asyncio
    async def test_extract_empty_observations(self):
        source = BankOfCanadaSource()
        empty_payload = {"observations": [], "seriesDetail": {}}
        with respx.mock() as router:
            router.get(url__regex=r".*valet.*").mock(
                return_value=httpx.Response(200, json=empty_payload)
            )
            df = await source.extract(series=["FXUSDCAD"])

        assert df.is_empty() or len(df) == 0

    @pytest.mark.asyncio
    async def test_extract_raises_on_server_error(self):
        source = BankOfCanadaSource()
        with respx.mock() as router:
            router.get(url__regex=r".*valet.*").mock(
                return_value=httpx.Response(503)
            )
            with pytest.raises(httpx.HTTPStatusError):
                await source.extract(series=["FXUSDCAD"])


# ---------------------------------------------------------------------------
# transform() tests
# ---------------------------------------------------------------------------

class TestBoCTransform:
    @pytest.fixture
    def raw_boc(self, boc_payload: dict) -> pl.DataFrame:
        """Simulate output of extract() from the fixture payload."""
        observations = boc_payload["observations"]
        rows = []
        for obs in observations:
            d = obs["d"]
            for code in ["FXUSDCAD", "V39079", "V80691338"]:
                if code in obs:
                    rows.append({"d": d, "series_code": code, "raw_value": obs[code].get("v")})
        return pl.DataFrame(rows)

    def test_transform_creates_ref_date_column(self, raw_boc: pl.DataFrame):
        source = BankOfCanadaSource()
        df = source.transform(raw_boc)
        assert "ref_date" in df.columns
        assert df["ref_date"].dtype == pl.Date

    def test_transform_parses_value_as_float(self, raw_boc: pl.DataFrame):
        source = BankOfCanadaSource()
        df = source.transform(raw_boc)
        assert "value" in df.columns
        assert df["value"].dtype == pl.Float64
        # FXUSDCAD values should be around 1.3x
        fx_rows = df.filter(pl.col("series_code") == "FXUSDCAD")
        assert fx_rows["value"].mean() > 1.0
        assert fx_rows["value"].mean() < 2.0

    def test_transform_maps_series_to_indicator_id(self, raw_boc: pl.DataFrame):
        source = BankOfCanadaSource()
        df = source.transform(raw_boc)
        assert "indicator_id" in df.columns
        indicator_ids = df["indicator_id"].unique().to_list()
        assert "usdcad" in indicator_ids
        assert "overnight_rate" in indicator_ids

    def test_transform_drops_unknown_series(self):
        source = BankOfCanadaSource()
        raw = pl.DataFrame({
            "d": ["2023-01-01", "2023-01-02"],
            "series_code": ["FXUSDCAD", "UNKNOWN_SERIES"],
            "raw_value": ["1.3544", "99.9"],
        })
        df = source.transform(raw)
        assert "UNKNOWN_SERIES" not in df["series_code"].to_list()
        assert len(df) == 1  # only the known series

    def test_transform_drops_invalid_dates(self):
        source = BankOfCanadaSource()
        raw = pl.DataFrame({
            "d": ["2023-01-01", "not-a-date"],
            "series_code": ["FXUSDCAD", "FXUSDCAD"],
            "raw_value": ["1.3544", "1.3500"],
        })
        df = source.transform(raw)
        assert len(df) == 1  # invalid date row dropped

    def test_transform_output_columns(self, raw_boc: pl.DataFrame):
        source = BankOfCanadaSource()
        df = source.transform(raw_boc)
        assert set(df.columns) == {"ref_date", "series_code", "indicator_id", "value"}

    def test_transform_empty_input(self):
        source = BankOfCanadaSource()
        empty = pl.DataFrame({"d": [], "series_code": [], "raw_value": []})
        df = source.transform(empty)
        assert df.is_empty()
        # Schema should still have the right columns
        assert "ref_date" in df.columns


# ---------------------------------------------------------------------------
# SERIES_INDICATOR_MAP tests
# ---------------------------------------------------------------------------

def test_all_boc_series_have_indicator_ids():
    """Ensure every series in DEFAULT_SERIES maps to a known indicator ID."""
    from candata_pipeline.sources.bankofcanada import DEFAULT_SERIES
    from candata_shared.constants import INDICATOR_IDS
    for series in DEFAULT_SERIES:
        assert series in SERIES_INDICATOR_MAP, f"Series {series} missing from map"
        indicator = SERIES_INDICATOR_MAP[series]
        assert indicator in INDICATOR_IDS, f"Indicator '{indicator}' not in INDICATOR_IDS"
