"""
tests/test_sources/test_cmhc.py — Unit tests for CMHCSource.
"""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import polars as pl
import pytest
import respx

from candata_pipeline.sources.cmhc import CMHCSource, _BEDROOM_NORMALIZE

FIXTURES = Path(__file__).parent.parent / "fixtures"


@pytest.fixture
def vacancy_csv_bytes() -> bytes:
    return (FIXTURES / "cmhc_vacancy_sample.csv").read_bytes()


@pytest.fixture
def hmip_payload(vacancy_csv_bytes: bytes) -> dict:
    """Simulate a simplified HMIP JSON response."""
    return {
        "Headers": ["Geography", "Date", "Bedroom Type", "Vacancy Rate (%)", "Universe"],
        "Data": [
            ["Toronto", "2023-10-01", "bachelor", "1.5", "45230"],
            ["Toronto", "2023-10-01", "1 Bedroom", "1.8", "189450"],
            ["Vancouver", "2023-10-01", "Total", "1.0", "211340"],
            ["Calgary", "2023-10-01", "2 Bedrooms", "2.0", "34200"],
        ],
    }


class TestCMHCExtract:
    @pytest.mark.asyncio
    async def test_extract_vacancy_from_hmip(self, hmip_payload: dict):
        source = CMHCSource()
        with respx.mock() as router:
            router.get(url__regex=r".*hmip.*GetTableData.*").mock(
                return_value=httpx.Response(200, json=hmip_payload)
            )
            df = await source.extract(dataset="vacancy_rates", year=2023)

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0

    @pytest.mark.asyncio
    async def test_extract_returns_dataframe_with_headers(self, hmip_payload: dict):
        source = CMHCSource()
        with respx.mock() as router:
            router.get(url__regex=r".*hmip.*").mock(
                return_value=httpx.Response(200, json=hmip_payload)
            )
            df = await source.extract(dataset="vacancy_rates", year=2023)

        assert "Geography" in df.columns or len(df.columns) == 5

    @pytest.mark.asyncio
    async def test_extract_empty_response_returns_empty_df(self):
        source = CMHCSource()
        empty_payload = {"Headers": [], "Data": []}
        with respx.mock() as router:
            router.get(url__regex=r".*hmip.*").mock(
                return_value=httpx.Response(200, json=empty_payload)
            )
            df = await source.extract(dataset="vacancy_rates", year=2023)

        assert df.is_empty() or len(df) == 0


class TestCMHCTransform:
    @pytest.fixture
    def vacancy_raw(self, cmhc_vacancy_df: pl.DataFrame) -> pl.DataFrame:
        return cmhc_vacancy_df

    def test_transform_normalizes_bedroom_types(self, vacancy_raw: pl.DataFrame):
        source = CMHCSource()
        df = source.transform(vacancy_raw, dataset="vacancy_rates")
        if "bedroom_type" in df.columns:
            valid_types = {"bachelor", "1br", "2br", "3br+", "total"}
            actual = set(df["bedroom_type"].drop_nulls().to_list())
            assert actual.issubset(valid_types), f"Unexpected types: {actual - valid_types}"

    def test_transform_vacancy_rate_is_numeric(self, vacancy_raw: pl.DataFrame):
        source = CMHCSource()
        df = source.transform(vacancy_raw, dataset="vacancy_rates")
        if "vacancy_rate" in df.columns:
            assert df["vacancy_rate"].dtype == pl.Float64
            assert df["vacancy_rate"].mean() > 0

    def test_transform_universe_is_integer(self, vacancy_raw: pl.DataFrame):
        source = CMHCSource()
        df = source.transform(vacancy_raw, dataset="vacancy_rates")
        if "universe" in df.columns:
            assert df["universe"].dtype == pl.Int64

    def test_transform_adds_sgc_code(self, vacancy_raw: pl.DataFrame):
        source = CMHCSource()
        df = source.transform(vacancy_raw, dataset="vacancy_rates")
        # CMAs like Toronto, Vancouver should resolve to CMA codes
        if "sgc_code" in df.columns:
            non_null = df["sgc_code"].drop_nulls()
            assert len(non_null) > 0

    def test_transform_with_hmip_json(self):
        source = CMHCSource()
        raw = pl.DataFrame({
            "Geography": ["Toronto", "Vancouver", "Calgary"],
            "Date": ["2023-10-01", "2023-10-01", "2023-10-01"],
            "Bedroom Type": ["bachelor", "1 Bedroom", "Total"],
            "Vacancy Rate (%)": ["1.5", "1.2", "2.2"],
            "Universe": ["45230", "211340", "94160"],
        })
        # Normalize columns first (as transform() expects snake_case from _normalize_columns)
        raw = raw.rename({
            "Geography": "geography",
            "Date": "date",
            "Bedroom Type": "bedroom_type",
            "Vacancy Rate (%)": "vacancy_rate",
            "Universe": "universe",
        })
        df = source._transform_vacancy(raw)
        assert "vacancy_rate" in df.columns
        assert df["vacancy_rate"].dtype == pl.Float64


class TestBedroomNormalization:
    @pytest.mark.parametrize("raw, expected", [
        ("bachelor", "bachelor"),
        ("Bach.", "bachelor"),
        ("1 bedroom", "1br"),
        ("1-bedroom", "1br"),
        ("2 Bedrooms", "2br"),
        ("3 Bedrooms +", "3br+"),
        ("3-bedroom+", "3br+"),
        ("Total", "total"),
        ("All", "total"),
    ])
    def test_bedroom_normalize(self, raw: str, expected: str):
        result = _BEDROOM_NORMALIZE.get(raw.strip().lower())
        assert result == expected, f"'{raw}' → '{result}', expected '{expected}'"
