"""
tests/test_pipelines/test_housing.py — Unit tests for the housing pipeline.

All external calls (CMHC HTTP, Supabase) are mocked. No network access required.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from candata_pipeline.loaders.supabase_loader import LoadResult
from candata_pipeline.pipelines.housing import (
    _build_indicator_values,
    _fetch_dataset,
    _prepare_rents_df,
    _prepare_starts_df,
    _prepare_vacancy_df,
    _resolve_cma_filter,
    run,
)
from candata_pipeline.sources.cmhc import CMHC_GEO_TO_SGC, CMHCSource
from candata_pipeline.transforms.normalize import GeoNormalizer

# Shared patch for CMHCSource.extract_cmhc_api so run() doesn't make real HTTP calls
_mock_extract_cmhc_api = patch(
    "candata_pipeline.sources.cmhc.CMHCSource.extract_cmhc_api",
    new_callable=AsyncMock,
    return_value=(pl.DataFrame(), 0),
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_geo_lookup() -> dict[str, str]:
    return {
        "535": "uuid-toronto-535",
        "933": "uuid-vancouver-933",
        "825": "uuid-calgary-825",
        "462": "uuid-montreal-462",
        "505": "uuid-ottawa-505",
    }


def _make_vacancy_df() -> pl.DataFrame:
    return pl.DataFrame({
        "sgc_code": ["535", "535", "535", "535", "535"],
        "ref_date": [date(2023, 10, 1)] * 5,
        "bedroom_type": ["bachelor", "1br", "2br", "3br+", "total"],
        "vacancy_rate": [1.5, 1.8, 1.3, 0.9, 1.6],
        "universe": [45230, 189450, 134820, 23410, 392910],
    })


def _make_rents_df() -> pl.DataFrame:
    return pl.DataFrame({
        "sgc_code": ["933", "933", "933", "933", "933"],
        "ref_date": [date(2023, 10, 1)] * 5,
        "bedroom_type": ["bachelor", "1br", "2br", "3br+", "total"],
        "average_rent": [1450.0, 1750.0, 2200.0, 2800.0, 1950.0],
    })


def _make_starts_df() -> pl.DataFrame:
    return pl.DataFrame({
        "sgc_code": ["825", "825", "825", "825", "825"],
        "ref_date": [date(2023, 10, 1)] * 5,
        "dwelling_type": ["single", "semi", "row", "apartment", "total"],
        "units": [1250, 320, 780, 4500, 6850],
    })


def _make_normalizer(geo_lookup: dict[str, str]) -> GeoNormalizer:
    normalizer = GeoNormalizer()
    normalizer._cache = geo_lookup
    normalizer._loaded = True
    return normalizer


def _make_loader(geo_lookup: dict[str, str]) -> MagicMock:
    loader = MagicMock()
    loader.build_geo_lookup = AsyncMock(return_value=geo_lookup)
    loader.start_pipeline_run = AsyncMock(return_value="run-uuid-housing")
    loader.finish_pipeline_run = AsyncMock()
    loader.fail_pipeline_run = AsyncMock()
    loader.upsert = AsyncMock(
        return_value=LoadResult(table="test", records_loaded=5)
    )
    return loader


# ---------------------------------------------------------------------------
# _resolve_cma_filter
# ---------------------------------------------------------------------------

class TestResolveCmaFilter:
    def test_resolve_by_name(self):
        result = _resolve_cma_filter(["Toronto", "Vancouver"])
        assert 2270 in result  # Toronto
        assert 2410 in result  # Vancouver

    def test_resolve_by_alias(self):
        result = _resolve_cma_filter(["montreal", "ottawa"])
        assert 2480 in result  # Montreal
        assert 1680 in result  # Ottawa

    def test_resolve_by_numeric_id(self):
        result = _resolve_cma_filter(["2270", "2410"])
        assert 2270 in result
        assert 2410 in result

    def test_unknown_cma_skipped(self):
        result = _resolve_cma_filter(["toronto", "atlantis"])
        assert len(result) == 1
        assert 2270 in result

    def test_empty_input(self):
        result = _resolve_cma_filter([])
        assert result == []


# ---------------------------------------------------------------------------
# _prepare_*_df helpers
# ---------------------------------------------------------------------------

class TestPrepareDf:
    def test_prepare_vacancy_df_selects_columns(self):
        df = _make_vacancy_df()
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        # Add geography_id
        df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
        df = df.with_columns(pl.lit("test-uuid").alias("id"))

        result = _prepare_vacancy_df(df)
        assert "id" in result.columns
        assert "geography_id" in result.columns
        assert "ref_date" in result.columns
        assert "bedroom_type" in result.columns
        assert "vacancy_rate" in result.columns

    def test_prepare_rents_df_selects_columns(self):
        df = _make_rents_df()
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
        df = df.with_columns(pl.lit("test-uuid").alias("id"))

        result = _prepare_rents_df(df)
        assert "average_rent" in result.columns
        assert "bedroom_type" in result.columns

    def test_prepare_starts_df_selects_columns(self):
        df = _make_starts_df()
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
        df = df.with_columns(pl.lit("test-uuid").alias("id"))

        result = _prepare_starts_df(df)
        assert "dwelling_type" in result.columns
        assert "units" in result.columns

    def test_prepare_empty_returns_empty(self):
        assert _prepare_vacancy_df(pl.DataFrame()).is_empty()
        assert _prepare_rents_df(pl.DataFrame()).is_empty()
        assert _prepare_starts_df(pl.DataFrame()).is_empty()


# ---------------------------------------------------------------------------
# _build_indicator_values
# ---------------------------------------------------------------------------

class TestBuildIndicatorValues:
    def test_builds_vacancy_rate_indicator(self):
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        vacancy = _make_vacancy_df()
        vacancy = normalizer.add_geography_id(vacancy, sgc_code_col="sgc_code")
        vacancy = vacancy.with_columns(pl.lit("id").alias("id"))

        result = _build_indicator_values(vacancy, pl.DataFrame(), pl.DataFrame())

        assert not result.is_empty()
        assert "indicator_id" in result.columns
        vr_rows = result.filter(pl.col("indicator_id") == "vacancy_rate")
        assert len(vr_rows) > 0
        # Should use the "total" bedroom type
        assert vr_rows["value"][0] == 1.6

    def test_builds_average_rent_indicator(self):
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        rents = _make_rents_df()
        rents = normalizer.add_geography_id(rents, sgc_code_col="sgc_code")
        rents = rents.with_columns(pl.lit("id").alias("id"))

        result = _build_indicator_values(pl.DataFrame(), rents, pl.DataFrame())

        ar_rows = result.filter(pl.col("indicator_id") == "average_rent")
        assert len(ar_rows) > 0
        # Should use the "2br" bedroom type
        assert ar_rows["value"][0] == 2200.0

    def test_builds_housing_starts_indicator(self):
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        starts = _make_starts_df()
        starts = normalizer.add_geography_id(starts, sgc_code_col="sgc_code")
        starts = starts.with_columns(pl.lit("id").alias("id"))

        result = _build_indicator_values(pl.DataFrame(), pl.DataFrame(), starts)

        hs_rows = result.filter(pl.col("indicator_id") == "housing_starts")
        assert len(hs_rows) > 0
        assert hs_rows["value"][0] == 6850.0

    def test_all_empty_returns_empty_schema(self):
        result = _build_indicator_values(pl.DataFrame(), pl.DataFrame(), pl.DataFrame())
        assert result.is_empty()
        assert "indicator_id" in result.columns
        assert "value" in result.columns


# ---------------------------------------------------------------------------
# _fetch_dataset
# ---------------------------------------------------------------------------

class TestFetchDataset:
    @pytest.mark.asyncio
    async def test_fetch_dataset_adds_geography_id(self):
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        vacancy = _make_vacancy_df()

        source = MagicMock(spec=CMHCSource)
        source.extract = AsyncMock(return_value=vacancy)

        result = await _fetch_dataset(
            source, "vacancy_rates", normalizer, [2270], None, dry_run=False
        )

        assert not result.is_empty()
        assert "geography_id" in result.columns
        assert "id" in result.columns

    @pytest.mark.asyncio
    async def test_fetch_dataset_dry_run_uses_sgc_as_geo_id(self):
        normalizer = _make_normalizer({})

        vacancy = _make_vacancy_df()

        source = MagicMock(spec=CMHCSource)
        source.extract = AsyncMock(return_value=vacancy)

        result = await _fetch_dataset(
            source, "vacancy_rates", normalizer, [2270], None, dry_run=True
        )

        assert not result.is_empty()
        assert "geography_id" in result.columns
        # In dry-run, geography_id == sgc_code
        assert result["geography_id"][0] == "535"

    @pytest.mark.asyncio
    async def test_fetch_dataset_empty_returns_empty(self):
        geo_lookup = _make_geo_lookup()
        normalizer = _make_normalizer(geo_lookup)

        source = MagicMock(spec=CMHCSource)
        source.extract = AsyncMock(return_value=pl.DataFrame())

        result = await _fetch_dataset(
            source, "vacancy_rates", normalizer, [2270], None
        )
        assert result.is_empty()


# ---------------------------------------------------------------------------
# run() pipeline tests
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.mark.asyncio
    async def test_run_dry_run_does_not_upsert(self):
        geo_lookup = _make_geo_lookup()
        loader = _make_loader(geo_lookup)
        normalizer = _make_normalizer(geo_lookup)

        # Simulate what _fetch_dataset returns (with geography_id and id columns)
        vacancy = _make_vacancy_df()
        vacancy = normalizer.add_geography_id(vacancy, sgc_code_col="sgc_code")
        vacancy = vacancy.with_columns(pl.lit("vid").alias("id"))

        rents = _make_rents_df()
        rents = normalizer.add_geography_id(rents, sgc_code_col="sgc_code")
        rents = rents.with_columns(pl.lit("rid").alias("id"))

        starts = _make_starts_df()
        starts = normalizer.add_geography_id(starts, sgc_code_col="sgc_code")
        starts = starts.with_columns(pl.lit("sid").alias("id"))

        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            with _mock_extract_cmhc_api:
                with patch(
                    "candata_pipeline.pipelines.housing.SupabaseLoader",
                    return_value=loader,
                ):
                    with patch(
                        "candata_pipeline.pipelines.housing._fetch_dataset",
                        side_effect=[vacancy, rents, starts],
                    ):
                        results = await run(dry_run=True)

        loader.upsert.assert_not_called()
        # dry_run should still report record counts
        total = sum(r.records_loaded for r in results.values())
        assert total > 0

    @pytest.mark.asyncio
    async def test_run_upserts_to_five_tables(self):
        geo_lookup = _make_geo_lookup()
        loader = _make_loader(geo_lookup)

        vacancy = _make_vacancy_df()
        rents = _make_rents_df()
        starts = _make_starts_df()

        # Add geography_id for indicator value building
        normalizer = _make_normalizer(geo_lookup)
        vacancy = normalizer.add_geography_id(vacancy, sgc_code_col="sgc_code")
        vacancy = vacancy.with_columns(pl.lit("id").alias("id"))
        rents = normalizer.add_geography_id(rents, sgc_code_col="sgc_code")
        rents = rents.with_columns(pl.lit("id").alias("id"))
        starts = normalizer.add_geography_id(starts, sgc_code_col="sgc_code")
        starts = starts.with_columns(pl.lit("id").alias("id"))

        cmhc_api_df = pl.DataFrame({
            "cma_name": ["Toronto"],
            "cma_geoid": ["535"],
            "year": [2024],
            "month": [1],
            "dwelling_type": ["Total"],
            "data_type": ["Starts"],
            "intended_market": ["Total"],
            "value": [100],
        }).with_columns([
            pl.col("year").cast(pl.Int32),
            pl.col("month").cast(pl.Int32),
            pl.col("value").cast(pl.Int64),
        ])

        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            with patch(
                "candata_pipeline.sources.cmhc.CMHCSource.extract_cmhc_api",
                new_callable=AsyncMock,
                return_value=(cmhc_api_df, 0),
            ):
                with patch(
                    "candata_pipeline.pipelines.housing.SupabaseLoader",
                    return_value=loader,
                ):
                    with patch(
                        "candata_pipeline.pipelines.housing._fetch_dataset",
                        side_effect=[vacancy, rents, starts],
                    ):
                        results = await run(dry_run=False)

        # Should have upserted to all five tables
        assert "vacancy_rates" in results
        assert "average_rents" in results
        assert "housing_starts" in results
        assert "indicator_values" in results
        assert "cmhc_housing" in results

        # loader.upsert should have been called for each table with data
        assert loader.upsert.call_count >= 4

    @pytest.mark.asyncio
    async def test_run_with_cmas_filter(self):
        geo_lookup = _make_geo_lookup()
        loader = _make_loader(geo_lookup)
        normalizer = _make_normalizer(geo_lookup)

        # Only Toronto data, with geography_id and id columns
        vacancy = _make_vacancy_df()
        vacancy = normalizer.add_geography_id(vacancy, sgc_code_col="sgc_code")
        vacancy = vacancy.with_columns(pl.lit("vid").alias("id"))

        fetch_calls: list[tuple] = []

        async def mock_fetch(source, dataset, norm, geo_ids, start_date, *, dry_run=False):
            fetch_calls.append((dataset, geo_ids))
            if dataset == "vacancy_rates":
                return vacancy
            return pl.DataFrame()

        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            with _mock_extract_cmhc_api:
                with patch(
                    "candata_pipeline.pipelines.housing.SupabaseLoader",
                    return_value=loader,
                ):
                    with patch(
                        "candata_pipeline.pipelines.housing._fetch_dataset",
                        side_effect=mock_fetch,
                    ):
                        results = await run(cmas=["toronto"], dry_run=True)

        # _fetch_dataset should have been called with the resolved geo IDs
        assert all(geo_ids == [2270] for _, geo_ids in fetch_calls)

    @pytest.mark.asyncio
    async def test_run_all_empty_data(self):
        geo_lookup = _make_geo_lookup()
        loader = _make_loader(geo_lookup)

        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            with _mock_extract_cmhc_api:
                with patch(
                    "candata_pipeline.pipelines.housing.SupabaseLoader",
                    return_value=loader,
                ):
                    with patch(
                        "candata_pipeline.pipelines.housing._fetch_dataset",
                        return_value=pl.DataFrame(),
                    ):
                        results = await run(dry_run=True)

        # Should still return a dict even with no data
        assert isinstance(results, dict)
        total = sum(r.records_loaded for r in results.values())
        assert total == 0

    @pytest.mark.asyncio
    async def test_run_invalid_cmas_returns_empty(self):
        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            results = await run(cmas=["atlantis", "mordor"], dry_run=True)

        assert results == {}

    @pytest.mark.asyncio
    async def test_run_records_pipeline_run(self):
        """Non-dry-run should record a pipeline_run in Supabase."""
        geo_lookup = _make_geo_lookup()
        loader = _make_loader(geo_lookup)

        with patch("candata_pipeline.pipelines.housing.configure_logging"):
            with _mock_extract_cmhc_api:
                with patch(
                    "candata_pipeline.pipelines.housing.SupabaseLoader",
                    return_value=loader,
                ):
                    with patch(
                        "candata_pipeline.pipelines.housing._fetch_dataset",
                        return_value=pl.DataFrame(),
                    ):
                        await run(dry_run=False)

        loader.start_pipeline_run.assert_called_once()
        loader.finish_pipeline_run.assert_called_once()
