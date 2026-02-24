"""
tests/test_pipelines/test_economic_pulse.py — Unit tests for economic_pulse pipeline.

All external calls (StatCan HTTP, BoC HTTP, Supabase) are mocked.
No network access required.
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from candata_pipeline.pipelines.economic_pulse import (
    STATCAN_TABLES,
    TABLE_ALIASES,
    _fetch_boc,
    _fetch_statcan,
    run,
)
from candata_pipeline.loaders.supabase_loader import LoadResult
from candata_pipeline.transforms.normalize import GeoNormalizer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_geo_lookup() -> dict[str, str]:
    return {
        "01": "uuid-canada-0001",
        "35": "uuid-ontario-035",
        "24": "uuid-quebec-024",
    }


def _make_indicator_df(indicator_id: str = "cpi_monthly") -> pl.DataFrame:
    """Return a minimal indicator_values-shaped DataFrame."""
    return pl.DataFrame(
        {
            "indicator_id": [indicator_id, indicator_id],
            "geography_id": ["uuid-canada-0001", "uuid-ontario-035"],
            "ref_date": [date(2023, 1, 1), date(2023, 1, 1)],
            "value": [157.1, 158.2],
        }
    )


# ---------------------------------------------------------------------------
# TABLE_ALIASES tests
# ---------------------------------------------------------------------------

def test_table_aliases_cover_all_statcan_tables():
    """Every alias should resolve to a key in STATCAN_TABLES."""
    for alias, table_id in TABLE_ALIASES.items():
        assert table_id in STATCAN_TABLES, (
            f"Alias '{alias}' maps to '{table_id}' which is not in STATCAN_TABLES"
        )


def test_table_aliases_expected_names():
    assert "gdp" in TABLE_ALIASES
    assert "cpi" in TABLE_ALIASES
    assert "unemployment" in TABLE_ALIASES
    assert "retail" in TABLE_ALIASES


# ---------------------------------------------------------------------------
# _fetch_statcan tests
# ---------------------------------------------------------------------------

class TestFetchStatcan:
    @pytest.mark.asyncio
    async def test_fetch_statcan_returns_dataframe(self):
        geo_lookup = _make_geo_lookup()
        normalizer = GeoNormalizer()
        normalizer._cache = geo_lookup
        normalizer._loaded = True

        raw_df = pl.DataFrame(
            {
                "REF_DATE": ["2023-01", "2023-01"],
                "GEO": ["Canada", "Ontario"],
                "VALUE": ["157.1", "158.2"],
                "VECTOR": ["v001", "v002"],
                "SCALAR_FACTOR": ["Units", "Units"],
                "UOM": ["2002=100", "2002=100"],
                "STATUS": ["", ""],
            }
        )

        cfg = {"indicator_id": "cpi_monthly", "value_filter": None, "frequency": "monthly"}

        with patch(
            "candata_pipeline.pipelines.economic_pulse.StatCanSource"
        ) as MockSource:
            instance = MockSource.return_value
            instance.extract = AsyncMock(return_value=raw_df)
            instance.transform = MagicMock(return_value=raw_df)

            # Patch transform to return a properly shaped DF
            shaped_df = pl.DataFrame(
                {
                    "ref_date": [date(2023, 1, 1), date(2023, 1, 1)],
                    "geo": ["Canada", "Ontario"],
                    "sgc_code": ["01", "35"],
                    "geo_level": ["country", "pr"],
                    "value": [157.1, 158.2],
                }
            )
            instance.transform = MagicMock(return_value=shaped_df)

            result = await _fetch_statcan("18100004", cfg, normalizer, None)

        assert isinstance(result, pl.DataFrame)
        assert "indicator_id" in result.columns
        assert "geography_id" in result.columns
        assert "ref_date" in result.columns
        assert "value" in result.columns

    @pytest.mark.asyncio
    async def test_fetch_statcan_drops_null_values(self):
        geo_lookup = _make_geo_lookup()
        normalizer = GeoNormalizer()
        normalizer._cache = geo_lookup
        normalizer._loaded = True

        shaped_df = pl.DataFrame(
            {
                "ref_date": [date(2023, 1, 1), date(2023, 1, 1)],
                "geo": ["Canada", "Ontario"],
                "sgc_code": ["01", "35"],
                "geo_level": ["country", "pr"],
                "value": [157.1, None],  # one null value
            }
        )

        cfg = {"indicator_id": "cpi_monthly", "value_filter": None, "frequency": "monthly"}

        with patch(
            "candata_pipeline.pipelines.economic_pulse.StatCanSource"
        ) as MockSource:
            instance = MockSource.return_value
            instance.extract = AsyncMock(return_value=pl.DataFrame())
            instance.transform = MagicMock(return_value=shaped_df)

            result = await _fetch_statcan("18100004", cfg, normalizer, None)

        # Null value row should be dropped
        assert result["value"].null_count() == 0


# ---------------------------------------------------------------------------
# _fetch_boc tests
# ---------------------------------------------------------------------------

class TestFetchBoc:
    @pytest.mark.asyncio
    async def test_fetch_boc_returns_dataframe(self):
        geo_lookup = _make_geo_lookup()

        boc_df = pl.DataFrame(
            {
                "ref_date": [date(2023, 1, 3), date(2023, 1, 4)],
                "series_code": ["FXUSDCAD", "FXUSDCAD"],
                "indicator_id": ["usdcad", "usdcad"],
                "value": [1.3544, 1.3512],
            }
        )

        with patch(
            "candata_pipeline.pipelines.economic_pulse.BankOfCanadaSource"
        ) as MockBoc:
            instance = MockBoc.return_value
            instance.extract = AsyncMock(return_value=boc_df)
            instance.transform = MagicMock(return_value=boc_df)

            result = await _fetch_boc(geo_lookup, None, None)

        assert isinstance(result, pl.DataFrame)
        assert "geography_id" in result.columns
        # All BoC rows should use Canada geography
        assert all(gid == "uuid-canada-0001" for gid in result["geography_id"].to_list())

    @pytest.mark.asyncio
    async def test_fetch_boc_empty_when_canada_missing(self):
        geo_lookup: dict[str, str] = {}  # no Canada entry

        with patch(
            "candata_pipeline.pipelines.economic_pulse.BankOfCanadaSource"
        ) as MockBoc:
            instance = MockBoc.return_value
            instance.extract = AsyncMock(return_value=pl.DataFrame())
            instance.transform = MagicMock(
                return_value=pl.DataFrame(
                    schema={
                        "ref_date": pl.Date,
                        "series_code": pl.String,
                        "indicator_id": pl.String,
                        "value": pl.Float64,
                    }
                )
            )

            result = await _fetch_boc(geo_lookup, None, None)

        assert result.is_empty()


# ---------------------------------------------------------------------------
# run() pipeline tests
# ---------------------------------------------------------------------------

class TestRun:
    @staticmethod
    def _mock_loader(geo_lookup: dict[str, str]) -> MagicMock:
        loader = MagicMock()
        loader.build_geo_lookup = AsyncMock(return_value=geo_lookup)
        loader.start_pipeline_run = AsyncMock(return_value="run-uuid-1234")
        loader.finish_pipeline_run = AsyncMock()
        loader.fail_pipeline_run = AsyncMock()
        loader.upsert = AsyncMock(
            return_value=LoadResult(table="indicator_values", records_loaded=10)
        )
        return loader

    @pytest.mark.asyncio
    async def test_run_dry_run_does_not_upsert(self):
        geo_lookup = _make_geo_lookup()
        loader = self._mock_loader(geo_lookup)

        cpi_df = _make_indicator_df("cpi_monthly")
        gdp_df = _make_indicator_df("gdp_monthly")

        with patch("candata_pipeline.pipelines.economic_pulse.configure_logging"):
            with patch(
                "candata_pipeline.pipelines.economic_pulse.SupabaseLoader",
                return_value=loader,
            ):
                with patch(
                    "candata_pipeline.pipelines.economic_pulse._fetch_statcan",
                    side_effect=[cpi_df, gdp_df, pl.DataFrame()],
                ):
                    with patch(
                        "candata_pipeline.pipelines.economic_pulse._fetch_boc",
                        return_value=_make_indicator_df("usdcad"),
                    ):
                        result = await run(dry_run=True, tables=["cpi", "gdp"])

        loader.upsert.assert_not_called()
        assert result.records_loaded > 0

    @pytest.mark.asyncio
    async def test_run_tables_filter_restricts_statcan(self):
        """When --tables gdp is passed, only the GDP table should be fetched."""
        geo_lookup = _make_geo_lookup()
        loader = self._mock_loader(geo_lookup)

        fetch_calls: list[str] = []

        async def mock_fetch_statcan(pid: str, cfg, normalizer, start_date):
            fetch_calls.append(pid)
            return _make_indicator_df(cfg["indicator_id"])

        with patch("candata_pipeline.pipelines.economic_pulse.configure_logging"):
            with patch(
                "candata_pipeline.pipelines.economic_pulse.SupabaseLoader",
                return_value=loader,
            ):
                with patch(
                    "candata_pipeline.pipelines.economic_pulse._fetch_statcan",
                    side_effect=mock_fetch_statcan,
                ):
                    with patch(
                        "candata_pipeline.pipelines.economic_pulse._fetch_boc",
                        return_value=pl.DataFrame(
                            schema={
                                "indicator_id": pl.String,
                                "geography_id": pl.String,
                                "ref_date": pl.Date,
                                "value": pl.Float64,
                            }
                        ),
                    ):
                        await run(dry_run=True, tables=["gdp"])

        assert fetch_calls == ["36100434"]  # only GDP table

    @pytest.mark.asyncio
    async def test_run_partial_failure_continues(self):
        """If one StatCan table fails, others should still succeed."""
        geo_lookup = _make_geo_lookup()
        loader = self._mock_loader(geo_lookup)

        call_count = 0

        async def mock_fetch_statcan(pid: str, cfg, normalizer, start_date):
            nonlocal call_count
            call_count += 1
            if pid == "36100434":  # GDP fails
                raise RuntimeError("GDP fetch failed")
            return _make_indicator_df(cfg["indicator_id"])

        with patch("candata_pipeline.pipelines.economic_pulse.configure_logging"):
            with patch(
                "candata_pipeline.pipelines.economic_pulse.SupabaseLoader",
                return_value=loader,
            ):
                with patch(
                    "candata_pipeline.pipelines.economic_pulse._fetch_statcan",
                    side_effect=mock_fetch_statcan,
                ):
                    with patch(
                        "candata_pipeline.pipelines.economic_pulse._fetch_boc",
                        return_value=_make_indicator_df("usdcad"),
                    ):
                        # Should not raise despite GDP failure
                        result = await run(dry_run=True)

        # All statcan tasks were attempted (4 tables)
        assert call_count == 4
        # Some data was still loaded (non-failed tables)
        assert result.records_loaded > 0

    @pytest.mark.asyncio
    async def test_run_upserts_to_indicator_values(self):
        geo_lookup = _make_geo_lookup()
        loader = self._mock_loader(geo_lookup)

        with patch("candata_pipeline.pipelines.economic_pulse.configure_logging"):
            with patch(
                "candata_pipeline.pipelines.economic_pulse.SupabaseLoader",
                return_value=loader,
            ):
                with patch(
                    "candata_pipeline.pipelines.economic_pulse._fetch_statcan",
                    return_value=_make_indicator_df("cpi_monthly"),
                ):
                    with patch(
                        "candata_pipeline.pipelines.economic_pulse._fetch_boc",
                        return_value=_make_indicator_df("usdcad"),
                    ):
                        result = await run(dry_run=False, tables=["cpi"])

        loader.upsert.assert_called_once()
        call_args = loader.upsert.call_args
        # table is passed as first positional argument
        assert call_args.args[0] == "indicator_values"
