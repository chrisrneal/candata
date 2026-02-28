"""
tests/test_pipelines/test_trade.py — Unit tests for the trade pipeline.

Tests cover:
  - Dry run (no Supabase writes)
  - Date filtering passthrough
  - Empty data handling
  - Deduplication by composite key
  - Pipeline run tracking (start/finish/fail)
  - Error handling
  - Batch upsert with correct conflict columns
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from candata_pipeline.loaders.supabase_loader import LoadResult
from candata_pipeline.pipelines.trade import (
    CONFLICT_COLUMNS,
    _load_bilateral_trade,
    _load_commodity_trade,
    run,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sample_commodity_df() -> pl.DataFrame:
    """Pre-transformed commodity trade data matching trade_flows schema."""
    return pl.DataFrame({
        "direction": ["import", "export", "import", "export", "import"],
        "hs_code": ["0201", "0201", "2709", "2709", "0201"],
        "hs_description": [
            "Meat of bovine animals",
            "Meat of bovine animals",
            "Petroleum oils crude",
            "Petroleum oils crude",
            "Meat of bovine animals",
        ],
        "partner_country": ["WLD", "WLD", "WLD", "WLD", "WLD"],
        "province": ["01", "01", "01", "01", "35"],
        "ref_date": [
            date(2024, 1, 1),
            date(2024, 1, 1),
            date(2024, 1, 1),
            date(2024, 1, 1),
            date(2024, 1, 1),
        ],
        "value_cad": [5_000_000.0, 8_000_000.0, 12_000_000.0, 25_000_000.0, 2_000_000.0],
        "volume": [None, None, None, None, None],
        "volume_unit": [None, None, None, None, None],
    })


def _sample_bilateral_df() -> pl.DataFrame:
    return pl.DataFrame({
        "direction": ["import", "export", "import"],
        "hs_code": ["0201", "0201", "2709"],
        "hs_description": [
            "Meat of bovine animals",
            "Meat of bovine animals",
            "Petroleum oils crude",
        ],
        "partner_country": ["USA", "USA", "CHN"],
        "province": ["01", "01", "01"],
        "ref_date": [date(2024, 1, 1), date(2024, 1, 1), date(2024, 1, 1)],
        "value_cad": [3_000_000.0, 6_000_000.0, 1_000_000.0],
        "volume": [None, None, None],
        "volume_unit": [None, None, None],
    })


def _mock_source(commodity_df=None, bilateral_df=None):
    """Create a mock TradeSource."""
    source = MagicMock()

    async def mock_extract(*, table_pid="12100011", **kwargs):
        if table_pid == "12100011":
            return commodity_df if commodity_df is not None else pl.DataFrame()
        return bilateral_df if bilateral_df is not None else pl.DataFrame()

    source.extract = mock_extract
    source.transform = MagicMock(
        side_effect=lambda raw, **kw: commodity_df if commodity_df is not None else pl.DataFrame()
    )
    source.transform_bilateral = MagicMock(
        side_effect=lambda raw, **kw: bilateral_df if bilateral_df is not None else pl.DataFrame()
    )
    return source


# ---------------------------------------------------------------------------
# _load_commodity_trade
# ---------------------------------------------------------------------------

class TestLoadCommodityTrade:
    @pytest.mark.asyncio
    async def test_dry_run_returns_count(self):
        source = _mock_source(commodity_df=_sample_commodity_df())
        loader = MagicMock()
        loader.upsert = AsyncMock()

        result = await _load_commodity_trade(source, loader, None, None, dry_run=True)

        assert result.records_loaded == 5
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_upsert_called_with_conflict_columns(self):
        source = _mock_source(commodity_df=_sample_commodity_df())
        loader = MagicMock()
        loader.upsert = AsyncMock(
            return_value=LoadResult(table="trade_flows", records_loaded=5)
        )

        result = await _load_commodity_trade(source, loader, None, None, dry_run=False)

        assert result.records_loaded == 5
        loader.upsert.assert_called_once()
        call_kwargs = loader.upsert.call_args
        assert call_kwargs[0][0] == "trade_flows"
        assert call_kwargs[1]["conflict_columns"] == CONFLICT_COLUMNS

    @pytest.mark.asyncio
    async def test_empty_data_returns_zero(self):
        source = _mock_source(commodity_df=pl.DataFrame())
        loader = MagicMock()

        result = await _load_commodity_trade(source, loader, None, None, dry_run=False)

        assert result.records_loaded == 0

    @pytest.mark.asyncio
    async def test_deduplicates_by_composite_key(self):
        # Create duplicate rows
        df = pl.DataFrame({
            "direction": ["import", "import"],
            "hs_code": ["0201", "0201"],
            "hs_description": ["Meat v1", "Meat v2"],
            "partner_country": ["WLD", "WLD"],
            "province": ["01", "01"],
            "ref_date": [date(2024, 1, 1), date(2024, 1, 1)],
            "value_cad": [5_000_000.0, 5_500_000.0],
            "volume": [None, None],
            "volume_unit": [None, None],
        })
        source = _mock_source(commodity_df=df)
        loader = MagicMock()

        result = await _load_commodity_trade(source, loader, None, None, dry_run=True)

        # Deduplicated to 1 row
        assert result.records_loaded == 1


# ---------------------------------------------------------------------------
# _load_bilateral_trade
# ---------------------------------------------------------------------------

class TestLoadBilateralTrade:
    @pytest.mark.asyncio
    async def test_dry_run(self):
        source = _mock_source(bilateral_df=_sample_bilateral_df())
        loader = MagicMock()
        loader.upsert = AsyncMock()

        result = await _load_bilateral_trade(source, loader, None, None, dry_run=True)

        assert result.records_loaded == 3
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_bilateral(self):
        source = _mock_source(bilateral_df=pl.DataFrame())
        loader = MagicMock()

        result = await _load_bilateral_trade(source, loader, None, None, dry_run=False)

        assert result.records_loaded == 0


# ---------------------------------------------------------------------------
# Full pipeline run()
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.mark.asyncio
    async def test_dry_run_no_upserts(self):
        with (
            patch("candata_pipeline.pipelines.trade.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.trade.TradeSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock()

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_commodity_df())
            source.transform = MagicMock(return_value=_sample_commodity_df())
            source.transform_bilateral = MagicMock(return_value=_sample_bilateral_df())

            result = await run(dry_run=True)

        assert result.records_loaded == 8  # 5 commodity + 3 bilateral
        loader.start_pipeline_run.assert_called_once()
        loader.finish_pipeline_run.assert_called_once()
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_passes_dates(self):
        with (
            patch("candata_pipeline.pipelines.trade.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.trade.TradeSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock(
                return_value=LoadResult(table="trade_flows", records_loaded=5)
            )

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_commodity_df())
            source.transform = MagicMock(return_value=_sample_commodity_df())
            source.transform_bilateral = MagicMock(return_value=_sample_bilateral_df())

            sd = date(2024, 1, 1)
            ed = date(2024, 12, 31)
            await run(start_date=sd, end_date=ed)

        # Verify dates were passed to transforms
        source.transform.assert_called_once()
        call_kwargs = source.transform.call_args[1]
        assert call_kwargs["start_date"] == sd
        assert call_kwargs["end_date"] == ed

    @pytest.mark.asyncio
    async def test_run_records_failure(self):
        with (
            patch("candata_pipeline.pipelines.trade.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.trade.TradeSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.fail_pipeline_run = AsyncMock()

            source = MockSource.return_value
            source.extract = AsyncMock(side_effect=RuntimeError("StatCan down"))

            with pytest.raises(RuntimeError, match="StatCan down"):
                await run()

            loader.fail_pipeline_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_combined_result_sums_both_tables(self):
        with (
            patch("candata_pipeline.pipelines.trade.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.trade.TradeSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock(
                side_effect=[
                    LoadResult(table="trade_flows", records_loaded=100),
                    LoadResult(table="trade_flows", records_loaded=50),
                ]
            )

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_commodity_df())
            source.transform = MagicMock(return_value=_sample_commodity_df())
            source.transform_bilateral = MagicMock(return_value=_sample_bilateral_df())

            result = await run()

        assert result.records_loaded == 150
        assert result.table == "trade_flows"
