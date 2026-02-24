"""
tests/test_pipelines/test_procurement.py — Unit tests for the procurement pipeline.

Tests cover:
  - Dry run (no Supabase writes)
  - Fiscal year filtering
  - Empty data handling
  - Deduplication of contracts
  - Pipeline run tracking
  - Error handling
"""

from __future__ import annotations

from datetime import date
from unittest.mock import AsyncMock, MagicMock, patch

import polars as pl
import pytest

from candata_pipeline.loaders.supabase_loader import LoadResult
from candata_pipeline.pipelines.procurement import (
    _load_contracts,
    _load_tenders,
    _parse_fiscal_year,
    run,
)


# ---------------------------------------------------------------------------
# Fiscal year parsing
# ---------------------------------------------------------------------------

class TestParseFiscalYear:
    def test_valid_fiscal_year(self):
        start, end = _parse_fiscal_year("2024-2025")
        assert start == date(2024, 4, 1)
        assert end == date(2025, 3, 31)

    def test_older_fiscal_year(self):
        start, end = _parse_fiscal_year("2020-2021")
        assert start == date(2020, 4, 1)
        assert end == date(2021, 3, 31)

    def test_invalid_format_raises(self):
        with pytest.raises(ValueError, match="Invalid fiscal year format"):
            _parse_fiscal_year("2024")

    def test_non_consecutive_years_raises(self):
        with pytest.raises(ValueError, match="end year must be start year"):
            _parse_fiscal_year("2024-2026")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_source(contracts_df: pl.DataFrame | None = None, tenders_df: pl.DataFrame | None = None):
    """Create a mock ProcurementSource."""
    source = MagicMock()

    async def mock_extract(*, dataset="contracts", **kwargs):
        if dataset == "contracts":
            return contracts_df if contracts_df is not None else pl.DataFrame()
        return tenders_df if tenders_df is not None else pl.DataFrame()

    source.extract = mock_extract
    source.transform = MagicMock(side_effect=lambda raw, *, dataset="contracts": raw)
    return source


def _sample_contracts_df() -> pl.DataFrame:
    return pl.DataFrame({
        "contract_number": ["C-001", "C-002", "C-003", "C-004"],
        "vendor_name": ["Acme Inc.", "Deloitte Llp", "Ibm Canada Ltd", "Kpmg Consulting"],
        "department": ["National Defence", "Health Canada", "Public Services and Procurement Canada", "Transport Canada"],
        "description": ["Consulting", "Audit", "Cloud hosting", "Vehicle maint."],
        "contract_value": [100000.0, 500000.0, 2000000.0, 30000.0],
        "award_date": [date(2024, 6, 15), date(2024, 8, 1), date(2024, 11, 20), date(2023, 2, 10)],
        "start_date": [date(2024, 7, 1), date(2024, 9, 1), date(2025, 1, 1), date(2023, 3, 1)],
        "end_date": [date(2025, 3, 31), date(2025, 6, 30), date(2026, 12, 31), date(2023, 9, 30)],
        "raw_data": ['{"a":1}', '{"b":2}', '{"c":3}', '{"d":4}'],
    })


def _sample_tenders_df() -> pl.DataFrame:
    return pl.DataFrame({
        "tender_number": ["T-001", "T-002"],
        "title": ["Cloud services", "Network equipment"],
        "department": ["National Defence", "Health Canada"],
        "status": ["active", "active"],
        "closing_date": [date(2025, 6, 30), date(2025, 7, 15)],
    })


# ---------------------------------------------------------------------------
# _load_contracts
# ---------------------------------------------------------------------------

class TestLoadContracts:
    @pytest.mark.asyncio
    async def test_dry_run_returns_count_without_upsert(self):
        source = _mock_source(contracts_df=_sample_contracts_df())
        loader = MagicMock()
        loader.upsert = AsyncMock()

        result = await _load_contracts(source, loader, fiscal_year=None, dry_run=True)

        assert result.records_loaded == 4
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_fiscal_year_filters_dates(self):
        source = _mock_source(contracts_df=_sample_contracts_df())
        loader = MagicMock()
        loader.upsert = AsyncMock(
            return_value=LoadResult(table="contracts", records_loaded=3)
        )

        # FY 2024-2025: April 2024 – March 2025 → should include C-001, C-002, C-003
        result = await _load_contracts(source, loader, fiscal_year="2024-2025", dry_run=True)

        assert result.records_loaded == 3  # C-004 (2023-02-10) excluded

    @pytest.mark.asyncio
    async def test_fiscal_year_excludes_outside_range(self):
        source = _mock_source(contracts_df=_sample_contracts_df())
        loader = MagicMock()

        # FY 2022-2023: April 2022 – March 2023 → only C-004 (2023-02-10)
        result = await _load_contracts(source, loader, fiscal_year="2022-2023", dry_run=True)

        assert result.records_loaded == 1

    @pytest.mark.asyncio
    async def test_empty_data_returns_empty_result(self):
        source = _mock_source(contracts_df=pl.DataFrame())
        loader = MagicMock()

        result = await _load_contracts(source, loader, fiscal_year=None, dry_run=False)

        assert result.records_loaded == 0

    @pytest.mark.asyncio
    async def test_deduplicates_by_contract_number(self):
        # Two rows with same contract_number — keep last
        df = pl.DataFrame({
            "contract_number": ["C-001", "C-001"],
            "vendor_name": ["Acme V1", "Acme V2"],
            "department": ["DND", "DND"],
            "award_date": [date(2024, 1, 1), date(2024, 6, 1)],
            "raw_data": ['{}', '{}'],
        })
        source = _mock_source(contracts_df=df)
        loader = MagicMock()

        result = await _load_contracts(source, loader, fiscal_year=None, dry_run=True)

        assert result.records_loaded == 1

    @pytest.mark.asyncio
    async def test_drops_rows_missing_vendor(self):
        df = pl.DataFrame({
            "contract_number": ["C-001", "C-002"],
            "vendor_name": ["Acme", None],
            "department": ["DND", "DND"],
            "raw_data": ['{}', '{}'],
        })
        source = _mock_source(contracts_df=df)
        loader = MagicMock()

        result = await _load_contracts(source, loader, fiscal_year=None, dry_run=True)

        assert result.records_loaded == 1

    @pytest.mark.asyncio
    async def test_drops_rows_missing_department(self):
        df = pl.DataFrame({
            "contract_number": ["C-001", "C-002"],
            "vendor_name": ["Acme", "Deloitte"],
            "department": ["DND", ""],
            "raw_data": ['{}', '{}'],
        })
        source = _mock_source(contracts_df=df)
        loader = MagicMock()

        result = await _load_contracts(source, loader, fiscal_year=None, dry_run=True)

        assert result.records_loaded == 1


# ---------------------------------------------------------------------------
# _load_tenders
# ---------------------------------------------------------------------------

class TestLoadTenders:
    @pytest.mark.asyncio
    async def test_dry_run(self):
        source = _mock_source(tenders_df=_sample_tenders_df())
        loader = MagicMock()
        loader.upsert = AsyncMock()

        result = await _load_tenders(source, loader, dry_run=True)

        assert result.records_loaded == 2
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_tenders(self):
        source = _mock_source(tenders_df=pl.DataFrame())
        loader = MagicMock()

        result = await _load_tenders(source, loader, dry_run=False)

        assert result.records_loaded == 0


# ---------------------------------------------------------------------------
# Full pipeline run()
# ---------------------------------------------------------------------------

class TestRun:
    @pytest.mark.asyncio
    async def test_run_dry_run_both_datasets(self):
        with (
            patch("candata_pipeline.pipelines.procurement.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.procurement.ProcurementSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock()

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_contracts_df())
            source.transform = MagicMock(side_effect=lambda raw, *, dataset="contracts": raw)

            results = await run(dry_run=True)

        assert "contracts" in results
        assert "tenders" in results
        loader.start_pipeline_run.assert_called_once()
        loader.finish_pipeline_run.assert_called_once()
        loader.upsert.assert_not_called()

    @pytest.mark.asyncio
    async def test_run_contracts_only(self):
        with (
            patch("candata_pipeline.pipelines.procurement.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.procurement.ProcurementSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock(
                return_value=LoadResult(table="contracts", records_loaded=4)
            )

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_contracts_df())
            source.transform = MagicMock(side_effect=lambda raw, *, dataset="contracts": raw)

            results = await run(datasets=["contracts"])

        assert "contracts" in results
        assert "tenders" not in results

    @pytest.mark.asyncio
    async def test_run_records_failure(self):
        with (
            patch("candata_pipeline.pipelines.procurement.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.procurement.ProcurementSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.fail_pipeline_run = AsyncMock()

            source = MockSource.return_value
            source.extract = AsyncMock(side_effect=RuntimeError("API down"))

            with pytest.raises(RuntimeError, match="API down"):
                await run(datasets=["contracts"])

            loader.fail_pipeline_run.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_with_fiscal_year(self):
        with (
            patch("candata_pipeline.pipelines.procurement.SupabaseLoader") as MockLoader,
            patch("candata_pipeline.pipelines.procurement.ProcurementSource") as MockSource,
        ):
            loader = MockLoader.return_value
            loader.start_pipeline_run = AsyncMock(return_value="run-123")
            loader.finish_pipeline_run = AsyncMock()
            loader.upsert = AsyncMock()

            source = MockSource.return_value
            source.extract = AsyncMock(return_value=_sample_contracts_df())
            source.transform = MagicMock(side_effect=lambda raw, *, dataset="contracts": raw)

            results = await run(
                datasets=["contracts"],
                fiscal_year="2024-2025",
                dry_run=True,
            )

        assert results["contracts"].records_loaded == 3  # excludes 2023-02-10
