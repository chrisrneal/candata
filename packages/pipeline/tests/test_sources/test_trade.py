"""
tests/test_sources/test_trade.py — Unit tests for the trade data source.

Tests cover:
  - HS code extraction from NAPCS descriptions
  - Country name normalization
  - Commodity trade transform (direction filtering, province mapping)
  - Bilateral trade transform (partner country parsing)
  - Suppressed value handling
  - Date filtering (start_date / end_date)
"""

from __future__ import annotations

from datetime import date
from pathlib import Path

import polars as pl
import pytest

from candata_pipeline.sources.trade import (
    TradeSource,
    extract_hs_code,
    normalize_country,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# extract_hs_code
# ---------------------------------------------------------------------------

class TestExtractHsCode:
    def test_bracketed_four_digit(self):
        assert extract_hs_code("[0201] - Meat of bovine animals, fresh or chilled") == "0201"

    def test_bracketed_two_digit(self):
        assert extract_hs_code("[21] - Pharmaceutical products") == "21"

    def test_no_brackets(self):
        assert extract_hs_code("0201 - Meat of bovine animals") == "0201"

    def test_long_code(self):
        assert extract_hs_code("[27090010] - Petroleum crude") == "27090010"

    def test_total_row_returns_none(self):
        assert extract_hs_code("Total of all merchandise") is None

    def test_none_input(self):
        assert extract_hs_code(None) is None

    def test_empty_string(self):
        assert extract_hs_code("") is None

    def test_leading_whitespace(self):
        assert extract_hs_code("  [0201] - Meat") == "0201"


# ---------------------------------------------------------------------------
# normalize_country
# ---------------------------------------------------------------------------

class TestNormalizeCountry:
    def test_united_states(self):
        assert normalize_country("United States") == "USA"

    def test_china(self):
        assert normalize_country("China") == "CHN"

    def test_united_kingdom(self):
        assert normalize_country("United Kingdom") == "GBR"

    def test_all_countries(self):
        assert normalize_country("All countries") == "WLD"

    def test_unknown_country_returned_as_is(self):
        assert normalize_country("Narnia") == "Narnia"

    def test_none_input(self):
        assert normalize_country(None) is None

    def test_whitespace_stripped(self):
        assert normalize_country("  Japan  ") == "JPN"


# ---------------------------------------------------------------------------
# TradeSource.transform (commodity table)
# ---------------------------------------------------------------------------

class TestCommodityTransform:
    @pytest.fixture
    def raw_commodity_df(self) -> pl.DataFrame:
        return pl.read_csv(
            FIXTURES_DIR / "statcan_trade_commodity_sample.csv",
            infer_schema_length=0,
            null_values=["", "..", "x", "F"],
        )

    @pytest.fixture
    def source(self) -> TradeSource:
        return TradeSource.__new__(TradeSource)

    def _init_source(self, source: TradeSource):
        # Minimal init without settings dependency
        import structlog
        source._log = structlog.get_logger("test")

    def test_filters_trade_balance_rows(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        directions = df["direction"].unique().to_list()
        assert "trade balance" not in directions
        assert set(directions) <= {"import", "export"}

    def test_extracts_hs_codes(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        hs_codes = df["hs_code"].unique().to_list()
        assert "0201" in hs_codes
        assert "2709" in hs_codes

    def test_filters_total_rows(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        # "Total of all merchandise" has no HS code → filtered out
        assert df.filter(pl.col("hs_description").str.contains("Total")).is_empty()

    def test_maps_provinces(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        provinces = df["province"].unique().to_list()
        assert "01" in provinces  # Canada
        assert "35" in provinces  # Ontario

    def test_parses_ref_date(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        dates = df["ref_date"].unique().sort().to_list()
        assert date(2024, 1, 1) in dates
        assert date(2024, 2, 1) in dates

    def test_start_date_filter(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df, start_date=date(2024, 2, 1))
        dates = df["ref_date"].unique().to_list()
        assert date(2024, 1, 1) not in dates
        assert date(2024, 2, 1) in dates

    def test_end_date_filter(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df, end_date=date(2024, 1, 31))
        dates = df["ref_date"].unique().to_list()
        assert date(2024, 1, 1) in dates
        assert date(2024, 2, 1) not in dates

    def test_suppressed_value_is_null(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        # BC row with "x" in VALUE should have null value_cad
        bc_rows = df.filter(
            (pl.col("province") == "59") & (pl.col("hs_code") == "4407")
        )
        if not bc_rows.is_empty():
            assert bc_rows["value_cad"][0] is None

    def test_commodity_partner_is_wld(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        # Commodity table has no partner column → all WLD
        assert (df["partner_country"] == "WLD").all()

    def test_output_columns(self, raw_commodity_df, source):
        self._init_source(source)
        df = source.transform(raw_commodity_df)
        expected = {
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        }
        assert set(df.columns) == expected


# ---------------------------------------------------------------------------
# TradeSource.transform_bilateral
# ---------------------------------------------------------------------------

class TestBilateralTransform:
    @pytest.fixture
    def raw_bilateral_df(self) -> pl.DataFrame:
        return pl.read_csv(
            FIXTURES_DIR / "statcan_trade_bilateral_sample.csv",
            infer_schema_length=0,
            null_values=["", "..", "x", "F"],
        )

    @pytest.fixture
    def source(self) -> TradeSource:
        return TradeSource.__new__(TradeSource)

    def _init_source(self, source: TradeSource):
        import structlog
        source._log = structlog.get_logger("test")

    def test_normalizes_partner_countries(self, raw_bilateral_df, source):
        self._init_source(source)
        df = source.transform_bilateral(raw_bilateral_df)
        partners = df["partner_country"].unique().to_list()
        assert "USA" in partners
        assert "CHN" in partners
        assert "GBR" in partners

    def test_filters_trade_balance(self, raw_bilateral_df, source):
        self._init_source(source)
        df = source.transform_bilateral(raw_bilateral_df)
        assert "trade balance" not in df["direction"].unique().to_list()

    def test_maps_provinces(self, raw_bilateral_df, source):
        self._init_source(source)
        df = source.transform_bilateral(raw_bilateral_df)
        provinces = df["province"].unique().to_list()
        assert "01" in provinces  # Canada
        assert "35" in provinces  # Ontario

    def test_output_columns(self, raw_bilateral_df, source):
        self._init_source(source)
        df = source.transform_bilateral(raw_bilateral_df)
        expected = {
            "direction", "hs_code", "hs_description", "partner_country",
            "province", "ref_date", "value_cad", "volume", "volume_unit",
        }
        assert set(df.columns) == expected
