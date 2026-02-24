"""
tests/test_transforms/test_time_series.py — Tests for time-series transforms.
"""

from __future__ import annotations

from datetime import date

import polars as pl
import pytest

from candata_pipeline.transforms.time_series import (
    align_to_period_start,
    compute_period_over_period,
    deduplicate_series,
    fill_gaps,
    resample_to_frequency,
)


class TestAlignToPeriodStart:
    def test_monthly_alignment(self):
        df = pl.DataFrame({"ref_date": [date(2024, 3, 15), date(2024, 3, 1), date(2024, 3, 31)]})
        result = align_to_period_start(df, "ref_date", "monthly")
        assert all(d.day == 1 for d in result["ref_date"].to_list())

    def test_quarterly_alignment(self):
        df = pl.DataFrame({
            "ref_date": [date(2024, 1, 15), date(2024, 2, 28), date(2024, 3, 31),
                         date(2024, 4, 1), date(2024, 5, 15)]
        })
        result = align_to_period_start(df, "ref_date", "quarterly")
        dates = result["ref_date"].to_list()
        assert dates[0] == date(2024, 1, 1)  # Q1
        assert dates[1] == date(2024, 1, 1)  # Q1
        assert dates[2] == date(2024, 1, 1)  # Q1
        assert dates[3] == date(2024, 4, 1)  # Q2
        assert dates[4] == date(2024, 4, 1)  # Q2

    def test_annual_alignment(self):
        df = pl.DataFrame({"ref_date": [date(2024, 6, 15), date(2024, 12, 31)]})
        result = align_to_period_start(df, "ref_date", "annual")
        assert all(d == date(2024, 1, 1) for d in result["ref_date"].to_list())

    def test_semi_annual_alignment(self):
        df = pl.DataFrame({
            "ref_date": [date(2024, 3, 1), date(2024, 7, 1), date(2024, 11, 30)]
        })
        result = align_to_period_start(df, "ref_date", "semi-annual")
        dates = result["ref_date"].to_list()
        assert dates[0] == date(2024, 1, 1)  # H1
        assert dates[1] == date(2024, 7, 1)  # H2
        assert dates[2] == date(2024, 7, 1)  # H2

    def test_daily_alignment_is_identity(self):
        original = [date(2024, 3, 15), date(2024, 3, 16)]
        df = pl.DataFrame({"ref_date": original})
        result = align_to_period_start(df, "ref_date", "daily")
        assert result["ref_date"].to_list() == original

    def test_null_dates_remain_null(self):
        df = pl.DataFrame({"ref_date": [date(2024, 3, 15), None]})
        result = align_to_period_start(df, "ref_date", "monthly")
        assert result["ref_date"][1] is None


class TestDeduplicateSeries:
    def test_removes_exact_duplicates(self):
        df = pl.DataFrame({
            "indicator_id": ["cpi", "cpi", "cpi"],
            "geography_id": ["g1", "g1", "g1"],
            "ref_date": [date(2023, 1, 1), date(2023, 1, 1), date(2023, 2, 1)],
            "value": [157.1, 157.2, 158.0],
        })
        result = deduplicate_series(df, ["indicator_id", "geography_id", "ref_date"])
        assert len(result) == 2  # Jan and Feb, not Jan twice

    def test_keeps_last_by_default(self):
        df = pl.DataFrame({
            "key": ["a", "a"],
            "sort_col": [1, 2],
            "value": [10.0, 20.0],
        })
        result = deduplicate_series(df, ["key"], keep="last", sort_col="sort_col")
        assert result["value"][0] == pytest.approx(20.0)

    def test_keeps_first_when_specified(self):
        df = pl.DataFrame({
            "key": ["a", "a"],
            "sort_col": [1, 2],
            "value": [10.0, 20.0],
        })
        result = deduplicate_series(df, ["key"], keep="first", sort_col="sort_col")
        assert result["value"][0] == pytest.approx(10.0)

    def test_no_duplicates_returns_unchanged(self):
        df = pl.DataFrame({
            "key": ["a", "b", "c"],
            "value": [1.0, 2.0, 3.0],
        })
        result = deduplicate_series(df, ["key"])
        assert len(result) == 3

    def test_empty_dataframe(self):
        df = pl.DataFrame(schema={"key": pl.String, "value": pl.Float64})
        result = deduplicate_series(df, ["key"])
        assert len(result) == 0


class TestFillGaps:
    @pytest.fixture
    def monthly_series(self) -> pl.DataFrame:
        return pl.DataFrame({
            "indicator_id": ["cpi"] * 4,
            "geography_id": ["g1"] * 4,
            "ref_date": [
                date(2023, 1, 1),
                date(2023, 2, 1),
                # gap: March missing
                date(2023, 4, 1),
                date(2023, 5, 1),
            ],
            "value": [157.1, 158.4, 160.2, 161.0],
        })

    def test_fill_gaps_forward_fill(self, monthly_series: pl.DataFrame):
        result = fill_gaps(
            monthly_series,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            frequency="monthly",
            strategy="forward_fill",
        )
        # Should have 5 rows (Jan, Feb, Mar, Apr, May)
        assert len(result) == 5
        # March should be filled with February's value
        march = result.filter(pl.col("ref_date") == date(2023, 3, 1))
        assert len(march) == 1
        assert march["value"][0] == pytest.approx(158.4)

    def test_fill_gaps_zero_fill(self, monthly_series: pl.DataFrame):
        result = fill_gaps(
            monthly_series,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            frequency="monthly",
            strategy="zero",
        )
        march = result.filter(pl.col("ref_date") == date(2023, 3, 1))
        assert march["value"][0] == pytest.approx(0.0)

    def test_fill_gaps_drop_strategy(self, monthly_series: pl.DataFrame):
        result = fill_gaps(
            monthly_series,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            frequency="monthly",
            strategy="drop",
        )
        # No gaps generated when we drop null-filled rows
        assert len(result) == len(monthly_series)

    def test_fill_gaps_preserves_groups(self):
        """Each series group should be filled independently."""
        df = pl.DataFrame({
            "indicator_id": ["cpi", "cpi", "gdp", "gdp"],
            "geography_id": ["g1", "g1", "g1", "g1"],
            "ref_date": [date(2023, 1, 1), date(2023, 3, 1),
                         date(2023, 1, 1), date(2023, 3, 1)],
            "value": [100.0, 102.0, 200.0, 204.0],
        })
        result = fill_gaps(
            df,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            frequency="monthly",
            strategy="forward_fill",
        )
        # Each group has 3 months → 6 rows total
        assert len(result) == 6

    def test_fill_gaps_empty_df_returns_empty(self):
        df = pl.DataFrame(schema={
            "indicator_id": pl.String,
            "geography_id": pl.String,
            "ref_date": pl.Date,
            "value": pl.Float64,
        })
        result = fill_gaps(
            df,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            frequency="monthly",
            strategy="forward_fill",
        )
        assert result.is_empty()


class TestComputePeriodOverPeriod:
    def test_mom_change(self):
        df = pl.DataFrame({
            "indicator_id": ["cpi"] * 3,
            "geography_id": ["g1"] * 3,
            "ref_date": [date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1)],
            "value": [100.0, 102.0, 101.0],
        })
        result = compute_period_over_period(
            df,
            value_col="value",
            date_col="ref_date",
            group_cols=["indicator_id", "geography_id"],
            periods=1,
        )
        assert "value_pct_chg" in result.columns
        # Feb pct change: (102-100)/100 * 100 = 2.0
        feb = result.filter(pl.col("ref_date") == date(2023, 2, 1))
        assert feb["value_pct_chg"][0] == pytest.approx(2.0, rel=0.01)

    def test_custom_output_column(self):
        df = pl.DataFrame({
            "g": ["g1", "g1"],
            "d": [date(2023, 1, 1), date(2023, 2, 1)],
            "v": [100.0, 105.0],
        })
        result = compute_period_over_period(
            df, value_col="v", date_col="d", group_cols=["g"],
            output_col="mom_pct"
        )
        assert "mom_pct" in result.columns


class TestResampleToFrequency:
    def test_daily_to_monthly_mean(self):
        # 30 daily obs in January averaging 157.x
        dates = [date(2023, 1, d) for d in range(1, 31)]
        values = [157.0 + (i * 0.01) for i in range(30)]
        df = pl.DataFrame({
            "indicator_id": ["cpi"] * 30,
            "geography_id": ["g1"] * 30,
            "ref_date": dates,
            "value": values,
        })
        result = resample_to_frequency(
            df,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            source_freq="daily",
            target_freq="monthly",
            agg="mean",
        )
        assert len(result) == 1  # one monthly record
        assert result["ref_date"][0] == date(2023, 1, 1)
        assert result["value"][0] == pytest.approx(157.145, rel=0.01)

    def test_monthly_to_quarterly_sum(self):
        df = pl.DataFrame({
            "indicator_id": ["gdp"] * 6,
            "geography_id": ["g1"] * 6,
            "ref_date": [
                date(2023, 1, 1), date(2023, 2, 1), date(2023, 3, 1),
                date(2023, 4, 1), date(2023, 5, 1), date(2023, 6, 1),
            ],
            "value": [100.0, 105.0, 110.0, 108.0, 112.0, 115.0],
        })
        result = resample_to_frequency(
            df,
            date_col="ref_date",
            value_col="value",
            group_cols=["indicator_id", "geography_id"],
            source_freq="monthly",
            target_freq="quarterly",
            agg="sum",
        )
        assert len(result) == 2  # Q1 and Q2
        q1 = result.filter(pl.col("ref_date") == date(2023, 1, 1))
        assert q1["value"][0] == pytest.approx(315.0)
