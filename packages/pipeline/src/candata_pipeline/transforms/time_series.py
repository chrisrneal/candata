"""
transforms/time_series.py — Date alignment, resampling, and gap-fill for time series.

Works entirely on polars DataFrames. Intended for use after raw source data
has been parsed into a DataFrame with a ref_date Date column.

Usage:
    from candata_pipeline.transforms.time_series import (
        align_to_period_start,
        resample_to_frequency,
        fill_gaps,
        deduplicate_series,
    )

    # Snap all dates to first-of-month for a monthly series
    df = align_to_period_start(df, "ref_date", "monthly")

    # Forward-fill gaps (e.g. for daily → monthly alignment)
    df = fill_gaps(df, date_col="ref_date", value_col="value",
                   group_cols=["indicator_id", "geography_id"],
                   strategy="forward_fill")

    # Remove duplicate (indicator, geo, date) rows, keeping latest revision
    df = deduplicate_series(df, ["indicator_id", "geography_id", "ref_date"])
"""

from __future__ import annotations

from datetime import date
from typing import Literal

import polars as pl
import structlog

from candata_shared.time_utils import align_frequency, date_range

log = structlog.get_logger(__name__)

Frequency = Literal["daily", "weekly", "monthly", "quarterly", "semi-annual", "annual"]
FillStrategy = Literal["forward_fill", "backward_fill", "interpolate", "zero", "drop"]


def align_to_period_start(
    df: pl.DataFrame,
    date_col: str,
    frequency: Frequency,
) -> pl.DataFrame:
    """
    Snap all dates in date_col to the first day of their period.

    E.g. for "monthly": date(2024, 3, 15) → date(2024, 3, 1)

    Args:
        df:         Input DataFrame.
        date_col:   Name of the Date column.
        frequency:  Target frequency.

    Returns:
        DataFrame with date_col values snapped to period start.
    """
    return df.with_columns(
        pl.col(date_col)
        .map_elements(
            lambda d: align_frequency(d, frequency) if d is not None else None,
            return_dtype=pl.Date,
        )
        .alias(date_col)
    )


def deduplicate_series(
    df: pl.DataFrame,
    key_cols: list[str],
    *,
    keep: Literal["first", "last"] = "last",
    sort_col: str | None = None,
) -> pl.DataFrame:
    """
    Remove duplicate rows by (key_cols), keeping first or last occurrence.

    Use case: StatCan releases revised values; we keep the most recent.

    Args:
        df:       Input DataFrame.
        key_cols: Columns that define uniqueness.
        keep:     Which duplicate to keep ("first" | "last").
        sort_col: If set, sort by this column before deduplication.

    Returns:
        Deduplicated DataFrame.
    """
    n_before = len(df)

    if sort_col and sort_col in df.columns:
        df = df.sort(sort_col, descending=False)

    df = df.unique(subset=key_cols, keep=keep, maintain_order=True)

    dropped = n_before - len(df)
    if dropped:
        log.debug("deduplicated", dropped=dropped, key_cols=key_cols)

    return df


def fill_gaps(
    df: pl.DataFrame,
    *,
    date_col: str,
    value_col: str,
    group_cols: list[str],
    frequency: Frequency,
    strategy: FillStrategy = "forward_fill",
    start_date: date | None = None,
    end_date: date | None = None,
) -> pl.DataFrame:
    """
    Fill temporal gaps in a time series DataFrame.

    For each group (e.g. each indicator+geography combination), generates
    the full expected date range and fills missing periods according to
    strategy.

    Args:
        df:          Input DataFrame sorted by date within groups.
        date_col:    Date column name.
        value_col:   Value column to fill.
        group_cols:  Columns identifying each series (e.g. ["indicator_id", "geography_id"]).
        frequency:   Expected data frequency.
        strategy:    How to fill missing periods.
        start_date:  Override the series start date (default: earliest in df).
        end_date:    Override the series end date (default: latest in df).

    Returns:
        DataFrame with gaps filled according to strategy.
    """
    if df.is_empty():
        return df

    actual_start = start_date or df[date_col].min()
    actual_end = end_date or df[date_col].max()

    if actual_start is None or actual_end is None:
        return df

    full_dates = date_range(actual_start, actual_end, frequency)
    date_spine = pl.DataFrame({date_col: full_dates})

    filled_groups: list[pl.DataFrame] = []

    for group_keys, group_df in df.group_by(group_cols):
        if not isinstance(group_keys, (list, tuple)):
            group_keys = [group_keys]

        # Cross join the date spine with this group's constant key values
        key_df = pl.DataFrame(
            {col: [val] for col, val in zip(group_cols, group_keys)}
        )
        full_series = date_spine.join(key_df, how="cross")

        # Left join to bring in actual values
        merged = full_series.join(
            group_df.select([date_col, *group_cols, value_col]),
            on=[date_col, *group_cols],
            how="left",
        )

        # Apply fill strategy
        if strategy == "forward_fill":
            merged = merged.with_columns(pl.col(value_col).forward_fill())
        elif strategy == "backward_fill":
            merged = merged.with_columns(pl.col(value_col).backward_fill())
        elif strategy == "interpolate":
            merged = merged.with_columns(
                pl.col(value_col).interpolate()
            )
        elif strategy == "zero":
            merged = merged.with_columns(pl.col(value_col).fill_null(0.0))
        elif strategy == "drop":
            merged = merged.filter(pl.col(value_col).is_not_null())

        filled_groups.append(merged)

    if not filled_groups:
        return df

    result = pl.concat(filled_groups)
    log.debug(
        "gaps_filled",
        strategy=strategy,
        before_rows=len(df),
        after_rows=len(result),
        new_rows=len(result) - len(df),
    )
    return result


def compute_period_over_period(
    df: pl.DataFrame,
    *,
    value_col: str,
    date_col: str,
    group_cols: list[str],
    periods: int = 1,
    output_col: str | None = None,
) -> pl.DataFrame:
    """
    Add a period-over-period percent change column.

    Args:
        df:          Input DataFrame, sorted by date within groups.
        value_col:   Numeric column to compute change on.
        date_col:    Date column (used for sorting).
        group_cols:  Group-by columns (e.g. ["indicator_id", "geography_id"]).
        periods:     Number of periods to shift (default 1 = MoM, 12 = YoY monthly).
        output_col:  Name for the new column (default: "{value_col}_pct_chg").

    Returns:
        DataFrame with new pct change column appended.
    """
    out_col = output_col or f"{value_col}_pct_chg"
    return df.sort([*group_cols, date_col]).with_columns(
        (
            (pl.col(value_col) - pl.col(value_col).shift(periods).over(group_cols))
            / pl.col(value_col).shift(periods).over(group_cols)
            * 100.0
        ).alias(out_col)
    )


def resample_to_frequency(
    df: pl.DataFrame,
    *,
    date_col: str,
    value_col: str,
    group_cols: list[str],
    source_freq: Frequency,
    target_freq: Frequency,
    agg: Literal["mean", "sum", "last", "first"] = "mean",
) -> pl.DataFrame:
    """
    Resample a time series from one frequency to a lower one.

    E.g. daily → monthly, monthly → quarterly.

    Args:
        df:          Input DataFrame.
        date_col:    Date column.
        value_col:   Value column.
        group_cols:  Series group columns.
        source_freq: Current data frequency (must be higher than target).
        target_freq: Desired output frequency.
        agg:         Aggregation method for downsampling.

    Returns:
        Resampled DataFrame.
    """
    # Snap dates to the target period start
    df = df.with_columns(
        pl.col(date_col)
        .map_elements(
            lambda d: align_frequency(d, target_freq) if d is not None else None,
            return_dtype=pl.Date,
        )
        .alias(date_col)
    )

    agg_expr: pl.Expr
    match agg:
        case "mean":
            agg_expr = pl.col(value_col).mean()
        case "sum":
            agg_expr = pl.col(value_col).sum()
        case "last":
            agg_expr = pl.col(value_col).last()
        case "first":
            agg_expr = pl.col(value_col).first()
        case _:
            agg_expr = pl.col(value_col).mean()

    return df.group_by([*group_cols, date_col]).agg(agg_expr).sort([*group_cols, date_col])
