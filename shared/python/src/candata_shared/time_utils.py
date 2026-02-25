"""
time_utils.py — Date parsing and frequency alignment utilities.

StatCan publishes dates in various formats:
- ISO: "2024-01-01"
- Monthly: "January 2024", "Jan. 2024", "2024-01" (YYYY-MM)
- Quarterly: "2024-Q1", "Q1 2024", "2024Q1"
- Annual: "2024"
- Semi-annual: "2024 (October)", "2024H1", "2024H2"

Usage:
    from candata_shared.time_utils import parse_statcan_date, align_frequency

    dt = parse_statcan_date("2024-01")           # date(2024, 1, 1)
    dt = parse_statcan_date("Q1 2024")           # date(2024, 1, 1)
    dt = parse_statcan_date("January 2024")      # date(2024, 1, 1)
    dt = parse_statcan_date("2024-10-15")        # date(2024, 10, 15)
    aligned = align_frequency(date(2024, 3, 15), "monthly")  # date(2024, 3, 1)
"""

from __future__ import annotations

import re
from datetime import date, datetime
from typing import Literal

from dateutil.relativedelta import relativedelta

import polars as pl

Frequency = Literal["daily", "weekly", "monthly", "quarterly", "semi-annual", "annual"]


# Month name → month number (English, including French variants and abbreviations)
_MONTH_NAMES: dict[str, int] = {
    "january": 1, "jan": 1, "jan.": 1, "janvier": 1,
    "february": 2, "feb": 2, "feb.": 2, "février": 2,
    "march": 3, "mar": 3, "mar.": 3, "mars": 3,
    "april": 4, "apr": 4, "apr.": 4, "avril": 4,
    "may": 5, "mai": 5,
    "june": 6, "jun": 6, "jun.": 6, "juin": 6,
    "july": 7, "jul": 7, "jul.": 7, "juillet": 7,
    "august": 8, "aug": 8, "aug.": 8, "août": 8,
    "september": 9, "sep": 9, "sep.": 9, "sept": 9, "septembre": 9,
    "october": 10, "oct": 10, "oct.": 10, "octobre": 10,
    "november": 11, "nov": 11, "nov.": 11, "novembre": 11,
    "december": 12, "dec": 12, "dec.": 12, "décembre": 12,
}


def parse_statcan_date(raw: str) -> date | None:
    """
    Parse a StatCan date string into a Python date object.

    Returns the FIRST day of the period for all sub-annual frequencies.
    Returns None if the string cannot be parsed.

    Args:
        raw: Raw date string from StatCan / BoC data.

    Returns:
        datetime.date or None.
    """
    if not raw:
        return None

    s = raw.strip()

    # ISO full date: YYYY-MM-DD
    m = re.fullmatch(r"(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), int(m.group(3)))

    # YYYY-MM (monthly)
    m = re.fullmatch(r"(\d{4})-(\d{2})", s)
    if m:
        return date(int(m.group(1)), int(m.group(2)), 1)

    # Quarterly: Q1 2024 or 2024-Q1 or 2024Q1
    m = re.fullmatch(r"[Qq](\d)\s+(\d{4})", s)
    if not m:
        m = re.fullmatch(r"(\d{4})-?[Qq](\d)", s)
        if m:
            year, quarter = int(m.group(1)), int(m.group(2))
        else:
            quarter = None
    else:
        quarter, year = int(m.group(1)), int(m.group(2))
    if quarter is not None:
        month = (quarter - 1) * 3 + 1
        return date(year, month, 1)

    # Semi-annual: 2024H1, 2024H2, H1 2024
    m = re.fullmatch(r"(\d{4})[Hh]([12])", s)
    if not m:
        m = re.fullmatch(r"[Hh]([12])\s+(\d{4})", s)
        if m:
            half, year = int(m.group(1)), int(m.group(2))
        else:
            half = None
    else:
        year, half = int(m.group(1)), int(m.group(2))
    if half is not None:
        month = 1 if half == 1 else 7
        return date(year, month, 1)

    # "October 2024" or "Oct. 2024"
    m = re.fullmatch(r"([A-Za-zéèêûôîâùàäëïü.]+\.?)\s+(\d{4})", s)
    if m:
        month_str = m.group(1).rstrip(".").lower()
        year = int(m.group(2))
        month_num = _MONTH_NAMES.get(month_str)
        if month_num:
            return date(year, month_num, 1)

    # "2024 (October)" — CMHC style
    m = re.fullmatch(r"(\d{4})\s+\(([A-Za-z]+)\)", s)
    if m:
        year = int(m.group(1))
        month_str = m.group(2).lower()
        month_num = _MONTH_NAMES.get(month_str)
        if month_num:
            return date(year, month_num, 1)

    # Annual: plain 4-digit year
    m = re.fullmatch(r"(\d{4})", s)
    if m:
        return date(int(m.group(1)), 1, 1)

    return None


def parse_statcan_date_expr(col: str = "REF_DATE") -> pl.Expr:
    """Return a polars expression that parses StatCan date strings vectorially.

    Handles the two dominant formats entirely within the polars engine:
      - "YYYY-MM"     → date(YYYY, MM, 1)
      - "YYYY-MM-DD"  → date(YYYY, MM, DD)

    Rows that don't match either pattern (quarterly, annual, text months)
    fall back to the scalar ``parse_statcan_date`` function via
    ``map_elements``, but those are typically <1% of rows.

    Usage::

        df = df.with_columns(
            parse_statcan_date_expr("REF_DATE").alias("ref_date")
        )
    """
    c = pl.col(col).str.strip_chars()
    return (
        pl.when(c.str.len_chars() == 10)
        .then(c.str.to_date("%Y-%m-%d", strict=False))
        .when(c.str.len_chars() == 7)
        .then((c + "-01").str.to_date("%Y-%m-%d", strict=False))
        .otherwise(c.map_elements(parse_statcan_date, return_dtype=pl.Date))
    )


def align_frequency(d: date, frequency: Frequency) -> date:
    """
    Snap a date to the first day of its period for the given frequency.

    Args:
        d:          A date.
        frequency:  Target frequency.

    Returns:
        date aligned to period start.

    Examples:
        align_frequency(date(2024, 3, 15), "monthly")    -> date(2024, 3, 1)
        align_frequency(date(2024, 3, 15), "quarterly")  -> date(2024, 1, 1)
        align_frequency(date(2024, 3, 15), "annual")     -> date(2024, 1, 1)
        align_frequency(date(2024, 8, 15), "semi-annual")-> date(2024, 7, 1)
    """
    match frequency:
        case "daily":
            return d
        case "weekly":
            # Align to Monday of the week
            return d - relativedelta(days=d.weekday())
        case "monthly":
            return date(d.year, d.month, 1)
        case "quarterly":
            quarter_start_month = ((d.month - 1) // 3) * 3 + 1
            return date(d.year, quarter_start_month, 1)
        case "semi-annual":
            half_start_month = 1 if d.month <= 6 else 7
            return date(d.year, half_start_month, 1)
        case "annual":
            return date(d.year, 1, 1)
        case _:
            return d


def date_range(
    start: date,
    end: date,
    frequency: Frequency,
) -> list[date]:
    """
    Generate a list of period-start dates between start and end (inclusive).

    Args:
        start:      Start date (inclusive).
        end:        End date (inclusive).
        frequency:  Period frequency.

    Returns:
        List of dates, each aligned to period start.
    """
    dates = []
    current = align_frequency(start, frequency)
    step_map: dict[Frequency, relativedelta] = {
        "daily": relativedelta(days=1),
        "weekly": relativedelta(weeks=1),
        "monthly": relativedelta(months=1),
        "quarterly": relativedelta(months=3),
        "semi-annual": relativedelta(months=6),
        "annual": relativedelta(years=1),
    }
    step = step_map.get(frequency, relativedelta(months=1))
    while current <= end:
        dates.append(current)
        current = current + step
    return dates
