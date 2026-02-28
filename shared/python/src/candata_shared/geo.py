"""
geo.py — Geography lookup and normalization helpers.

Used by the pipeline to resolve StatCan geographic strings (which can be
inconsistent — "Ontario", "Ont.", "ON", province code "35") to canonical
SGC codes stored in the database.

Usage:
    from candata_shared.geo import normalize_statcan_geo, province_name_to_code

    level, code = normalize_statcan_geo("Ontario")   # ("pr", "35")
    level, code = normalize_statcan_geo("35")        # ("pr", "35")
    level, code = normalize_statcan_geo("Canada")    # ("country", "01")
    code = province_name_to_code("Ont.")             # "35"
"""

from __future__ import annotations

import re
from difflib import get_close_matches

import polars as pl

from candata_shared.constants import (
    ABBREVIATION_TO_CODE,
    CMA_CODES_CANONICAL,
    PROVINCE_ABBREVIATIONS,
    PROVINCE_NAME_TO_CODE,
    PROVINCES,
)

# ---------------------------------------------------------------------------
# Province lookup tables (all lowercase for fuzzy matching)
# ---------------------------------------------------------------------------

# Full name variations for fuzzy matching
_PROVINCE_ALIASES: dict[str, str] = {
    # Standard names
    **{name.lower(): code for code, name in PROVINCES.items()},
    # Common abbreviations
    **{abbr.lower(): code for abbr, code in ABBREVIATION_TO_CODE.items()},
    # SGC numeric codes
    **{code: code for code in PROVINCES},
    # Dot-abbreviations and other variants
    "nfld": "10",
    "nfld.": "10",
    "n.l.": "10",
    "n.l": "10",
    "nfl": "10",
    "p.e.i.": "11",
    "p.e.i": "11",
    "pei": "11",
    "n.s.": "12",
    "n.s": "12",
    "n.b.": "13",
    "n.b": "13",
    "que.": "24",
    "que": "24",
    "qué.": "24",
    "q.c.": "24",
    "ont.": "35",
    "ont": "35",
    "man.": "46",
    "man": "46",
    "sask.": "47",
    "sask": "47",
    "alta.": "48",
    "alta": "48",
    "b.c.": "59",
    "b.c": "59",
    "y.t.": "60",
    "y.t": "60",
    "n.w.t.": "61",
    "n.w.t": "61",
    "nwt": "61",
    "nun.": "62",
    "nun": "62",
}

_CMA_NAME_TO_CODE: dict[str, str] = {
    name.lower(): code for code, name in CMA_CODES_CANONICAL.items()
}


def province_name_to_code(name: str) -> str | None:
    """
    Resolve a province name, abbreviation, or SGC code string to a 2-digit SGC code.

    Performs an exact match first, then falls back to fuzzy matching.

    Args:
        name: Province name, abbreviation, or SGC code (e.g. "Ontario", "ON", "35").

    Returns:
        2-digit SGC code string (e.g. "35"), or None if no match found.
    """
    if not name:
        return None

    key = name.strip().lower()

    # Exact match
    if key in _PROVINCE_ALIASES:
        return _PROVINCE_ALIASES[key]

    # Numeric code as-is
    if re.fullmatch(r"\d{2}", name.strip()):
        if name.strip() in PROVINCES:
            return name.strip()

    # Fuzzy match against full province names
    candidates = list(_PROVINCE_ALIASES.keys())
    matches = get_close_matches(key, candidates, n=1, cutoff=0.8)
    if matches:
        return _PROVINCE_ALIASES[matches[0]]

    return None


def cma_name_to_code(name: str) -> str | None:
    """
    Resolve a CMA name to its canonical code.

    Args:
        name: CMA name (e.g. "Toronto", "Greater Toronto Area").

    Returns:
        3-digit canonical CMA code, or None if not found.
    """
    if not name:
        return None

    key = name.strip().lower()

    if key in _CMA_NAME_TO_CODE:
        return _CMA_NAME_TO_CODE[key]

    # Fuzzy
    matches = get_close_matches(key, list(_CMA_NAME_TO_CODE.keys()), n=1, cutoff=0.75)
    if matches:
        return _CMA_NAME_TO_CODE[matches[0]]

    return None


def normalize_statcan_geo(geo_string: str) -> tuple[str, str] | None:
    """
    Normalize a StatCan geographic string to (level, sgc_code).

    StatCan data often contains geographic strings like:
    - "Canada", "canada"
    - "Ontario", "Ont.", "ON", "35"
    - "Toronto, Ontario" (CMA name)
    - FSA strings like "M5V"

    Args:
        geo_string: Raw geographic string from StatCan data.

    Returns:
        Tuple of (level, sgc_code) where level is a geography_levels.id value,
        or None if the string cannot be resolved.

    Examples:
        >>> normalize_statcan_geo("Ontario")
        ("pr", "35")
        >>> normalize_statcan_geo("Canada")
        ("country", "01")
        >>> normalize_statcan_geo("M5V")
        ("fsa", "M5V")
    """
    if not geo_string:
        return None

    s = geo_string.strip()

    # Canada
    if s.lower() in ("canada", "can", "ca", "01"):
        return ("country", "01")

    # Province check
    code = province_name_to_code(s)
    if code:
        return ("pr", code)

    # CMA name (may contain ", Province" suffix)
    cma_part = s.split(",")[0].strip()
    cma_code = cma_name_to_code(cma_part)
    if cma_code:
        return ("cma", cma_code)

    # FSA: letter-digit-letter pattern (e.g. "M5V")
    if re.fullmatch(r"[A-Za-z]\d[A-Za-z]", s):
        return ("fsa", s.upper())

    return None


def fsa_to_province_code(fsa: str) -> str | None:
    """
    Map a Forward Sortation Area prefix letter to a province SGC code.

    The first letter of a Canadian postal code identifies the province.
    """
    fsa_province_map: dict[str, str] = {
        "A": "10",  # NL
        "B": "12",  # NS
        "C": "11",  # PE
        "E": "13",  # NB
        "G": "24",  # QC (eastern)
        "H": "24",  # QC (Montreal)
        "J": "24",  # QC (western)
        "K": "35",  # ON (eastern)
        "L": "35",  # ON (central)
        "M": "35",  # ON (Toronto)
        "N": "35",  # ON (southwestern)
        "P": "35",  # ON (northern)
        "R": "46",  # MB
        "S": "47",  # SK
        "T": "48",  # AB
        "V": "59",  # BC
        "X": "61",  # NT / NU
        "Y": "60",  # YT
    }
    if not fsa or len(fsa) < 1:
        return None
    return fsa_province_map.get(fsa[0].upper())


def normalize_geo_column(
    df: pl.DataFrame,
    geo_col: str = "GEO",
    *,
    code_alias: str = "sgc_code",
    level_alias: str = "geo_level",
) -> pl.DataFrame:
    """Add ``sgc_code`` and ``geo_level`` columns using a batch lookup.

    Instead of calling :func:`normalize_statcan_geo` row-by-row via
    ``map_elements`` (which converts every value to a Python object),
    this function:

    1. Extracts the unique GEO strings.
    2. Resolves each unique value once through :func:`normalize_statcan_geo`.
    3. Joins the results back to the original DataFrame.

    For a 500 000-row DataFrame with 15 unique GEO values this calls the
    Python function only 15 times instead of 500 000.

    Args:
        df:          Input DataFrame containing *geo_col*.
        geo_col:     Name of the column holding raw StatCan GEO strings.
        code_alias:  Output column name for the SGC code.
        level_alias: Output column name for the geo level.

    Returns:
        DataFrame with two new columns (*code_alias*, *level_alias*).
    """
    uniques = df.select(pl.col(geo_col).unique().drop_nulls()).to_series().to_list()

    codes: list[str | None] = []
    levels: list[str | None] = []
    for geo in uniques:
        result = normalize_statcan_geo(geo)
        if result:
            levels.append(result[0])
            codes.append(result[1])
        else:
            levels.append(None)
            codes.append(None)

    lookup = pl.DataFrame(
        {
            geo_col: uniques,
            code_alias: codes,
            level_alias: levels,
        },
        schema={
            geo_col: pl.String,
            code_alias: pl.String,
            level_alias: pl.String,
        },
    )

    return df.join(lookup, on=geo_col, how="left")
