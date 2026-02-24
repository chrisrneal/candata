"""
transforms/normalize.py — Geography normalization for pipeline DataFrames.

Takes a polars DataFrame with a raw geography column and resolves it to
the canonical sgc_code and geography_id values needed for Supabase inserts.

The geography_id lookup is cached in DuckDB (using the geographies table
dumped from Supabase) to avoid a Supabase round-trip per row.

Usage:
    from candata_pipeline.transforms.normalize import GeoNormalizer

    normalizer = GeoNormalizer()
    await normalizer.load_geo_cache()   # one-time, loads from Supabase

    df = normalizer.add_sgc_code(df, geo_col="GEO")
    df = normalizer.add_geography_id(df, sgc_code_col="sgc_code")
    # df now has sgc_code and geography_id columns
"""

from __future__ import annotations

from typing import Any

import polars as pl
import structlog

from candata_shared.db import get_supabase_client
from candata_shared.geo import normalize_statcan_geo

log = structlog.get_logger(__name__)


class GeoNormalizer:
    """
    Resolves geographic strings in a polars DataFrame to Supabase geography_ids.

    Internal cache: dict[sgc_code → geography_id (str UUID)]
    Loaded once from Supabase on first call to load_geo_cache().
    """

    def __init__(self) -> None:
        self._cache: dict[str, str] = {}   # sgc_code → geography_id UUID str
        self._loaded = False

    async def load_geo_cache(self, *, force_reload: bool = False) -> None:
        """
        Fetch all rows from the geographies table and build the lookup cache.

        Args:
            force_reload: If True, re-fetch even if already loaded.
        """
        if self._loaded and not force_reload:
            return

        log.info("loading_geo_cache")
        client = get_supabase_client()
        result = client.table("geographies").select("id, sgc_code").execute()
        rows = result.data or []
        self._cache = {row["sgc_code"]: row["id"] for row in rows}
        self._loaded = True
        log.info("geo_cache_loaded", count=len(self._cache))

    def sgc_code_to_geography_id(self, sgc_code: str | None) -> str | None:
        """Look up a Supabase geography UUID from an SGC code."""
        if not sgc_code:
            return None
        return self._cache.get(sgc_code)

    def add_sgc_code(
        self,
        df: pl.DataFrame,
        geo_col: str,
        *,
        sgc_col: str = "sgc_code",
        level_col: str = "geo_level",
    ) -> pl.DataFrame:
        """
        Add sgc_code and geo_level columns by normalizing a raw geography column.

        Args:
            df:        Input DataFrame.
            geo_col:   Name of the column containing raw geography strings.
            sgc_col:   Output column for SGC code.
            level_col: Output column for geography level.

        Returns:
            DataFrame with new columns appended.
        """
        def to_sgc(geo: str | None) -> str | None:
            if not geo:
                return None
            result = normalize_statcan_geo(geo)
            return result[1] if result else None

        def to_level(geo: str | None) -> str | None:
            if not geo:
                return None
            result = normalize_statcan_geo(geo)
            return result[0] if result else None

        return df.with_columns(
            pl.col(geo_col).map_elements(to_sgc, return_dtype=pl.String).alias(sgc_col),
            pl.col(geo_col).map_elements(to_level, return_dtype=pl.String).alias(level_col),
        )

    def add_geography_id(
        self,
        df: pl.DataFrame,
        sgc_code_col: str = "sgc_code",
        *,
        geo_id_col: str = "geography_id",
    ) -> pl.DataFrame:
        """
        Add a geography_id UUID column by looking up sgc_code in the cache.

        Call load_geo_cache() before this method.

        Args:
            df:           Input DataFrame with an sgc_code column.
            sgc_code_col: Column holding SGC codes.
            geo_id_col:   Output column name.

        Returns:
            DataFrame with new geography_id column.
        """
        if not self._loaded:
            raise RuntimeError("Call await load_geo_cache() before add_geography_id()")

        cache = self._cache

        return df.with_columns(
            pl.col(sgc_code_col)
            .map_elements(lambda c: cache.get(c) if c else None, return_dtype=pl.String)
            .alias(geo_id_col)
        )

    def normalize(
        self,
        df: pl.DataFrame,
        geo_col: str,
        *,
        drop_unmapped: bool = True,
    ) -> pl.DataFrame:
        """
        One-shot: add sgc_code, geo_level, and geography_id columns.

        Args:
            df:           Input DataFrame.
            geo_col:      Raw geography column name.
            drop_unmapped: If True, drop rows where geography_id is null.

        Returns:
            Normalized DataFrame.
        """
        df = self.add_sgc_code(df, geo_col)
        df = self.add_geography_id(df)

        unmapped = df["geography_id"].null_count()
        total = len(df)
        if unmapped:
            log.warning(
                "unmapped_geographies",
                count=unmapped,
                total=total,
                pct=round(unmapped / total * 100, 1),
            )

        if drop_unmapped:
            df = df.filter(pl.col("geography_id").is_not_null())

        return df


# ---------------------------------------------------------------------------
# Stateless helper functions (no DB lookup required)
# ---------------------------------------------------------------------------


def clean_string_columns(df: pl.DataFrame) -> pl.DataFrame:
    """Strip whitespace from all String columns."""
    return df.with_columns(
        [
            pl.col(c).str.strip_chars()
            for c in df.columns
            if df[c].dtype == pl.String
        ]
    )


def drop_all_null_rows(df: pl.DataFrame) -> pl.DataFrame:
    """Drop rows where every column is null."""
    return df.filter(
        pl.any_horizontal([pl.col(c).is_not_null() for c in df.columns])
    )


def cast_numeric_cols(
    df: pl.DataFrame,
    columns: list[str],
    dtype: type[pl.DataType] = pl.Float64,
) -> pl.DataFrame:
    """Cast specified columns to a numeric dtype, coercing errors to null."""
    return df.with_columns(
        [pl.col(c).cast(dtype, strict=False) for c in columns if c in df.columns]
    )
