"""
tests/test_transforms/test_normalize.py — Tests for geography normalization.
"""

from __future__ import annotations

import polars as pl
import pytest

from candata_pipeline.transforms.normalize import (
    GeoNormalizer,
    cast_numeric_cols,
    clean_string_columns,
    drop_all_null_rows,
)


class TestGeoNormalizerAddSgcCode:
    """Test GeoNormalizer.add_sgc_code() — no DB required."""

    def test_canada_maps_to_01(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["Canada", "canada", "CANADA"]})
        result = norm.add_sgc_code(df, "geo")
        assert all(c == "01" for c in result["sgc_code"].to_list())

    def test_province_names_map_correctly(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["Ontario", "British Columbia", "Quebec"]})
        result = norm.add_sgc_code(df, "geo")
        codes = result["sgc_code"].to_list()
        assert codes[0] == "35"
        assert codes[1] == "59"
        assert codes[2] == "24"

    def test_province_abbreviations_map_correctly(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["ON", "BC", "AB", "QC", "MB"]})
        result = norm.add_sgc_code(df, "geo")
        codes = result["sgc_code"].to_list()
        assert codes[0] == "35"
        assert codes[1] == "59"
        assert codes[2] == "48"
        assert codes[3] == "24"
        assert codes[4] == "46"

    def test_dotted_abbreviations_map(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["Ont.", "B.C.", "Alta."]})
        result = norm.add_sgc_code(df, "geo")
        codes = result["sgc_code"].to_list()
        assert codes[0] == "35"
        assert codes[1] == "59"
        assert codes[2] == "48"

    def test_fsa_maps_to_fsa_level(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["M5V", "V6B", "T2P"]})
        result = norm.add_sgc_code(df, "geo")
        levels = result["geo_level"].to_list()
        assert all(l == "fsa" for l in levels)

    def test_unknown_geo_maps_to_null(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"geo": ["Not A Place", "XYZ123"]})
        result = norm.add_sgc_code(df, "geo")
        codes = result["sgc_code"].to_list()
        assert all(c is None for c in codes)

    def test_all_thirteen_provinces_territories(self):
        norm = GeoNormalizer()
        provinces = [
            ("Newfoundland and Labrador", "10"),
            ("Prince Edward Island", "11"),
            ("Nova Scotia", "12"),
            ("New Brunswick", "13"),
            ("Quebec", "24"),
            ("Ontario", "35"),
            ("Manitoba", "46"),
            ("Saskatchewan", "47"),
            ("Alberta", "48"),
            ("British Columbia", "59"),
            ("Yukon", "60"),
            ("Northwest Territories", "61"),
            ("Nunavut", "62"),
        ]
        names = [p[0] for p in provinces]
        expected_codes = [p[1] for p in provinces]
        df = pl.DataFrame({"geo": names})
        result = norm.add_sgc_code(df, "geo")
        assert result["sgc_code"].to_list() == expected_codes

    def test_custom_output_column_name(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"province": ["Ontario"]})
        result = norm.add_sgc_code(df, "province", sgc_col="code", level_col="lvl")
        assert "code" in result.columns
        assert "lvl" in result.columns


class TestGeoNormalizerAddGeographyId:
    def test_raises_if_cache_not_loaded(self):
        norm = GeoNormalizer()
        df = pl.DataFrame({"sgc_code": ["35"]})
        with pytest.raises(RuntimeError, match="load_geo_cache"):
            norm.add_geography_id(df)

    def test_maps_sgc_code_to_uuid(self):
        norm = GeoNormalizer()
        norm._cache = {"35": "aaaa-1111", "59": "bbbb-2222"}
        norm._loaded = True
        df = pl.DataFrame({"sgc_code": ["35", "59", "99"]})
        result = norm.add_geography_id(df)
        assert result["geography_id"][0] == "aaaa-1111"
        assert result["geography_id"][1] == "bbbb-2222"
        assert result["geography_id"][2] is None

    def test_null_sgc_code_gives_null_geo_id(self):
        norm = GeoNormalizer()
        norm._cache = {"35": "aaaa-1111"}
        norm._loaded = True
        df = pl.DataFrame({"sgc_code": [None, "35"]})
        result = norm.add_geography_id(df)
        assert result["geography_id"][0] is None
        assert result["geography_id"][1] == "aaaa-1111"

    def test_normalize_drops_unmapped_rows(self):
        norm = GeoNormalizer()
        norm._cache = {"35": "aaaa-1111"}
        norm._loaded = True
        df = pl.DataFrame({"geo": ["Ontario", "Unknown Place"]})
        result = norm.normalize(df, "geo", drop_unmapped=True)
        assert len(result) == 1
        assert result["sgc_code"][0] == "35"

    def test_normalize_keeps_unmapped_when_flag_false(self):
        norm = GeoNormalizer()
        norm._cache = {"35": "aaaa-1111"}
        norm._loaded = True
        df = pl.DataFrame({"geo": ["Ontario", "Unknown Place"]})
        result = norm.normalize(df, "geo", drop_unmapped=False)
        assert len(result) == 2


class TestHelperFunctions:
    def test_clean_string_columns_strips_whitespace(self):
        df = pl.DataFrame({"name": ["  Ontario  ", " Canada "], "value": [1.0, 2.0]})
        result = clean_string_columns(df)
        assert result["name"][0] == "Ontario"
        assert result["name"][1] == "Canada"

    def test_drop_all_null_rows(self):
        df = pl.DataFrame({"a": [1, None, 3], "b": [None, None, 5]})
        result = drop_all_null_rows(df)
        # Row 1 has a=None, b=None — should be dropped
        assert len(result) == 2

    def test_cast_numeric_cols(self):
        df = pl.DataFrame({"val": ["1.5", "2.3", "bad"]})
        result = cast_numeric_cols(df, ["val"], pl.Float64)
        assert result["val"].dtype == pl.Float64
        assert result["val"][0] == pytest.approx(1.5)
        assert result["val"][2] is None  # coercion failure → null

    def test_cast_numeric_ignores_missing_columns(self):
        df = pl.DataFrame({"val": ["1.5"]})
        # Should not raise if column doesn't exist
        result = cast_numeric_cols(df, ["val", "nonexistent"])
        assert "nonexistent" not in result.columns
