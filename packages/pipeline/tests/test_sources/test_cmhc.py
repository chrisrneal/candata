"""
tests/test_sources/test_cmhc.py — Unit tests for CMHCSource.

Tests cover:
  - CMHC geo ID -> SGC code mapping
  - Bedroom and dwelling type normalization
  - StatCan CSV parsing and transformation
  - GEO name -> SGC code extraction
  - Extract method (mocked HTTP)
"""

from __future__ import annotations

import io
import zipfile
from datetime import date
from unittest.mock import AsyncMock, patch

import httpx
import polars as pl
import pytest
import respx

from candata_pipeline.sources.cmhc import (
    CMHC_GEO_NAMES,
    CMHC_GEO_TO_SGC,
    CMA_NAME_TO_CMHC,
    CMHCSource,
    _BEDROOM_NORMALIZE,
    _DWELLING_NORMALIZE,
    _extract_sgc_from_geo,
    normalize_bedroom,
    normalize_dwelling,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_statcan_zip(csv_text: str, table_id: str = "34100127") -> bytes:
    """Create a StatCan-style ZIP containing a data CSV."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(f"{table_id}.csv", csv_text)
        zf.writestr(f"{table_id}_MetaData.csv", "metadata")
    return buf.getvalue()


VACANCY_CSV = """\
REF_DATE,GEO,DGUID,UOM,UOM_ID,SCALAR_FACTOR,SCALAR_ID,VECTOR,COORDINATE,VALUE,STATUS,SYMBOL,TERMINATED,DECIMALS
2023,"Toronto, Ontario",2011S0503535,Percentage,239,units,0,v401234,1.1,1.5,,,,1
2022,"Toronto, Ontario",2011S0503535,Percentage,239,units,0,v401234,1.1,2.1,,,,1
2023,"Vancouver, British Columbia",2011S0509933,Percentage,239,units,0,v401235,1.1,0.9,,,,1
2023,"Unknown City, Ontario",2011S050XXXX,Percentage,239,units,0,v401299,1.1,3.0,,,,1
"""

RENTS_CSV = """\
REF_DATE,GEO,DGUID,Type of structure,Type of unit,UOM,UOM_ID,SCALAR_FACTOR,SCALAR_ID,VECTOR,COORDINATE,VALUE,STATUS,SYMBOL,TERMINATED,DECIMALS
2023,"Toronto, Ontario",2011S0503535,Row and apartment structures combined,Bachelor units,Dollars,81,units,0,v402000,1.1,1450.0,,,,0
2023,"Toronto, Ontario",2011S0503535,Row and apartment structures combined,One bedroom units,Dollars,81,units,0,v402001,1.2,1750.0,,,,0
2023,"Toronto, Ontario",2011S0503535,Row and apartment structures combined,Two bedroom units,Dollars,81,units,0,v402002,1.3,2200.0,,,,0
2023,"Toronto, Ontario",2011S0503535,Row and apartment structures combined,Three bedroom units and over,Dollars,81,units,0,v402003,1.4,2800.0,,,,0
2023,"Toronto, Ontario",2011S0503535,Row and apartment structures combined,Total units,Dollars,81,units,0,v402004,1.5,1950.0,,,,0
"""

STARTS_CSV = """\
REF_DATE,GEO,DGUID,Type of dwelling unit,Type of market,UOM,UOM_ID,SCALAR_FACTOR,SCALAR_ID,VECTOR,COORDINATE,VALUE,STATUS,SYMBOL,TERMINATED,DECIMALS
2023-06,"Toronto, Ontario",2011S0503535,Total units,Homeowner,Units,213,units,0,v403000,1.1,500,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Total units,Rental,Units,213,units,0,v403001,1.2,300,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Total units,Condo,Units,213,units,0,v403002,1.3,200,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Single units,Homeowner,Units,213,units,0,v403003,1.4,100,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Single units,Rental,Units,213,units,0,v403004,1.5,50,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Apartment and other types of units,Homeowner,Units,213,units,0,v403005,1.6,200,,,,0
2023-06,"Toronto, Ontario",2011S0503535,Apartment and other types of units,Rental,Units,213,units,0,v403006,1.7,150,,,,0
"""


# ---------------------------------------------------------------------------
# Geo mapping tests
# ---------------------------------------------------------------------------

class TestGeoMapping:
    def test_cmhc_geo_to_sgc_has_top_10_cmas(self):
        expected_sgcs = {"535", "462", "933", "825", "835", "505", "602", "421", "537", "205"}
        actual_sgcs = set(CMHC_GEO_TO_SGC.values())
        assert expected_sgcs.issubset(actual_sgcs)

    def test_cmhc_geo_names_matches_geo_to_sgc(self):
        for geo_id in CMHC_GEO_TO_SGC:
            assert geo_id in CMHC_GEO_NAMES, f"CMHC geo ID {geo_id} missing from CMHC_GEO_NAMES"

    def test_cma_name_lookup_toronto(self):
        assert CMA_NAME_TO_CMHC["toronto"] == 2270

    def test_cma_name_lookup_aliases(self):
        assert CMA_NAME_TO_CMHC["montreal"] == 2480
        assert CMA_NAME_TO_CMHC["ottawa"] == 1680
        assert CMA_NAME_TO_CMHC["quebec city"] == 2020

    def test_toronto_maps_to_sgc_535(self):
        assert CMHC_GEO_TO_SGC[2270] == "535"

    def test_vancouver_maps_to_sgc_933(self):
        assert CMHC_GEO_TO_SGC[2410] == "933"

    def test_no_duplicate_sgc_codes(self):
        sgc_values = list(CMHC_GEO_TO_SGC.values())
        assert len(sgc_values) == len(set(sgc_values)), "Duplicate SGC codes found"


# ---------------------------------------------------------------------------
# GEO name -> SGC code extraction
# ---------------------------------------------------------------------------

class TestExtractSgcFromGeo:
    def test_toronto(self):
        assert _extract_sgc_from_geo("Toronto, Ontario") == "535"

    def test_vancouver(self):
        assert _extract_sgc_from_geo("Vancouver, British Columbia") == "933"

    def test_montreal_with_accent(self):
        assert _extract_sgc_from_geo("Montréal, Quebec") == "462"

    def test_ottawa_gatineau(self):
        result = _extract_sgc_from_geo("Ottawa-Gatineau, Ontario/Quebec")
        assert result == "505"

    def test_unknown_returns_none(self):
        assert _extract_sgc_from_geo("Podunk, Saskatchewan") is None

    def test_city_only(self):
        assert _extract_sgc_from_geo("Toronto") == "535"


# ---------------------------------------------------------------------------
# Bedroom type normalization
# ---------------------------------------------------------------------------

class TestBedroomNormalization:
    @pytest.mark.parametrize("raw, expected", [
        ("bachelor", "bachelor"),
        ("Bach.", "bachelor"),
        ("Studio", "bachelor"),
        ("Bachelor units", "bachelor"),
        ("1 bedroom", "1br"),
        ("1-bedroom", "1br"),
        ("One bedroom units", "1br"),
        ("2 Bedrooms", "2br"),
        ("2-bedroom", "2br"),
        ("Two bedroom units", "2br"),
        ("3 Bedrooms +", "3br+"),
        ("3-bedroom+", "3br+"),
        ("Three bedroom units and over", "3br+"),
        ("Total", "total"),
        ("All", "total"),
        ("All bedroom types", "total"),
        ("Total units", "total"),
    ])
    def test_bedroom_normalize(self, raw: str, expected: str):
        assert normalize_bedroom(raw) == expected

    def test_bedroom_normalize_none(self):
        assert normalize_bedroom(None) is None

    def test_bedroom_normalize_unknown(self):
        assert normalize_bedroom("penthouse") is None

    def test_bedroom_normalize_int(self):
        """normalize_bedroom should handle non-string input gracefully."""
        assert normalize_bedroom(1) == "1br"
        assert normalize_bedroom(2) == "2br"


# ---------------------------------------------------------------------------
# Dwelling type normalization
# ---------------------------------------------------------------------------

class TestDwellingNormalization:
    @pytest.mark.parametrize("raw, expected", [
        ("single-detached", "single"),
        ("Single Detached", "single"),
        ("semi-detached", "semi"),
        ("row", "row"),
        ("Row Housing", "row"),
        ("apartment", "apartment"),
        ("Apartment and Other", "apartment"),
        ("Total", "total"),
        ("All Types", "total"),
        ("Total units", "total"),
    ])
    def test_dwelling_normalize(self, raw: str, expected: str):
        assert normalize_dwelling(raw) == expected

    def test_dwelling_normalize_none(self):
        assert normalize_dwelling(None) is None


# ---------------------------------------------------------------------------
# StatCan CSV parsing and transformation
# ---------------------------------------------------------------------------

class TestStatCanParsing:
    def test_extract_csv_from_zip(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        df = source._extract_csv_from_zip(zip_bytes, "34100127")

        assert not df.is_empty()
        assert "REF_DATE" in df.columns
        assert "GEO" in df.columns
        assert "VALUE" in df.columns

    def test_extract_csv_from_zip_skips_metadata(self):
        source = CMHCSource()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("34100127_MetaData.csv", "meta,data\n1,2")
            zf.writestr("34100127.csv", VACANCY_CSV)
        df = source._extract_csv_from_zip(buf.getvalue(), "34100127")
        assert not df.is_empty()

    def test_extract_csv_from_zip_empty_returns_empty(self):
        source = CMHCSource()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("readme.txt", "no csv here")
        df = source._extract_csv_from_zip(buf.getvalue(), "34100127")
        assert df.is_empty()

    def test_filter_cma_rows_maps_sgc_codes(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)

        assert "sgc_code" in filtered.columns
        sgc_codes = set(filtered["sgc_code"].to_list())
        assert "535" in sgc_codes  # Toronto
        assert "933" in sgc_codes  # Vancouver

    def test_filter_cma_rows_by_geo_ids(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, [2270])  # Toronto only

        sgc_codes = set(filtered["sgc_code"].to_list())
        assert sgc_codes == {"535"}

    def test_filter_cma_rows_drops_unknown_geos(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)

        # "Unknown City" should be dropped
        assert len(filtered) < len(raw_df)


class TestTransformVacancy:
    def test_vacancy_has_correct_columns(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_vacancy(filtered)

        assert "sgc_code" in result.columns
        assert "ref_date" in result.columns
        assert "bedroom_type" in result.columns
        assert "vacancy_rate" in result.columns

    def test_vacancy_rate_is_float(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_vacancy(filtered)

        assert result["vacancy_rate"].dtype == pl.Float64

    def test_vacancy_all_total_bedroom_type(self):
        """Vacancy table 34-10-0127 only has totals, no bedroom breakdown."""
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_vacancy(filtered)

        assert set(result["bedroom_type"].unique().to_list()) == {"total"}

    def test_vacancy_ref_date_parsed(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100127")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_vacancy(filtered)

        assert result["ref_date"].dtype == pl.Date
        dates = result["ref_date"].to_list()
        assert any(d.year == 2023 for d in dates)


class TestTransformRents:
    def test_rents_has_correct_columns(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(RENTS_CSV, "34100133")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100133")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_rents(filtered)

        assert "sgc_code" in result.columns
        assert "ref_date" in result.columns
        assert "bedroom_type" in result.columns
        assert "average_rent" in result.columns

    def test_rents_bedroom_types_normalized(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(RENTS_CSV, "34100133")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100133")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_rents(filtered)

        valid_types = {"bachelor", "1br", "2br", "3br+", "total"}
        actual = set(result["bedroom_type"].to_list())
        assert actual.issubset(valid_types)

    def test_rents_average_rent_numeric(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(RENTS_CSV, "34100133")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100133")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_rents(filtered)

        assert result["average_rent"].dtype == pl.Float64
        assert all(r > 0 for r in result["average_rent"].to_list())


class TestTransformStarts:
    def test_starts_has_correct_columns(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(STARTS_CSV, "34100148")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100148")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_starts(filtered)

        assert "sgc_code" in result.columns
        assert "ref_date" in result.columns
        assert "dwelling_type" in result.columns
        assert "units" in result.columns

    def test_starts_aggregates_market_types(self):
        """Units should be summed across market types (Homeowner, Rental, Condo)."""
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(STARTS_CSV, "34100148")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100148")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_starts(filtered)

        total_row = result.filter(pl.col("dwelling_type") == "total")
        assert len(total_row) == 1
        # 500 + 300 + 200 = 1000 (homeowner + rental + condo for total units)
        assert total_row["units"][0] == 1000

    def test_starts_dwelling_types_normalized(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(STARTS_CSV, "34100148")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100148")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_starts(filtered)

        valid_types = {"single", "semi", "row", "apartment", "total"}
        actual = set(result["dwelling_type"].to_list())
        assert actual.issubset(valid_types)

    def test_starts_monthly_date_parsed(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(STARTS_CSV, "34100148")
        raw_df = source._extract_csv_from_zip(zip_bytes, "34100148")
        filtered = source._filter_cma_rows(raw_df, None)
        result = source._transform_statcan_starts(filtered)

        assert result["ref_date"].dtype == pl.Date
        dates = result["ref_date"].to_list()
        assert any(d == date(2023, 6, 1) for d in dates)


# ---------------------------------------------------------------------------
# Extract (mocked HTTP)
# ---------------------------------------------------------------------------

class TestExtract:
    @pytest.mark.asyncio
    async def test_extract_vacancy_rates(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")

        with respx.mock() as router:
            router.get(url__regex=r".*34100127.*").mock(
                return_value=httpx.Response(200, content=zip_bytes)
            )
            df = await source.extract(
                dataset="vacancy_rates",
                cmhc_geo_ids=[2270],  # Toronto
            )

        assert not df.is_empty()
        assert "sgc_code" in df.columns
        assert "vacancy_rate" in df.columns
        sgc_codes = set(df["sgc_code"].to_list())
        assert sgc_codes == {"535"}

    @pytest.mark.asyncio
    async def test_extract_average_rents(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(RENTS_CSV, "34100133")

        with respx.mock() as router:
            router.get(url__regex=r".*34100133.*").mock(
                return_value=httpx.Response(200, content=zip_bytes)
            )
            df = await source.extract(
                dataset="average_rents",
                cmhc_geo_ids=[2270],
            )

        assert not df.is_empty()
        assert "average_rent" in df.columns
        assert "bedroom_type" in df.columns

    @pytest.mark.asyncio
    async def test_extract_housing_starts(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(STARTS_CSV, "34100148")

        with respx.mock() as router:
            router.get(url__regex=r".*34100148.*").mock(
                return_value=httpx.Response(200, content=zip_bytes)
            )
            df = await source.extract(
                dataset="housing_starts",
                cmhc_geo_ids=[2270],
            )

        assert not df.is_empty()
        assert "dwelling_type" in df.columns
        assert "units" in df.columns

    @pytest.mark.asyncio
    async def test_extract_filters_by_start_date(self):
        source = CMHCSource()
        zip_bytes = _make_statcan_zip(VACANCY_CSV, "34100127")

        with respx.mock() as router:
            router.get(url__regex=r".*34100127.*").mock(
                return_value=httpx.Response(200, content=zip_bytes)
            )
            df = await source.extract(
                dataset="vacancy_rates",
                start_date=date(2023, 1, 1),
            )

        # Only 2023 data should survive
        if not df.is_empty():
            dates = df["ref_date"].to_list()
            assert all(d.year >= 2023 for d in dates)

    @pytest.mark.asyncio
    async def test_extract_http_error_returns_empty(self):
        source = CMHCSource()

        with respx.mock() as router:
            router.get(url__regex=r".*34100127.*").mock(
                return_value=httpx.Response(500, text="Server Error")
            )
            df = await source.extract(
                dataset="vacancy_rates",
                cmhc_geo_ids=[2270],
            )

        assert df.is_empty()


# ---------------------------------------------------------------------------
# Transform (passthrough)
# ---------------------------------------------------------------------------

class TestTransform:
    def test_transform_returns_input(self):
        source = CMHCSource()
        df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
        result = source.transform(df, dataset="vacancy_rates")
        assert result.equals(df)

    def test_transform_empty(self):
        source = CMHCSource()
        result = source.transform(pl.DataFrame(), dataset="vacancy_rates")
        assert result.is_empty()


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

class TestMetadata:
    @pytest.mark.asyncio
    async def test_get_metadata(self):
        source = CMHCSource()
        metadata = await source.get_metadata()

        assert metadata["source_name"] == "CMHC"
        assert "vacancy_rates" in metadata["datasets"]
        assert "average_rents" in metadata["datasets"]
        assert "housing_starts" in metadata["datasets"]
        assert metadata["n_cmas"] == len(CMHC_GEO_TO_SGC)
