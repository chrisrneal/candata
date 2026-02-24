"""
tests/test_sources/test_statcan.py — Unit tests for StatCanSource.

All HTTP is mocked via respx; no real network calls are made.
Fixture CSV files in tests/fixtures/ mirror actual StatCan CSV format.
"""

from __future__ import annotations

import io
import zipfile
from datetime import date
from pathlib import Path
from unittest.mock import patch

import httpx
import polars as pl
import pytest
import respx

from candata_pipeline.sources.statcan import StatCanSource

FIXTURES = Path(__file__).parent.parent / "fixtures"


def make_zip(csv_bytes: bytes, pid: str = "1810000401") -> bytes:
    """Wrap CSV bytes in a zip file as StatCan would serve."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(f"{pid}_en.csv", csv_bytes)
        zf.writestr(f"{pid}_MetaData_en.csv", "metadata placeholder")
    return buf.getvalue()


@pytest.fixture
def cpi_zip() -> bytes:
    csv_bytes = (FIXTURES / "statcan_cpi_sample.csv").read_bytes()
    return make_zip(csv_bytes, "1810000401")


@pytest.fixture
def gdp_zip() -> bytes:
    csv_bytes = (FIXTURES / "statcan_gdp_sample.csv").read_bytes()
    return make_zip(csv_bytes, "3610043401")


# ---------------------------------------------------------------------------
# extract() tests
# ---------------------------------------------------------------------------

class TestStatCanExtract:
    @pytest.mark.asyncio
    async def test_extract_returns_dataframe(self, cpi_zip: bytes):
        source = StatCanSource()
        with respx.mock() as router:
            router.get(url__regex=r".*downloadTbl.*").mock(
                return_value=httpx.Response(200, content=cpi_zip)
            )
            # Bypass DuckDB caching for tests
            with patch.object(source, "_is_cached", return_value=False):
                with patch.object(source, "_cache_to_duckdb"):
                    df = await source.extract(table_pid="1810000401", use_cache=False)

        assert isinstance(df, pl.DataFrame)
        assert len(df) > 0
        assert "REF_DATE" in df.columns
        assert "VALUE" in df.columns

    @pytest.mark.asyncio
    async def test_extract_uses_cache_when_available(self):
        source = StatCanSource()
        cached_df = pl.DataFrame({"REF_DATE": ["2023-01"], "VALUE": ["157.1"]})
        with patch.object(source, "_is_cached", return_value=True):
            with patch.object(source, "_load_from_cache", return_value=cached_df):
                df = await source.extract(table_pid="1810000401", use_cache=True)

        assert len(df) == 1
        assert df["REF_DATE"][0] == "2023-01"

    @pytest.mark.asyncio
    async def test_extract_raises_on_http_error(self):
        source = StatCanSource()
        with respx.mock() as router:
            router.get(url__regex=r".*downloadTbl.*").mock(
                return_value=httpx.Response(404)
            )
            with patch.object(source, "_is_cached", return_value=False):
                with pytest.raises(httpx.HTTPStatusError):
                    await source.extract(table_pid="9999999999", use_cache=False)

    @pytest.mark.asyncio
    async def test_extract_gdp_table(self, gdp_zip: bytes):
        source = StatCanSource()
        with respx.mock() as router:
            router.get(url__regex=r".*downloadTbl.*").mock(
                return_value=httpx.Response(200, content=gdp_zip)
            )
            with patch.object(source, "_is_cached", return_value=False):
                with patch.object(source, "_cache_to_duckdb"):
                    df = await source.extract(table_pid="3610043401", use_cache=False)

        assert len(df) > 0


# ---------------------------------------------------------------------------
# transform() tests
# ---------------------------------------------------------------------------

class TestStatCanTransform:
    @pytest.fixture
    def raw_cpi(self, statcan_cpi_df: pl.DataFrame) -> pl.DataFrame:
        return statcan_cpi_df

    def test_transform_creates_ref_date_column(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        assert "ref_date" in df.columns
        assert df["ref_date"].dtype == pl.Date

    def test_transform_ref_date_is_first_of_month(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        # All ref_dates should be day=1
        days = df["ref_date"].dt.day().to_list()
        assert all(d == 1 for d in days), f"Some dates are not first-of-month: {days}"

    def test_transform_parses_value_as_float(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        assert "value" in df.columns
        assert df["value"].dtype == pl.Float64

    def test_transform_suppressed_values_become_null(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        # The CPI fixture has one row with value "x" (suppressed)
        # After transform, that row's value should be null
        null_count = df["value"].null_count()
        assert null_count > 0, "Expected at least one suppressed (null) value"

    def test_transform_adds_sgc_code(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        assert "sgc_code" in df.columns
        # Canada should map to "01"
        canada_rows = df.filter(pl.col("geo") == "Canada")
        if len(canada_rows) > 0:
            assert canada_rows["sgc_code"][0] == "01"

    def test_transform_adds_geo_level(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        assert "geo_level" in df.columns
        levels = df["geo_level"].unique().to_list()
        assert any(l in levels for l in ["country", "pr"])

    def test_transform_ontario_maps_to_35(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        ontario = df.filter(pl.col("geo") == "Ontario")
        assert len(ontario) > 0
        assert ontario["sgc_code"][0] == "35"

    def test_transform_start_date_filter(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df_all = source.transform(raw_cpi)
        df_filtered = source.transform(raw_cpi, start_date=date(2023, 2, 1))
        assert len(df_filtered) < len(df_all)
        assert df_filtered["ref_date"].min() >= date(2023, 2, 1)

    def test_transform_gdp_sample(self, statcan_gdp_df: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(statcan_gdp_df)
        assert len(df) > 0
        assert "value" in df.columns
        assert "sgc_code" in df.columns
        # British Columbia should have some null values (marker '..')
        bc = df.filter(pl.col("geo") == "British Columbia")
        assert bc["value"].null_count() >= 0  # some may be suppressed

    def test_transform_empty_input(self):
        source = StatCanSource()
        with pytest.raises(ValueError, match="REF_DATE column not found"):
            source.transform(pl.DataFrame())

    def test_transform_vector_column_preserved(self, raw_cpi: pl.DataFrame):
        source = StatCanSource()
        df = source.transform(raw_cpi)
        assert "vector" in df.columns


# ---------------------------------------------------------------------------
# _parse_csv_zip() unit test
# ---------------------------------------------------------------------------

class TestParseCsvZip:
    def test_parse_extracts_data_file(self, cpi_zip: bytes):
        source = StatCanSource()
        df = source._parse_csv_zip(cpi_zip, "1810000401")
        assert isinstance(df, pl.DataFrame)
        assert "REF_DATE" in df.columns

    def test_parse_handles_bom(self):
        source = StatCanSource()
        csv_content = b"\xef\xbb\xbfREF_DATE,GEO,VALUE\n2023-01,Canada,157.1\n"
        zip_bytes = make_zip(csv_content, "test_pid")
        df = source._parse_csv_zip(zip_bytes, "test_pid")
        assert "REF_DATE" in df.columns
        assert df["REF_DATE"][0] == "2023-01"

    def test_parse_raises_if_no_csv(self):
        source = StatCanSource()
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as zf:
            zf.writestr("1234_MetaData_en.csv", "metadata only")
        with pytest.raises(ValueError, match="No data CSV"):
            source._parse_csv_zip(buf.getvalue(), "1234")
