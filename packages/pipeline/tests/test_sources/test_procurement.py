"""
tests/test_sources/test_procurement.py — Unit tests for ProcurementSource.

Tests cover:
  - CKAN API metadata parsing
  - CSV extraction with CKAN discovery and fallback
  - Contract CSV parsing and transformation
  - Vendor name normalization
  - Department name normalization
  - Economic object code categorization
  - Tender API pagination and transformation
  - raw_data JSONB serialization
"""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import polars as pl
import pytest
import respx

from candata_pipeline.sources.procurement import (
    ProcurementSource,
    _CKAN_API_URL,
    _CKAN_DATASET_ID,
    _PROACTIVE_CSV_URL,
    _CANADABUYS_TENDERS_URL,
    categorize_economic_object,
    normalize_department,
    normalize_vendor,
)

FIXTURES_DIR = Path(__file__).parent.parent / "fixtures"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _sample_csv_bytes() -> bytes:
    return (FIXTURES_DIR / "procurement_contracts_sample.csv").read_bytes()


def _ckan_response(csv_urls: list[str] | None = None) -> dict:
    """Build a mock CKAN package_show response."""
    if csv_urls is None:
        csv_urls = ["https://open.canada.ca/data/contracts.csv"]
    resources = [
        {"format": "CSV", "url": url, "name": f"resource_{i}"}
        for i, url in enumerate(csv_urls)
    ]
    return {
        "success": True,
        "result": {
            "id": _CKAN_DATASET_ID,
            "title": "Proactive Disclosure of Contracts",
            "resources": resources,
        },
    }


def _tenders_page(notices: list[dict], page: int = 1) -> dict:
    return {"data": notices, "page": page, "total": len(notices)}


# ---------------------------------------------------------------------------
# Vendor name normalization
# ---------------------------------------------------------------------------

class TestNormalizeVendor:
    def test_title_case(self):
        assert normalize_vendor("ACME CONSULTING INC.") == "Acme Consulting Inc."

    def test_strips_whitespace(self):
        assert normalize_vendor("  deloitte LLP  ") == "Deloitte Llp"

    def test_collapses_internal_whitespace(self):
        assert normalize_vendor("IBM   Canada   Ltd") == "Ibm Canada Ltd"

    def test_none_returns_none(self):
        assert normalize_vendor(None) is None

    def test_empty_returns_none(self):
        assert normalize_vendor("") is None
        assert normalize_vendor("   ") is None


# ---------------------------------------------------------------------------
# Department name normalization
# ---------------------------------------------------------------------------

class TestNormalizeDepartment:
    def test_known_abbreviation(self):
        assert normalize_department("PSPC") == "Public Services and Procurement Canada"

    def test_old_name_maps_to_current(self):
        assert (
            normalize_department("Public Works and Government Services Canada")
            == "Public Services and Procurement Canada"
        )

    def test_case_insensitive(self):
        assert normalize_department("national defence") == "National Defence"
        assert normalize_department("DND") == "National Defence"

    def test_unknown_department_preserved(self):
        assert normalize_department("Some New Department") == "Some New Department"

    def test_strips_whitespace(self):
        assert normalize_department("  Health Canada  ") == "Health Canada"

    def test_none_returns_none(self):
        assert normalize_department(None) is None


# ---------------------------------------------------------------------------
# Economic object code categorization
# ---------------------------------------------------------------------------

class TestCategorizeEconomicObject:
    def test_personnel(self):
        assert categorize_economic_object("0350") == "Personnel"

    def test_professional_services(self):
        assert categorize_economic_object("3") == "Professional and Special Services"

    def test_acquisition_equipment(self):
        assert categorize_economic_object("8910") == "Acquisition of Machinery and Equipment"

    def test_other_subsidies(self):
        assert categorize_economic_object("1201") == "Other Subsidies and Payments"

    def test_transport(self):
        assert categorize_economic_object("1100") == "Transportation and Communications"

    def test_none_returns_none(self):
        assert categorize_economic_object(None) is None

    def test_empty_returns_none(self):
        assert categorize_economic_object("") is None
        assert categorize_economic_object("  ") is None


# ---------------------------------------------------------------------------
# CKAN URL extraction
# ---------------------------------------------------------------------------

class TestExtractCsvUrls:
    def test_extracts_csv_urls(self):
        source = ProcurementSource()
        result = _ckan_response(
            ["https://example.com/a.csv", "https://example.com/b.csv"]
        )["result"]
        urls = source._extract_csv_urls(result)
        assert urls == [
            "https://example.com/a.csv",
            "https://example.com/b.csv",
        ]

    def test_ignores_non_csv(self):
        source = ProcurementSource()
        result = {
            "resources": [
                {"format": "CSV", "url": "https://example.com/data.csv"},
                {"format": "JSON", "url": "https://example.com/data.json"},
                {"format": "XML", "url": "https://example.com/data.xml"},
            ]
        }
        urls = source._extract_csv_urls(result)
        assert urls == ["https://example.com/data.csv"]

    def test_empty_resources(self):
        source = ProcurementSource()
        urls = source._extract_csv_urls({"resources": []})
        assert urls == []


# ---------------------------------------------------------------------------
# Extract — contracts (mocked HTTP)
# ---------------------------------------------------------------------------

class TestExtractContracts:
    @pytest.mark.asyncio
    async def test_extract_via_ckan(self):
        source = ProcurementSource()
        csv_bytes = _sample_csv_bytes()

        with respx.mock() as router:
            # CKAN API returns metadata with one CSV URL
            router.get(url__startswith=_CKAN_API_URL).mock(
                return_value=httpx.Response(
                    200, json=_ckan_response(["https://open.canada.ca/data/contracts.csv"])
                )
            )
            # CSV download
            router.get("https://open.canada.ca/data/contracts.csv").mock(
                return_value=httpx.Response(200, content=csv_bytes)
            )

            df = await source.extract(dataset="contracts")

        assert not df.is_empty()
        assert len(df) == 7  # 7 rows in fixture

    @pytest.mark.asyncio
    async def test_extract_falls_back_on_ckan_failure(self):
        source = ProcurementSource()
        csv_bytes = _sample_csv_bytes()

        with respx.mock() as router:
            # CKAN API fails
            router.get(url__startswith=_CKAN_API_URL).mock(
                return_value=httpx.Response(500, text="Server Error")
            )
            # Fallback direct URL works
            router.get(url__startswith=_PROACTIVE_CSV_URL.split("?")[0]).mock(
                return_value=httpx.Response(200, content=csv_bytes)
            )

            df = await source.extract(dataset="contracts")

        assert not df.is_empty()

    @pytest.mark.asyncio
    async def test_extract_handles_bom(self):
        source = ProcurementSource()
        csv_bytes = b"\xef\xbb\xbf" + _sample_csv_bytes()

        with respx.mock() as router:
            router.get(url__startswith=_CKAN_API_URL).mock(
                return_value=httpx.Response(
                    200, json=_ckan_response(["https://example.com/bom.csv"])
                )
            )
            router.get("https://example.com/bom.csv").mock(
                return_value=httpx.Response(200, content=csv_bytes)
            )

            df = await source.extract(dataset="contracts")

        assert not df.is_empty()

    @pytest.mark.asyncio
    async def test_extract_multiple_csvs_concatenated(self):
        source = ProcurementSource()
        csv_bytes = _sample_csv_bytes()

        with respx.mock() as router:
            router.get(url__startswith=_CKAN_API_URL).mock(
                return_value=httpx.Response(
                    200,
                    json=_ckan_response([
                        "https://example.com/a.csv",
                        "https://example.com/b.csv",
                    ]),
                )
            )
            router.get("https://example.com/a.csv").mock(
                return_value=httpx.Response(200, content=csv_bytes)
            )
            router.get("https://example.com/b.csv").mock(
                return_value=httpx.Response(200, content=csv_bytes)
            )

            df = await source.extract(dataset="contracts")

        # 7 rows from each CSV = 14 total
        assert len(df) == 14


# ---------------------------------------------------------------------------
# Extract — tenders (mocked HTTP)
# ---------------------------------------------------------------------------

class TestExtractTenders:
    @pytest.mark.asyncio
    async def test_extract_single_page(self):
        source = ProcurementSource()
        notices = [
            {
                "reference_number": f"T-{i}",
                "title": f"Tender {i}",
                "department": "DND",
                "closing_date": "2025-06-30",
                "status": "active",
            }
            for i in range(5)
        ]

        with respx.mock() as router:
            router.get(url__startswith=_CANADABUYS_TENDERS_URL).mock(
                return_value=httpx.Response(200, json=_tenders_page(notices))
            )

            df = await source.extract(dataset="tenders", max_tenders=100)

        assert len(df) == 5

    @pytest.mark.asyncio
    async def test_extract_empty_tenders(self):
        source = ProcurementSource()

        with respx.mock() as router:
            router.get(url__startswith=_CANADABUYS_TENDERS_URL).mock(
                return_value=httpx.Response(200, json=_tenders_page([]))
            )

            df = await source.extract(dataset="tenders")

        assert df.is_empty()

    @pytest.mark.asyncio
    async def test_extract_tenders_api_error_returns_empty(self):
        source = ProcurementSource()

        with respx.mock() as router:
            router.get(url__startswith=_CANADABUYS_TENDERS_URL).mock(
                return_value=httpx.Response(500, text="Server Error")
            )

            df = await source.extract(dataset="tenders")

        assert df.is_empty()

    @pytest.mark.asyncio
    async def test_extract_tenders_403_fails_fast(self):
        """A 403 (client error) should not be retried — fail immediately."""
        source = ProcurementSource()

        call_count = 0

        def _count_and_respond(request):
            nonlocal call_count
            call_count += 1
            return httpx.Response(403, text="Forbidden")

        with respx.mock() as router:
            router.get(url__startswith=_CANADABUYS_TENDERS_URL).mock(
                side_effect=_count_and_respond
            )

            df = await source.extract(dataset="tenders")

        assert df.is_empty()
        assert call_count == 1, f"Expected 1 attempt (no retries), got {call_count}"


# ---------------------------------------------------------------------------
# Transform — contracts
# ---------------------------------------------------------------------------

class TestTransformContracts:
    def _load_and_transform(self) -> pl.DataFrame:
        source = ProcurementSource()
        raw = pl.read_csv(
            FIXTURES_DIR / "procurement_contracts_sample.csv",
            infer_schema_length=0,
        )
        return source.transform(raw, dataset="contracts")

    def test_has_expected_columns(self):
        df = self._load_and_transform()
        expected = {
            "contract_number",
            "vendor_name",
            "department",
            "description",
            "contract_value",
            "award_date",
            "start_date",
            "end_date",
            "raw_data",
            "category",
        }
        assert expected.issubset(set(df.columns))

    def test_vendor_names_title_cased(self):
        df = self._load_and_transform()
        vendors = df["vendor_name"].drop_nulls().to_list()
        for v in vendors:
            assert v == v.title(), f"Vendor not title-cased: {v!r}"

    def test_vendor_whitespace_trimmed(self):
        df = self._load_and_transform()
        vendors = df["vendor_name"].drop_nulls().to_list()
        for v in vendors:
            assert v == v.strip()
            assert "  " not in v  # no double spaces

    def test_department_normalized(self):
        df = self._load_and_transform()
        depts = df["department"].drop_nulls().to_list()
        # "Public Works and Government Services Canada" → "Public Services and Procurement Canada"
        assert "Public Services and Procurement Canada" in depts
        # "PSPC" → "Public Services and Procurement Canada"
        assert depts.count("Public Services and Procurement Canada") >= 2

    def test_contract_value_numeric(self):
        df = self._load_and_transform()
        assert df["contract_value"].dtype == pl.Float64
        non_null = df["contract_value"].drop_nulls().to_list()
        assert all(v > 0 for v in non_null)

    def test_dates_parsed(self):
        df = self._load_and_transform()
        assert df["award_date"].dtype == pl.Date
        assert df["start_date"].dtype == pl.Date
        assert df["end_date"].dtype == pl.Date

    def test_raw_data_is_json(self):
        df = self._load_and_transform()
        for row_json in df["raw_data"].to_list():
            parsed = json.loads(row_json)
            assert isinstance(parsed, dict)
            assert len(parsed) > 0  # not empty

    def test_raw_data_preserves_bilingual_fields(self):
        df = self._load_and_transform()
        # The first row should have both English and French descriptions in raw_data
        first_raw = json.loads(df["raw_data"][0])
        assert "description_en" in first_raw
        assert "description_fr" in first_raw

    def test_category_from_economic_object_code(self):
        df = self._load_and_transform()
        categories = df["category"].drop_nulls().to_list()
        assert len(categories) > 0
        # economic_object_code "0350" → "Personnel"
        assert "Personnel" in categories

    def test_empty_input_returns_empty(self):
        source = ProcurementSource()
        result = source.transform(pl.DataFrame(), dataset="contracts")
        assert result.is_empty()


# ---------------------------------------------------------------------------
# Transform — tenders
# ---------------------------------------------------------------------------

class TestTransformTenders:
    def test_maps_tender_fields(self):
        source = ProcurementSource()
        raw = pl.DataFrame({
            "reference_number": ["T-001", "T-002"],
            "title": ["Cloud services", "Network equipment"],
            "department": ["DND", "PSPC"],
            "closing_date": ["2025-06-30", "2025-07-15"],
            "status": ["active", "active"],
            "estimated_value": ["100000", "250000"],
            "category": ["IT", "Network"],
            "region": ["Ontario", "National"],
            "url": ["https://example.com/1", "https://example.com/2"],
        })
        df = source.transform(raw, dataset="tenders")

        assert "tender_number" in df.columns
        assert "title" in df.columns
        assert "department" in df.columns
        assert "closing_date" in df.columns

    def test_department_normalized_in_tenders(self):
        source = ProcurementSource()
        raw = pl.DataFrame({
            "reference_number": ["T-001"],
            "title": ["Test"],
            "department": ["DND"],
            "status": ["active"],
        })
        df = source.transform(raw, dataset="tenders")
        assert df["department"][0] == "National Defence"

    def test_empty_tenders_returns_empty(self):
        source = ProcurementSource()
        result = source.transform(pl.DataFrame(), dataset="tenders")
        assert result.is_empty()


# ---------------------------------------------------------------------------
# Metadata
# ---------------------------------------------------------------------------

class TestMetadata:
    @pytest.mark.asyncio
    async def test_get_metadata(self):
        source = ProcurementSource()
        metadata = await source.get_metadata()

        assert metadata["source_name"] == "CanadaBuys"
        assert _CKAN_DATASET_ID in metadata["ckan_dataset_id"]
