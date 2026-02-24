"""
sources/opencanada.py — Generic CKAN API client for open.canada.ca.

Wraps the open.canada.ca CKAN data portal API so any pipeline module
can search for or download datasets without reimplementing HTTP boilerplate.

CKAN API base: https://open.canada.ca/data/api/3

Key actions:
  /action/package_search     — search for datasets by keyword / organization
  /action/package_show       — metadata for one dataset by id or slug
  /action/resource_show      — metadata for a specific resource (file) by id
  /action/datastore_search   — paginated row-level access to tabular resources

Usage:
    client = OpenCanadaSource()

    # Search for procurement datasets
    packages = await client.search_packages("proactive disclosure contracts", rows=10)

    # Get all resources for a known dataset
    resources = await client.get_resources("d8f85d91-7dec-4fd1-8055-483b77225d8b")

    # Stream a CSV resource as polars DataFrame (large files)
    df = await client.download_resource_csv(resource_id)

    # Query the CKAN Datastore (for datasets with tabular indexing)
    df = await client.datastore_search(resource_id, filters={"year": "2023"}, limit=1000)
"""

from __future__ import annotations

import io
from typing import Any

import httpx
import polars as pl
import structlog

from candata_pipeline.sources.base import BaseSource
from candata_pipeline.utils.retry import with_retry

log = structlog.get_logger(__name__)

CKAN_BASE = "https://open.canada.ca/data/api/3"


class OpenCanadaSource(BaseSource):
    """Generic CKAN client for the open.canada.ca data portal."""

    name = "OpenCanada"

    def __init__(self, timeout: float = 60.0) -> None:
        super().__init__()
        self._timeout = timeout

    # ------------------------------------------------------------------
    # Low-level CKAN API calls
    # ------------------------------------------------------------------

    @with_retry(max_attempts=3, base_delay=1.0, retry_on=(httpx.HTTPError,))
    async def _ckan_action(
        self,
        action: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Call a CKAN action endpoint and return the result dict."""
        url = f"{CKAN_BASE}/action/{action}"
        self._log.debug("ckan_action", action=action, params=params)
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.get(url, params=params or {})
            response.raise_for_status()
            data = response.json()
            if not data.get("success"):
                raise ValueError(f"CKAN action {action} returned success=false: {data.get('error')}")
            return data["result"]  # type: ignore[no-any-return]

    @with_retry(max_attempts=3, base_delay=2.0, retry_on=(httpx.HTTPError,))
    async def _download_url(self, url: str) -> bytes:
        """Download raw bytes from any URL (for CSV resources)."""
        self._log.info("resource_download", url=url)
        async with httpx.AsyncClient(
            timeout=self._timeout, follow_redirects=True
        ) as client:
            response = await client.get(url)
            response.raise_for_status()
            return response.content

    # ------------------------------------------------------------------
    # High-level helpers
    # ------------------------------------------------------------------

    async def search_packages(
        self,
        query: str,
        rows: int = 20,
        organization: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Search the open.canada.ca catalog.

        Args:
            query:        Free-text search query.
            rows:         Maximum number of results.
            organization: Filter by publishing organization slug.

        Returns:
            List of CKAN package dicts.
        """
        params: dict[str, Any] = {"q": query, "rows": rows}
        if organization:
            params["fq"] = f"organization:{organization}"
        result = await self._ckan_action("package_search", params)
        return result.get("results", [])

    async def get_resources(self, package_id: str) -> list[dict[str, Any]]:
        """
        Return all resource metadata for a given dataset.

        Args:
            package_id: CKAN package id or slug.

        Returns:
            List of resource dicts with 'id', 'url', 'format', 'name'.
        """
        result = await self._ckan_action("package_show", {"id": package_id})
        return result.get("resources", [])

    async def download_resource_csv(
        self,
        resource_id: str,
        *,
        encoding: str = "utf-8-sig",
    ) -> pl.DataFrame:
        """
        Download a CSV resource by id and return a polars DataFrame.

        Args:
            resource_id: CKAN resource UUID.
            encoding:    File encoding (utf-8-sig strips BOM).

        Returns:
            polars DataFrame with all columns as Strings.
        """
        resource = await self._ckan_action("resource_show", {"id": resource_id})
        url = resource.get("url", "")
        if not url:
            raise ValueError(f"Resource {resource_id} has no download URL")

        raw = await self._download_url(url)
        # Decode with specified encoding (handles Windows-1252, UTF-8-BOM etc.)
        text = raw.decode(encoding, errors="replace")
        return pl.read_csv(io.StringIO(text), infer_schema_length=0, truncate_ragged_lines=True)

    async def datastore_search(
        self,
        resource_id: str,
        filters: dict[str, Any] | None = None,
        limit: int = 10_000,
        offset: int = 0,
    ) -> pl.DataFrame:
        """
        Query the CKAN Datastore for a tabular resource.

        Handles pagination automatically up to `limit` rows.

        Args:
            resource_id: CKAN resource UUID with datastore enabled.
            filters:     Dict of exact-match filters (e.g. {"year": "2023"}).
            limit:       Maximum rows to return.
            offset:      Row offset.

        Returns:
            polars DataFrame.
        """
        PAGE_SIZE = 1000
        all_records: list[dict[str, Any]] = []
        current_offset = offset

        while True:
            params: dict[str, Any] = {
                "resource_id": resource_id,
                "limit": min(PAGE_SIZE, limit - len(all_records)),
                "offset": current_offset,
            }
            if filters:
                import json
                params["filters"] = json.dumps(filters)

            result = await self._ckan_action("datastore_search", params)
            records = result.get("records", [])
            all_records.extend(records)

            total = result.get("total", 0)
            current_offset += len(records)
            self._log.debug("datastore_page", fetched=len(all_records), total=total)

            if not records or len(all_records) >= limit or current_offset >= total:
                break

        if not all_records:
            return pl.DataFrame()

        return pl.from_dicts(all_records)

    # ------------------------------------------------------------------
    # BaseSource interface (generic — callers use high-level helpers)
    # ------------------------------------------------------------------

    async def extract(
        self,
        *,
        resource_id: str | None = None,
        package_id: str | None = None,
        query: str | None = None,
        **kwargs: Any,
    ) -> pl.DataFrame:
        """
        Generic extract: download a resource CSV or run a datastore query.
        """
        if resource_id:
            return await self.download_resource_csv(resource_id)
        if package_id:
            resources = await self.get_resources(package_id)
            csv_resources = [r for r in resources if r.get("format", "").upper() == "CSV"]
            if not csv_resources:
                raise ValueError(f"No CSV resources found for package {package_id}")
            return await self.download_resource_csv(csv_resources[0]["id"])
        if query:
            packages = await self.search_packages(query)
            if not packages:
                return pl.DataFrame()
            return pl.from_dicts(packages)
        raise ValueError("Provide resource_id, package_id, or query")

    def transform(self, raw: pl.DataFrame) -> pl.DataFrame:
        """Identity transform — callers apply domain-specific transforms."""
        return self._normalize_columns(raw)

    async def get_metadata(self) -> dict[str, Any]:
        return {
            "source_name": self.name,
            "base_url": CKAN_BASE,
            "description": "open.canada.ca CKAN API",
        }
