# /loci_platform/platform/airflow/dags/loci/collectors/ckan/client.py
from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import requests
from loci.collectors.ckan.metadata import CKANMetadata
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class CKANClient:
    """Downloads resources and queries DataStore endpoints on a CKAN portal."""

    def __init__(
        self,
        base_url: str,
        request_timeout: int = 120,
        page_size: int = 10000,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.request_timeout = request_timeout
        self.page_size = page_size
        self.logger = logging.getLogger("ckan_client")

        self._session = requests.Session()
        self.metadata = CKANMetadata(self.base_url)

    # ------------------------------------------------------------------
    # File downloads
    # ------------------------------------------------------------------

    @retry(
        retry=retry_if_exception_type((ChunkedEncodingError, ConnectionError, ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, max=300),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def download_to_tempfile(self, url: str, suffix: str = ".csv") -> Path:
        """Download a URL to a temporary file. Returns the file path.

        The caller is responsible for deleting the file when done.
        """
        self.logger.info("Downloading %s", url)
        resp = self._session.get(url, stream=True, timeout=self.request_timeout)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="ckan_", delete=False)
        try:
            for chunk in resp.iter_content(chunk_size=8192):
                tmp.write(chunk)
            tmp.close()
            filepath = Path(tmp.name)
            self.logger.info(
                "Downloaded %s to %s (%.1f MB)",
                url,
                filepath,
                filepath.stat().st_size / (1024 * 1024),
            )
            return filepath
        except Exception:
            tmp.close()
            Path(tmp.name).unlink(missing_ok=True)
            raise

    def _suffix_for_format(self, fmt: str) -> str:
        """Map a CKAN resource format string to a file suffix."""
        fmt_upper = (fmt or "").upper()
        mapping = {
            "CSV": ".csv",
            "GEOJSON": ".geojson",
            "JSON": ".json",
            "TSV": ".tsv",
            "XML": ".xml",
        }
        return mapping.get(fmt_upper, ".csv")

    # ------------------------------------------------------------------
    # DataStore queries
    # ------------------------------------------------------------------

    def datastore_search(
        self,
        resource_id: str,
        fields: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        sort: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a single DataStore search and return the records."""
        params: dict[str, Any] = {"resource_id": resource_id}
        if fields:
            params["fields"] = ",".join(fields)
        if filters:
            import json

            params["filters"] = json.dumps(filters)
        if sort:
            params["sort"] = sort
        if limit is not None:
            params["limit"] = limit

        return self._datastore_request(params)

    def datastore_paginate(
        self,
        resource_id: str,
        fields: list[str] | None = None,
        filters: dict[str, Any] | None = None,
        sort: str | None = None,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of records from a DataStore resource until exhausted.

        Uses offset-based pagination with the configured page_size.
        Automatically strips the internal _id field that CKAN adds.
        """
        offset = 0
        while True:
            params: dict[str, Any] = {
                "resource_id": resource_id,
                "limit": self.page_size,
                "offset": offset,
            }
            if fields:
                params["fields"] = ",".join(fields)
            if filters:
                import json

                params["filters"] = json.dumps(filters)
            if sort:
                params["sort"] = sort

            records = self._datastore_request(params)
            if not records:
                break

            # Strip CKAN's internal _id column
            for row in records:
                row.pop("_id", None)

            yield records

            if len(records) < self.page_size:
                break
            offset += self.page_size

    @retry(
        retry=retry_if_exception_type((ConnectionError, ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _datastore_request(self, params: dict[str, Any]) -> list[dict[str, Any]]:
        """Make a single DataStore search request and return the records."""
        url = f"{self.metadata.api_base}/datastore_search"
        self.logger.debug("GET %s  params=%s", url, params)

        resp = self._session.get(url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()
        data = resp.json()

        if not data.get("success"):
            raise ValueError(f"DataStore search failed: {data.get('error', data)}")

        return data["result"].get("records", [])
