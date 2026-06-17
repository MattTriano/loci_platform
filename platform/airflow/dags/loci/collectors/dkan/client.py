# loci_platform/platform/airflow/dags/loci/collectors/dkan/client.py
"""
DKANClient — thin HTTP client for a DKAN data portal.

One client instance per portal; the portal is identified by base_url.
Known CMS DKAN portals:

    https://data.cms.gov/provider-data     (Provider Data Catalog)
    https://openpaymentsdata.cms.gov       (Open Payments)

Knows three things:
  1. how to fetch and cache the metastore dataset catalog
  2. how to page rows out of the datastore query API
  3. how to stream large files (CSV downloads) to a tempfile, with
     retry on mid-stream connection failures

Usage:
    from loci.collectors.dkan.client import DKANClient

    client = DKANClient("https://data.cms.gov/provider-data")
    catalog = client.get_catalog()
    n = client.row_count("xubh-q36u")
    for page in client.iter_pages("xubh-q36u"):
        ...
"""

from __future__ import annotations

import logging
import tempfile
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import requests
from requests.adapters import HTTPAdapter
from requests.exceptions import ChunkedEncodingError, ConnectionError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)
from urllib3.util.retry import Retry

logger = logging.getLogger(__name__)


class DKANClient:
    """
    Thin requests wrapper for a DKAN portal.

    Parameters
    ----------
    base_url : str
        Root URL of the portal (e.g. "https://data.cms.gov/provider-data").
    timeout : int
        Per-request timeout in seconds. Default 120.
    page_size : int
        Default rows per page for iter_pages. DKAN's datastore caps
        this at 500. Default 500.
    """

    def __init__(self, base_url: str, timeout: int = 120, page_size: int = 500):
        self.base_url = base_url.rstrip("/")
        self.api_base = f"{self.base_url}/api/1"
        self.timeout = timeout
        self.page_size = page_size
        self.logger = logging.getLogger("dkan_client")
        self._catalog: list[dict] | None = None

        self.session = requests.Session()
        http_retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=http_retry))

    def _get_json(self, url: str, params: dict | None = None):
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # -- metastore (catalog) ----------------------------------------------

    def get_catalog(self, refresh: bool = False) -> list[dict]:
        """
        Return the portal's dataset entries from the metastore.

        Cached after the first call; pass refresh=True to re-fetch.
        """
        if self._catalog is None or refresh:
            self._catalog = self._get_json(f"{self.api_base}/metastore/schemas/dataset/items")
        return self._catalog

    def get_dataset(self, identifier: str) -> dict:
        """Return one dataset's metastore entry by identifier."""
        return self._get_json(f"{self.api_base}/metastore/schemas/dataset/items/{identifier}")

    # -- datastore ----------------------------------------------------------

    def query(
        self,
        dataset_id: str,
        index: int = 0,
        limit: int | None = None,
        offset: int = 0,
        count: bool = False,
    ) -> dict:
        """
        Run a datastore query against distribution `index` of a dataset.

        Returns the raw response dict (keys typically include "results",
        and "count" when requested).
        """
        params: dict[str, Any] = {"offset": offset, "count": str(count).lower()}
        if limit is not None:
            params["limit"] = limit
        return self._get_json(
            f"{self.api_base}/datastore/query/{dataset_id}/{index}", params=params
        )

    def get_page(self, dataset_id: str, size: int, offset: int, index: int = 0) -> list[dict]:
        """Return one page of rows (list of column-name-keyed dicts)."""
        response = self.query(dataset_id, index=index, limit=size, offset=offset)
        # Be tolerant of the exact response shape: rows live under
        # "results" on current DKAN; fall back to a bare list.
        if isinstance(response, list):
            return response
        return response.get("results", [])

    def iter_pages(
        self, dataset_id: str, size: int | None = None, index: int = 0
    ) -> Iterator[list[dict]]:
        """
        Yield pages of rows for a dataset's distribution until exhausted.

        Stops after the first empty or short page.
        """
        size = size or self.page_size
        offset = 0
        while True:
            page = self.get_page(dataset_id, size=size, offset=offset, index=index)
            if not page:
                return
            yield page
            if len(page) < size:
                return
            offset += size

    def row_count(self, dataset_id: str, index: int = 0) -> int:
        """Return the total row count for a dataset's distribution."""
        response = self.query(dataset_id, index=index, limit=1, count=True)
        if isinstance(response, dict) and "count" in response:
            return int(response["count"])
        raise ValueError(f"Could not find a row count in datastore response: {response!r:.500}")

    def datastore_csv_url(self, dataset_id: str, index: int = 0) -> str:
        """The full-distribution CSV download endpoint for a datastore."""
        return f"{self.api_base}/datastore/query/{dataset_id}/{index}/download?format=csv"

    # -- file downloads -----------------------------------------------------

    @retry(
        retry=retry_if_exception_type((ChunkedEncodingError, ConnectionError, ReadTimeout)),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=30, max=300),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def download_to_tempfile(self, url: str, suffix: str = ".csv") -> Path:
        """
        Download a URL to a temporary file and return the path.

        The caller is responsible for deleting the file when done. Retries
        on mid-stream connection failures, which matter for the multi-GB
        Open Payments distributions.
        """
        self.logger.info("Downloading %s", url)
        resp = self.session.get(url, stream=True, timeout=self.timeout)
        resp.raise_for_status()

        tmp = tempfile.NamedTemporaryFile(suffix=suffix, prefix="dkan_", delete=False)
        try:
            for chunk in resp.iter_content(chunk_size=1 << 20):
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
