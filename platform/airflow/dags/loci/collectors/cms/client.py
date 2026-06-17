# /loci_platform/platform/airflow/dags/loci/collectors/cms/client.py
"""
CMSClient — thin HTTP client for the data.cms.gov catalog and data API.

Knows three things:
  1. how to fetch and cache the data.json catalog
  2. how to page rows out of the versioned JSON data API
  3. how to stream a CSV distribution to disk

Usage:
    from loci.collectors.cms.client import CMSClient

    client = CMSClient()
    catalog = client.get_catalog()
    n = client.row_count("9887a515-7552-4693-bf58-735c77af46d7")
    for page in client.iter_pages("9887a515-7552-4693-bf58-735c77af46d7"):
        ...
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

CATALOG_URL = "https://data.cms.gov/data.json"
DATA_API_BASE = "https://data.cms.gov/data-api/v1/dataset"


class CMSClient:
    """
    Thin requests wrapper for data.cms.gov.

    Parameters
    ----------
    timeout : int
        Per-request timeout in seconds. Default 60 (the catalog is large
        and big API pages can be slow).
    page_size : int
        Default rows per page for iter_pages/iter_rows. The API maximum
        is 5000.
    """

    def __init__(self, timeout: int = 60, page_size: int = 5000):
        self.timeout = timeout
        self.page_size = page_size
        self._catalog: list[dict] | None = None

        self.session = requests.Session()
        retry = Retry(
            total=5,
            backoff_factor=1.0,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retry))

    def _get_json(self, url: str, params: dict | None = None):
        resp = self.session.get(url, params=params, timeout=self.timeout)
        resp.raise_for_status()
        return resp.json()

    # -- catalog ---------------------------------------------------------

    def get_catalog(self, refresh: bool = False) -> list[dict]:
        """
        Return the list of dataset entries from the data.json catalog.

        Cached after the first call; pass refresh=True to re-fetch.
        """
        if self._catalog is None or refresh:
            self._catalog = self._get_json(CATALOG_URL)["dataset"]
        return self._catalog

    # -- data API --------------------------------------------------------

    def get_stats(self, version_uuid: str) -> dict:
        """Return the raw /data/stats response for a dataset version."""
        return self._get_json(f"{DATA_API_BASE}/{version_uuid}/data/stats")

    def row_count(self, version_uuid: str) -> int:
        """Return the total row count for a dataset version."""
        stats = self.get_stats(version_uuid)
        # Be tolerant of the exact response shape: counts have appeared
        # both at the top level and nested under "data".
        candidates = stats.get("data", stats) if isinstance(stats, dict) else {}
        for key in ("total_rows", "found_rows"):
            if key in candidates:
                return int(candidates[key])
        raise ValueError(f"Could not find a row count in stats response: {stats}")

    def get_page(self, version_uuid: str, size: int, offset: int) -> list[dict]:
        """Return one page of rows (list of column-name-keyed dicts)."""
        return self._get_json(
            f"{DATA_API_BASE}/{version_uuid}/data",
            params={"size": size, "offset": offset},
        )

    def iter_pages(self, version_uuid: str, size: int | None = None) -> Iterator[list[dict]]:
        """
        Yield pages of rows for a dataset version until exhausted.

        Stops after the first empty or short page. The final page may be
        shorter than size.
        """
        size = size or self.page_size
        offset = 0
        while True:
            page = self.get_page(version_uuid, size=size, offset=offset)
            if not page:
                return
            yield page
            if len(page) < size:
                return
            offset += size

    def iter_rows(self, version_uuid: str, size: int | None = None) -> Iterator[dict]:
        """Yield every row of a dataset version, paging through the API."""
        for page in self.iter_pages(version_uuid, size=size):
            yield from page

    # -- CSV distributions -------------------------------------------------

    def download_csv(self, url: str, dest_path: str | Path) -> Path:
        """
        Stream a CSV distribution to dest_path and return the path.
        """
        dest_path = Path(dest_path)
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        with self.session.get(url, stream=True, timeout=self.timeout) as resp:
            resp.raise_for_status()
            with open(dest_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=1 << 20):
                    f.write(chunk)
        return dest_path
