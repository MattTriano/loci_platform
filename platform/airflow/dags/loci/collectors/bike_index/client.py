"""
BikeIndexClient — HTTP interface for the Bike Index API v3.

Handles pagination, rate limiting, and optional OAuth2 authentication.

API docs: https://bikeindex.org/documentation/api_v3
Source: https://github.com/bikeindex/bike_index

Key endpoints used:
    GET /api/v3/search
        ?stolenness=proximity&location=Chicago,IL&distance=10&per_page=100&page=1
    GET /api/v3/search/count
        (same params, returns {proximity: N, stolen: N, non: N})
    GET /api/v3/bikes/{id}
        Returns full detail for a single bike.
"""

import logging
import time
from collections.abc import Iterator
from dataclasses import dataclass

import requests

log = logging.getLogger(__name__)

BASE_URL = "https://bikeindex.org/api/v3"


@dataclass(frozen=True)
class BikeIndexSearchParams:
    """Parameters for a Bike Index stolen-bike search.

    Attributes:
        location:    City name, zip code, address, or "lat,lon".
        distance:    Radius in miles from location.
        stolenness:  One of "proximity", "stolen", "non", "all".
        query:       Optional free-text search (brand, model, color, etc.).
        per_page:    Results per page (max 100).
    """

    location: str = "Chicago, IL"
    distance: int = 10
    stolenness: str = "proximity"
    query: str | None = None
    per_page: int = 100

    def to_params(self) -> dict:
        params = {
            "location": self.location,
            "distance": self.distance,
            "stolenness": self.stolenness,
            "per_page": self.per_page,
        }
        if self.query:
            params["query"] = self.query
        return params


class BikeIndexClient:
    """HTTP client for the Bike Index API v3.

    Args:
        access_token: Optional OAuth2 token. Unauthenticated requests have
                      stricter rate limits but work for read-only searches.
        timeout:      Request timeout in seconds.
        max_retries:  Number of retries on transient errors (429, 5xx).
        retry_delay:  Base delay between retries in seconds (doubles each retry).
    """

    def __init__(
        self,
        access_token: str | None = None,
        timeout: float = 30.0,
        max_retries: int = 3,
        retry_delay: float = 2.0,
    ):
        self.access_token = access_token
        self.timeout = timeout
        self.max_retries = max_retries
        self.retry_delay = retry_delay

        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        if access_token:
            self._session.headers["Authorization"] = f"Bearer {access_token}"

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    # -- Low-level request with retry ------------------------------------------

    def _request(self, method: str, path: str, **kwargs) -> dict:
        """Make an HTTP request with retry logic for rate limits and server errors."""
        url = f"{BASE_URL}{path}"
        kwargs.setdefault("timeout", self.timeout)
        delay = self.retry_delay

        for attempt in range(self.max_retries + 1):
            try:
                resp = self._session.request(method, url, **kwargs)
            except requests.ConnectionError as e:
                if attempt == self.max_retries:
                    raise
                log.warning("Connection error (attempt %d): %s", attempt + 1, e)
                time.sleep(delay)
                delay *= 2
                continue

            if resp.status_code == 429:
                retry_after = float(resp.headers.get("Retry-After", delay))
                log.warning("Rate limited, sleeping %.1fs", retry_after)
                time.sleep(retry_after)
                delay *= 2
                continue

            if resp.status_code >= 500:
                if attempt == self.max_retries:
                    resp.raise_for_status()
                log.warning("Server error %d (attempt %d)", resp.status_code, attempt + 1)
                time.sleep(delay)
                delay *= 2
                continue

            resp.raise_for_status()
            return resp.json()

        raise RuntimeError(f"Exhausted retries for {method} {path}")

    def _get(self, path: str, params: dict | None = None) -> dict:
        return self._request("GET", path, params=params)

    # -- Public API methods ----------------------------------------------------

    def search_count(self, search: BikeIndexSearchParams) -> dict:
        """Get counts of bikes matching a search.

        Returns:
            dict with keys: proximity, stolen, non
        """
        return self._get("/search/count", params=search.to_params())

    def search(self, search: BikeIndexSearchParams, page: int = 1) -> dict:
        """Search for bikes (one page).

        Returns:
            dict with key "bikes" containing a list of bike summaries.
        """
        params = search.to_params()
        params["page"] = page
        return self._get("/search", params=params)

    def search_all(self, search: BikeIndexSearchParams) -> Iterator[list[dict]]:
        """Paginate through all search results, yielding one page at a time.

        Each yielded list contains bike summary dicts for one page.
        Stops when a page returns fewer results than per_page.
        """
        page = 1
        while True:
            data = self.search(search, page=page)
            bikes = data.get("bikes", [])
            if not bikes:
                break
            log.info("Page %d: %d bikes", page, len(bikes))
            yield bikes
            if len(bikes) < search.per_page:
                break
            page += 1

    def get_bike(self, bike_id: int) -> dict:
        """Get full details for a single bike by ID.

        The search endpoint returns summary info. This endpoint adds
        the stolen_record (with lat/lon), components, and photos.
        """
        data = self._get(f"/bikes/{bike_id}")
        return data.get("bike", data)
