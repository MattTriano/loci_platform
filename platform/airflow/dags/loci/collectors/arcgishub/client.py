# /loci_platform/platform/airflow/dags/loci/collectors/arcgishub/client.py
"""HTTP client for ArcGIS Hub data portals."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

import requests
from tenacity import (
    retry,
    retry_if_exception,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def _is_retryable_http_error(exc: BaseException) -> bool:
    """Retry on 5xx responses, but not on 4xx."""
    if isinstance(exc, requests.HTTPError):
        response = exc.response
        return response is not None and 500 <= response.status_code < 600
    return False


def hub_retry():
    return retry(
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, min=1, max=10),
        retry=(
            retry_if_exception_type(requests.ConnectionError)
            | retry_if_exception_type(requests.Timeout)
            | retry_if_exception(_is_retryable_http_error)
        ),
        reraise=True,
    )


class ArcGISHubClient:
    """
    Thin wrapper around requests.Session for ArcGIS Hub sites.

    Knows how to GET JSON with retries and how to paginate OGC API - Records
    responses. Does not know anything about datasets, fields, or feature
    services -- that's the Metadata/Collector layer's job.

    Usage:
        with ArcGISHubClient("https://data.tps.ca") as client:
            payload = client.get_json("/api/search/v1/collections/dataset/items",
                                      params={"limit": 5})
    """

    def __init__(
        self,
        base_url: str,
        timeout: tuple[float, float] = (10.0, 60.0),
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self._session = requests.Session()
        self._session.headers.update({"Accept": "application/json"})

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    @hub_retry()
    def get_json(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        GET a URL and return its parsed JSON body.

        `path_or_url` may be either an absolute URL (e.g. a `next` link from a
        previous response) or a path relative to `base_url`.
        """
        url = self._resolve(path_or_url)
        response = self._session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        return response.json()

    def paginate(
        self,
        path_or_url: str,
        params: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        """
        Yield items from a paginated OGC API - Records endpoint.

        Follows `next` links in the response's `links` array until exhausted.
        Yields each entry in `features` one at a time.
        """
        next_url: str | None = self._resolve(path_or_url)
        next_params: dict[str, Any] | None = params

        while next_url is not None:
            payload = self.get_json(next_url, params=next_params)

            for feature in payload.get("features", []):
                yield feature

            # After the first request, `next` links are fully-qualified and
            # already carry their own query string -- don't re-send params.
            next_url = _find_next_link(payload)
            next_params = None

    def close(self) -> None:
        self._session.close()

    def __enter__(self) -> ArcGISHubClient:
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> bool:
        self.close()
        return False

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve(self, path_or_url: str) -> str:
        if path_or_url.startswith(("http://", "https://")):
            return path_or_url
        if not path_or_url.startswith("/"):
            path_or_url = "/" + path_or_url
        return self.base_url + path_or_url


def _find_next_link(payload: dict[str, Any]) -> str | None:
    """Return the href of the `next` link in an OGC Records payload, if any."""
    for link in payload.get("links", []) or []:
        if link.get("rel") == "next" and link.get("href"):
            return link["href"]
    return None
