# /loci_platform/platform/airflow/dags/loci/collectors/socrata/client.py
from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from typing import Any

import requests
from requests.exceptions import ChunkedEncodingError, ConnectionError, HTTPError, ReadTimeout
from tenacity import (
    before_sleep_log,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

logger = logging.getLogger(__name__)


class SocrataClient:
    """Executes SoQL queries against a Socrata domain and yields results."""

    RETRYABLE_STATUS = {429, 500, 502, 503, 504}

    def __init__(
        self,
        app_token: str | None = None,
        page_size: int = 10000,
        request_timeout: int = 120,
    ) -> None:
        self.app_token = app_token
        self.page_size = page_size
        self.request_timeout = request_timeout
        self.logger = logging.getLogger("socrata_client")

        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        if self.app_token:
            self._session.headers["X-App-Token"] = self.app_token

    def paginate(
        self,
        domain: str,
        dataset_id: str,
        columns: list[str] | None = None,
        where: str | None = None,
        order_by: str = ":updated_at, :id",
        include_system_fields: bool = False,
    ) -> Iterator[list[dict[str, Any]]]:
        """
        Yield pages of results from a Socrata dataset until exhausted.

        Args:
            domain:      Socrata domain (e.g. "data.cityofchicago.org").
            dataset_id:  Socrata 4x4 identifier.
            columns:     Columns to $select. None means all.
            where:       Optional $where clause.
            order_by:    $order clause. Defaults to ":id" for stable pagination.

        Yields:
            Lists of dicts, one per page. Final page may be shorter than page_size.
        """
        offset = 0
        while True:
            params: dict[str, str] = {
                "$limit": str(self.page_size),
                "$offset": str(offset),
                "$order": order_by,
            }
            if columns:
                params["$select"] = ", ".join(columns)
            elif include_system_fields:
                params["$select"] = ":*, *"
            if where:
                params["$where"] = where

            batch = self._request(domain, dataset_id, params, include_system_fields)
            if not batch:
                break

            yield batch

            if len(batch) < self.page_size:
                break
            offset += self.page_size

    def query(
        self,
        domain: str,
        dataset_id: str,
        select: str | None = None,
        where: str | None = None,
        order: str | None = None,
        limit: int | None = None,
        include_system_fields: bool = False,
    ) -> list[dict[str, Any]]:
        """Execute a one-shot SoQL query and return the JSON result."""
        params: dict[str, str] = {}
        if select:
            params["$select"] = select
        elif include_system_fields:
            params["$select"] = ":*, *"
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order
        if limit is not None:
            params["$limit"] = str(limit)
        return self._request(domain, dataset_id, params, include_system_fields)

    def is_retryable(self, exc: Exception) -> bool:
        if isinstance(exc, HTTPError) and exc.response is not None:
            return exc.response.status_code in self.RETRYABLE_STATUS
        return isinstance(
            exc, (json.JSONDecodeError, ConnectionError, ReadTimeout, ChunkedEncodingError)
        )

    @retry(
        retry=retry_if_exception(is_retryable),
        stop=stop_after_attempt(3),
        wait=wait_exponential(multiplier=1, max=10),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    def _request(
        self,
        domain: str,
        dataset_id: str,
        params: dict[str, str],
        include_system_fields: bool = False,
    ) -> list[dict[str, Any]]:
        """Make a single request to the Socrata JSON endpoint."""
        url = f"https://{domain}/resource/{dataset_id}.json"
        if include_system_fields and "$select" not in params:
            params["$$exclude_system_fields"] = "false"
        self.logger.debug("GET %s  params=%s", url, params)
        resp = self._session.get(url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()
        if "X-Rate-Limit-Remaining" in resp.headers:
            self.logger.debug("Rate limit remaining: %s", resp.headers["X-Rate-Limit-Remaining"])
        try:
            return resp.json()
        except json.JSONDecodeError as e:
            self.logger.warning(
                "Bad JSON on char %d of %d: ...%s...",
                e.pos,
                len(resp.text),
                resp.text[max(0, e.pos - 200) : e.pos + 200],
            )
            raise

    def find_dupes(self, domain: str, dataset_id: str, columns: list[str]):
        cols = ", ".join(columns)
        params = {
            "$select": f"{cols}, count(*) as n",
            "$group": cols,
            "$having": "n > 1",
            "$limit": 5,
        }
        headers = {"X-App-Token": self.app_token} if self.app_token else {}
        r = requests.get(
            f"https://{domain}/resource/{dataset_id}.json",
            params=params,
            headers=headers,
            timeout=60,
        )
        r.raise_for_status()
        return r.json()
