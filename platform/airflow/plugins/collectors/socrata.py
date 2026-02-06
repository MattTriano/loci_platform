"""
Socrata SoQL query executor and PostgreSQL ingestor.

Supports:
- Arbitrary SoQL queries against any Socrata dataset
- Automatic pagination for large result sets
- Incremental loads using a high-water mark column
- Structured logging throughout

Requirements:
    pip install requests psycopg2-binary

Usage:
    from socrata_ingestor import SocrataIngestor
    from postgres_engine import PostgresEngine

    engine = PostgresEngine(creds=my_creds)
    ingestor = SocrataIngestor(
        domain="data.cityofchicago.org",
        engine=engine,
        app_token="your_socrata_app_token",  # optional but recommended
    )

    # Full load
    ingestor.ingest(
        dataset_id="ijzp-q8t2",
        target_table="raw.crimes",
        columns=["id", "date", "primary_type", "description", "latitude", "longitude"],
    )

    # Incremental load
    ingestor.ingest(
        dataset_id="ijzp-q8t2",
        target_table="raw.crimes",
        columns=["id", "date", "primary_type", "description", "latitude", "longitude"],
        incremental_column="date",
        high_water_mark="2025-01-01T00:00:00",
    )
"""

from __future__ import annotations

import logging
from typing import Any, Iterator, Optional

import requests

logger = logging.getLogger(__name__)


class SocrataIngestor:
    """Executes SoQL queries against a Socrata domain and loads results into PostgreSQL."""

    def __init__(
        self,
        domain: str,
        engine: Any,
        app_token: Optional[str] = None,
        page_size: int = 10_000,
        request_timeout: int = 120,
    ) -> None:
        self.domain = domain
        self.engine = engine
        self.app_token = app_token
        self.page_size = page_size
        self.request_timeout = request_timeout
        self.logger = logging.getLogger("socrata_ingestor")

        self._session = requests.Session()
        self._session.headers["Accept"] = "application/json"
        if self.app_token:
            self._session.headers["X-App-Token"] = self.app_token

    def ingest(
        self,
        dataset_id: str,
        target_table: str,
        columns: list[str] | None = None,
        where: str | None = None,
        order_by: str | None = None,
        incremental_column: str | None = None,
        high_water_mark: str | None = None,
    ) -> int:
        """
        Run a SoQL query with pagination and insert all results into *target_table*.

        Args:
            dataset_id:          Socrata 4x4 dataset identifier (e.g. "ijzp-q8t2").
            target_table:        Fully-qualified PostgreSQL table (e.g. "raw.crimes").
                                 Must already exist.
            columns:             Columns to SELECT. None means all (*).
            where:               Optional SoQL $where clause fragment.
            order_by:            Column(s) to $order by. Defaults to ":id" for
                                 stable pagination.
            incremental_column:  Column to filter on for incremental loads.
            high_water_mark:     Only fetch rows where incremental_column > this value.
                                 Ignored if incremental_column is None.

        Returns:
            Total number of rows inserted.
        """
        where_clauses = []
        if incremental_column and high_water_mark:
            where_clauses.append(f"{incremental_column} > '{high_water_mark}'")
            self.logger.info(
                "Incremental load: %s > '%s'", incremental_column, high_water_mark
            )
        if where:
            where_clauses.append(where)

        combined_where = " AND ".join(where_clauses) if where_clauses else None
        effective_order = order_by or (incremental_column or ":id")

        total_inserted = 0
        for page_num, batch in enumerate(
            self._paginate(dataset_id, columns, combined_where, effective_order),
            start=1,
        ):
            if not batch:
                break
            rows_inserted = self.engine.ingest_batch(batch, target_table)
            total_inserted += rows_inserted
            self.logger.info(
                "Page %d: fetched %d rows, inserted %d (running total: %d)",
                page_num,
                len(batch),
                rows_inserted,
                total_inserted,
            )

        self.logger.info(
            "Ingest complete for %s → %s: %d rows",
            dataset_id,
            target_table,
            total_inserted,
        )
        return total_inserted

    def query(
        self,
        dataset_id: str,
        select: str | None = None,
        where: str | None = None,
        order: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, Any]]:
        """Execute a one-shot SoQL query and return the JSON result."""
        params: dict[str, str] = {}
        if select:
            params["$select"] = select
        if where:
            params["$where"] = where
        if order:
            params["$order"] = order
        if limit is not None:
            params["$limit"] = str(limit)
        return self._request(dataset_id, params)

    def _paginate(
        self,
        dataset_id: str,
        columns: list[str] | None,
        where: str | None,
        order_by: str,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of results until the API returns fewer rows than page_size."""
        offset = 0
        while True:
            params: dict[str, str] = {
                "$limit": str(self.page_size),
                "$offset": str(offset),
                "$order": order_by,
            }
            if columns:
                params["$select"] = ", ".join(columns)
            if where:
                params["$where"] = where

            batch = self._request(dataset_id, params)
            yield batch

            if len(batch) < self.page_size:
                break
            offset += self.page_size

    def _request(
        self, dataset_id: str, params: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Make a single request to the Socrata JSON endpoint."""
        url = f"https://{self.domain}/resource/{dataset_id}.json"
        self.logger.debug("GET %s  params=%s", url, params)

        resp = self._session.get(url, params=params, timeout=self.request_timeout)
        resp.raise_for_status()

        if "X-Rate-Limit-Remaining" in resp.headers:
            self.logger.debug(
                "Rate limit remaining: %s", resp.headers["X-Rate-Limit-Remaining"]
            )

        return resp.json()