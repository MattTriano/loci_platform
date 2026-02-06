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
import re
from dataclasses import dataclass
from functools import cached_property
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

    def _request(self, dataset_id: str, params: dict[str, str]) -> list[dict[str, Any]]:
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


@dataclass
class SocrataColumnInfo:
    """Represents a single column from a Socrata dataset."""

    name: str
    field_name: str
    datatype: str
    description: str
    pg_type: str

    @property
    def ddl_fragment(self) -> str:
        safe_name = f'"{self.field_name}"'
        return f"    {safe_name} {self.pg_type}"


class SocrataTableMetadata:
    """Fetches Socrata dataset metadata and generates PostgreSQL DDL."""

    CATALOG_API = "http://api.us.socrata.com/api/catalog/v1"
    VIEWS_API_TEMPLATE = "https://{domain}/api/views/{table_id}.json"
    # Socrata datatype → PostgreSQL type mapping
    # Covers SODA 2.0 and 2.1 types, plus deprecated types that appear in older datasets
    SOCRATA_TO_PG_TYPE: dict[str, str] = {
        "text": "text",
        "url": "text",
        "number": "numeric",
        "double": "double precision",
        "money": "numeric(19,4)",
        "percent": "numeric",
        "checkbox": "boolean",
        "calendar_date": "timestamptz",
        "date": "timestamptz",
        "fixed_timestamp": "timestamptz",
        "floating_timestamp": "timestamp",
        "point": "geometry(Point, 4326)",
        "multipoint": "geometry(MultiPoint, 4326)",
        "line": "geometry(LineString, 4326)",
        "multiline": "geometry(MultiLineString, 4326)",
        "polygon": "geometry(Polygon, 4326)",
        "multipolygon": "geometry(MultiPolygon, 4326)",
        "location": "jsonb",
        "blob": "text",
        "photo": "text",
        "document": "text",
        "html": "text",
        "email": "text",
        "phone": "text",
    }
    DEFAULT_PG_TYPE = "text"

    def __init__(self, table_id: str) -> None:
        self.table_id = table_id
        self.logger = logging.getLogger("socrata_table_metadata")

        self._catalog_metadata: Optional[dict] = None
        self._views_metadata: Optional[dict] = None
        self._columns: Optional[list[SocrataColumnInfo]] = None

    @cached_property
    def catalog_metadata(self) -> dict:
        if self._catalog_metadata is None:
            self._catalog_metadata = self._fetch_catalog_metadata()
        return self._catalog_metadata

    @cached_property
    def views_metadata(self) -> dict:
        if self._views_metadata is None:
            self._views_metadata = self._fetch_views_metadata()
        return self._views_metadata

    @cached_property
    def domain(self) -> str:
        _domain = self.catalog_metadata.get("metadata", {}).get("domain")
        if not _domain:
            raise ValueError(f"Could not determine domain for {self.table_id}")
        return _domain

    def _fetch_catalog_metadata(self) -> dict:
        url = f"{self.CATALOG_API}?ids={self.table_id}"
        self.logger.info("Fetching catalog metadata for %s", self.table_id)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            raise ValueError(f"No results found for table_id '{self.table_id}'")
        return results[0]

    def _fetch_views_metadata(self) -> dict:
        url = self.VIEWS_API_TEMPLATE.format(domain=self.domain, table_id=self.table_id)
        self.logger.info(
            "Fetching views metadata for %s from %s", self.table_id, self.domain
        )
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @cached_property
    def columns(self) -> list[SocrataColumnInfo]:
        if self._columns is None:
            self._columns = self._parse_columns()
        return self._columns

    def _parse_columns(self) -> list[SocrataColumnInfo]:
        raw_columns = self.views_metadata.get("columns", [])
        if not raw_columns:
            self.logger.warning(
                "No columns in views API for %s, falling back to catalog API",
                self.table_id,
            )
            return self._parse_columns_from_catalog()

        parsed = []
        for col in raw_columns:
            flags = col.get("flags", [])
            if "hidden" in flags:
                continue

            datatype = col.get("dataTypeName", "text").lower()
            pg_type = self.SOCRATA_TO_PG_TYPE.get(datatype, self.DEFAULT_PG_TYPE)

            parsed.append(
                SocrataColumnInfo(
                    name=col.get("name", ""),
                    field_name=col.get("fieldName", ""),
                    datatype=datatype,
                    description=col.get("description", ""),
                    pg_type=pg_type,
                )
            )

        self.logger.info("Parsed %d columns for %s", len(parsed), self.table_id)
        return parsed

    def _parse_columns_from_catalog(self) -> list[SocrataColumnInfo]:
        resource = self.catalog_metadata.get("resource", {})
        names = resource.get("columns_name", [])
        field_names = resource.get("columns_field_name", [])
        datatypes = resource.get("columns_datatype", [])
        descriptions = resource.get("columns_description", [])

        if not field_names:
            raise ValueError(
                f"No column metadata found for table_id '{self.table_id}' "
                f"in either views or catalog API"
            )

        parsed = []
        for i, field_name in enumerate(field_names):
            datatype = datatypes[i].lower() if i < len(datatypes) else "text"
            pg_type = self.SOCRATA_TO_PG_TYPE.get(datatype, self.DEFAULT_PG_TYPE)

            parsed.append(
                SocrataColumnInfo(
                    name=names[i] if i < len(names) else field_name,
                    field_name=field_name,
                    datatype=datatype,
                    description=descriptions[i] if i < len(descriptions) else "",
                    pg_type=pg_type,
                )
            )

        self.logger.info(
            "Parsed %d columns from catalog API for %s", len(parsed), self.table_id
        )
        return parsed

    def generate_ddl(
        self,
        schema: str = "raw_data",
        table_name: str | None = None,
        include_ingested_at: bool = True,
        include_comments: bool = True,
    ) -> str:
        """
        Generate a CREATE TABLE IF NOT EXISTS statement from the dataset's columns.

        Args:
            schema:              Target schema name.
            table_name:          Target table name. Defaults to a sanitized version
                                 of the dataset name from the metadata.
            include_ingested_at: If True, appends an 'ingested_at' TIMESTAMPTZ column
                                 with a default of now().
            include_comments:    If True, adds COMMENT ON COLUMN statements for columns
                                 that have descriptions in the metadata.

        Returns:
            The full DDL string.
        """
        if table_name is None:
            table_name = self._default_table_name()

        fqn = f"{schema}.{table_name}"
        col_defs = [col.ddl_fragment for col in self.columns]

        if include_ingested_at:
            col_defs.append(
                "    \"ingested_at\" TIMESTAMPTZ NOT NULL DEFAULT (now() AT TIME ZONE 'UTC')"
            )

        ddl = f"CREATE TABLE IF NOT EXISTS {fqn} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);\n"

        if include_comments:
            for col in self.columns:
                if col.description:
                    escaped = col.description.replace("'", "''")
                    ddl += (
                        f'\nCOMMENT ON COLUMN {fqn}."{col.field_name}" '
                        f"IS '{escaped}';"
                    )

        return ddl

    def print_ddl(
        self,
        schema: str = "raw_data",
        table_name: str | None = None,
        include_ingested_at: bool = True,
        include_comments: bool = True,
    ) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        ddl = self.generate_ddl(
            schema=schema,
            table_name=table_name,
            include_ingested_at=include_ingested_at,
            include_comments=include_comments,
        )
        print(ddl)

    @property
    def resource_metadata(self) -> dict:
        return self.catalog_metadata.get("resource", {})

    @property
    def dataset_name(self) -> str:
        return self.resource_metadata.get("name", self.table_id)

    @cached_property
    def has_geospatial_columns(self) -> bool:
        geo_types = {
            "point",
            "multipoint",
            "line",
            "multiline",
            "polygon",
            "multipolygon",
            "location",
        }
        return any(col.datatype in geo_types for col in self.columns)

    @cached_property
    def has_map_type_display(self) -> bool:
        table_display_type = self.resource_metadata.get("lens_display_type")
        return table_display_type == "map"

    @cached_property
    def has_geo_type_view(self) -> bool:
        table_view_type = self.resource_metadata.get("lens_view_type")
        return table_view_type == "geo"

    @cached_property
    def has_data_columns(self) -> bool:
        table_data_cols = self.resource_metadata.get("columns_name")
        return len(table_data_cols) != 0

    @cached_property
    def is_geospatial(self) -> bool:
        return (
            (not self.has_data_columns)
            and (self.has_geo_type_view or self.has_map_type_display)
        ) or (self.has_geospatial_columns)

    @cached_property
    def download_format(self) -> str:
        if self.is_geospatial:
            return "GeoJSON"
        else:
            return "csv"

    @property
    def data_download_url(self) -> str:
        if self.is_geospatial:
            return f"https://{self.domain}/api/geospatial/{self.table_id}?method=export&format={self.download_format}"
        else:
            return f"https://{self.domain}/api/views/{self.table_id}/rows.{self.download_format}?accessType=DOWNLOAD"

    def _default_table_name(self) -> str:
        name = self.dataset_name.lower()
        name = re.sub(r"[^0-9a-z]+", "_", name)
        return name.strip("_")

    def print_column_summary(self) -> None:
        header = f"{'Field Name':40s} {'Socrata Type':20s} {'PG Type':30s}"
        print(header)
        print("-" * len(header))
        for col in self.columns:
            print(f"{col.field_name:40s} {col.datatype:20s} {col.pg_type:30s}")
