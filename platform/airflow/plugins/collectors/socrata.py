from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import cached_property
from typing import Any, Iterator, Optional

import requests

from collectors.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class SocrataClient:
    """Executes SoQL queries against a Socrata domain and yields results."""

    def __init__(
        self,
        app_token: Optional[str] = None,
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
    VIEWS_API_TEMPLATE = "https://{domain}/api/views/{dataset_id}.json"
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

    def __init__(self, dataset_id: str) -> None:
        self.dataset_id = dataset_id
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
            raise ValueError(f"Could not determine domain for {self.dataset_id}")
        return _domain

    def _fetch_catalog_metadata(self) -> dict:
        url = f"{self.CATALOG_API}?ids={self.dataset_id}"
        self.logger.info("Fetching catalog metadata for %s", self.dataset_id)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        results = resp.json().get("results", [])
        if not results:
            raise ValueError(f"No results found for dataset_id '{self.dataset_id}'")
        return results[0]

    def _fetch_views_metadata(self) -> dict:
        url = self.VIEWS_API_TEMPLATE.format(
            domain=self.domain, dataset_id=self.dataset_id
        )
        self.logger.info(
            "Fetching views metadata for %s from %s", self.dataset_id, self.domain
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
                self.dataset_id,
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

        self.logger.info("Parsed %d columns for %s", len(parsed), self.dataset_id)
        return parsed

    def _parse_columns_from_catalog(self) -> list[SocrataColumnInfo]:
        resource = self.catalog_metadata.get("resource", {})
        names = resource.get("columns_name", [])
        field_names = resource.get("columns_field_name", [])
        datatypes = resource.get("columns_datatype", [])
        descriptions = resource.get("columns_description", [])

        if not field_names:
            raise ValueError(
                f"No column metadata found for dataset_id '{self.dataset_id}' "
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
            "Parsed %d columns from catalog API for %s", len(parsed), self.dataset_id
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
                "    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC')"
            )

        ddl = f"create table if not exists {fqn} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);\n"

        if include_comments:
            for col in self.columns:
                if col.description:
                    escaped = col.description.replace("'", "''")
                    ddl += (
                        f'\ncomment on column {fqn}."{col.field_name}" '
                        f"is '{escaped}';"
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
        return self.resource_metadata.get("name", self.dataset_id)

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
            return f"https://{self.domain}/api/geospatial/{self.dataset_id}?method=export&format={self.download_format}"
        else:
            return f"https://{self.domain}/api/views/{self.dataset_id}/rows.{self.download_format}?accessType=DOWNLOAD"

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


@dataclass
class IncrementalConfig:
    """
    Configuration for incremental loading of a specific dataset.

    Attributes:
        incremental_column:  The Socrata column to filter on (e.g. "updated_on").
                             Should be monotonically increasing for new/changed rows.
        conflict_key:        Column(s) forming the natural key for upsert.
                             e.g. ["case_number"] or ["pin14", "tax_year"].
        columns:             Optional subset of columns to SELECT. None = all.
        order_by:            Explicit $order clause. Defaults to incremental_column.
                             Use "updated_on, :id" if the column has duplicates.
        where:               Additional static $where filter (combined via AND).
    """

    incremental_column: str
    conflict_key: list[str]
    columns: list[str] | None = None
    order_by: str | None = None
    where: str | None = None


class SocrataCollector:
    """
    High-level orchestrator for Socrata dataset ingestion.
    """

    SOURCE_NAME = "socrata"
    SYSTEM_FIELD_RENAMES = {
        ":id": "socrata_id",
        ":updated_at": "socrata_updated_at",
        ":created_at": "socrata_created_at",
        ":version": "socrata_version",
    }

    def __init__(
        self,
        engine: Any,
        tracker: IngestionTracker | None = None,
        app_token: str | None = None,
        page_size: int = 25000,
    ) -> None:
        self.engine = engine
        self.tracker = tracker or IngestionTracker(engine=engine)
        self.app_token = app_token
        self.page_size = page_size
        self.logger = logging.getLogger("socrata_collector")

        self._client: Optional[SocrataClient] = None
        self._metadata_cache: dict[str, SocrataTableMetadata] = {}

    @property
    def client(self) -> SocrataClient:
        if self._client is None:
            self._client = SocrataClient(
                app_token=self.app_token,
                page_size=self.page_size,
            )
        return self._client

    def _get_metadata(self, dataset_id: str) -> SocrataTableMetadata:
        if dataset_id not in self._metadata_cache:
            self._metadata_cache[dataset_id] = SocrataTableMetadata(dataset_id)
        return self._metadata_cache[dataset_id]

    def full_refresh_via_api(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        conflict_key: list[str] | None = None,
    ) -> int:
        """Full refresh using paginated SODA API (includes system fields)."""
        return self.incremental_update(
            dataset_id=dataset_id,
            target_table=target_table,
            target_schema=target_schema,
            config=IncrementalConfig(
                incremental_column=":updated_at",
                conflict_key=conflict_key,
            ),
            high_water_mark_override="",
        )

    def _rename_system_fields(self, rows: list[dict]) -> list[dict]:
        return [
            {self.SYSTEM_FIELD_RENAMES.get(k, k): v for k, v in row.items()}
            for row in rows
        ]

    def incremental_update(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        config: IncrementalConfig | None = None,
        high_water_mark_override: str | None = None,
    ) -> int:
        """
        Run an incremental paginated ingest using staged_ingest.

        All pages are accumulated in a staging table, then merged into the
        target in a single INSERT ... ON CONFLICT at the end. One
        IngestionTracker entry is created for the entire run.
        """
        raw_hwm = high_water_mark_override or self.tracker.get_high_water_mark(
            self.SOURCE_NAME, dataset_id
        )

        hwm_value, hwm_id = None, None
        if raw_hwm and "|" in raw_hwm:
            hwm_value, hwm_id = raw_hwm.rsplit("|", 1)
        elif raw_hwm:
            hwm_value = raw_hwm

        if hwm_value:
            self.logger.info(
                "Resuming from high_water_mark: %s (id: %s)", hwm_value, hwm_id
            )
        else:
            self.logger.info("No prior high_water_mark — full incremental load")

        meta = self._get_metadata(dataset_id)
        domain = meta.domain
        fqn = f"{target_schema}.{target_table}"
        inc_col = config.incremental_column

        run_metadata = {
            "mode": "incremental",
            "incremental_column": inc_col,
            "conflict_key": config.conflict_key,
            "prior_high_water_mark": raw_hwm,
            "domain": domain,
        }

        with self.tracker.track(
            self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata
        ) as run:
            where_parts = []
            if hwm_value and hwm_id:
                where_parts.append(
                    f"({inc_col} = '{hwm_value}' AND :id > '{hwm_id}') "
                    f"OR ({inc_col} > '{hwm_value}')"
                )
            elif hwm_value:
                where_parts.append(f"{inc_col} > '{hwm_value}'")
            if config.where:
                where_parts.append(config.where)

            combined_where = (
                " AND ".join(f"({p})" for p in where_parts) if where_parts else None
            )
            order_by = config.order_by or f"{inc_col}, :id"

            renamed_inc_col = self.SYSTEM_FIELD_RENAMES.get(inc_col, inc_col)
            max_hwm = hwm_value
            max_hwm_id = hwm_id

            with self.engine.staged_ingest(
                target_table=target_table,
                target_schema=target_schema,
                conflict_column=config.conflict_key,
                conflict_action="NOTHING",
            ) as stager:
                for page_num, batch in enumerate(
                    self.client.paginate(
                        domain=domain,
                        dataset_id=dataset_id,
                        columns=config.columns,
                        where=combined_where,
                        order_by=order_by,
                        include_system_fields=True,
                    ),
                    start=1,
                ):
                    renamed_batch = self._rename_system_fields(batch)
                    stager.write_batch(renamed_batch)

                    batch_hwm, batch_id = self._extract_max_hwm(
                        renamed_batch, renamed_inc_col
                    )
                    if batch_hwm and (
                        max_hwm is None
                        or batch_hwm > max_hwm
                        or (batch_hwm == max_hwm and batch_id > (max_hwm_id or ""))
                    ):
                        max_hwm = batch_hwm
                        max_hwm_id = batch_id

                    self.logger.info(
                        "Page %d: fetched %d (staged total: %d, hwm: %s|%s)",
                        page_num,
                        len(renamed_batch),
                        stager.rows_staged,
                        max_hwm,
                        max_hwm_id,
                    )

            # stager has now merged — record results on the run
            run.rows_staged = stager.rows_staged
            run.rows_merged = stager.rows_merged
            run.rows_ingested = stager.rows_merged
            run.high_water_mark = (
                f"{max_hwm}|{max_hwm_id}" if max_hwm and max_hwm_id else max_hwm
            )

        return stager.rows_merged

    # def incremental_update(
    #     self,
    #     dataset_id: str,
    #     target_table: str,
    #     target_schema: str = "raw_data",
    #     config: IncrementalConfig | None = None,
    #     high_water_mark_override: str | None = None,
    # ) -> int:
    #     """
    #     Run an incremental paginated ingest with automatic high-water mark
    #     management and idempotent upsert.

    #     The high_water_mark is stored as "value|id" to support keyset
    #     pagination with a tiebreaker, e.g. "2025-02-01T12:34:56.000|12345".
    #     """
    #     raw_hwm = high_water_mark_override or self.tracker.get_high_water_mark(
    #         self.SOURCE_NAME, dataset_id
    #     )

    #     hwm_value, hwm_id = None, None
    #     if raw_hwm and "|" in raw_hwm:
    #         hwm_value, hwm_id = raw_hwm.rsplit("|", 1)
    #     elif raw_hwm:
    #         hwm_value = raw_hwm

    #     if hwm_value:
    #         self.logger.info(
    #             "Resuming from high_water_mark: %s (id: %s)", hwm_value, hwm_id
    #         )
    #     else:
    #         self.logger.info("No prior high_water_mark — full incremental load")

    #     meta = self._get_metadata(dataset_id)
    #     domain = meta.domain
    #     fqn = f"{target_schema}.{target_table}"
    #     inc_col = config.incremental_column

    #     run_metadata = {
    #         "mode": "incremental",
    #         "incremental_column": inc_col,
    #         "conflict_key": config.conflict_key,
    #         "prior_high_water_mark": raw_hwm,
    #         "domain": domain,
    #     }

    #     with self.tracker.track(
    #         self.SOURCE_NAME, dataset_id, fqn, metadata=run_metadata
    #     ) as run:
    #         where_parts = []
    #         if hwm_value and hwm_id:
    #             where_parts.append(
    #                 f"({inc_col} = '{hwm_value}' AND :id > '{hwm_id}') "
    #                 f"OR ({inc_col} > '{hwm_value}')"
    #             )
    #         elif hwm_value:
    #             where_parts.append(f"{inc_col} > '{hwm_value}'")
    #         if config.where:
    #             where_parts.append(config.where)

    #         combined_where = (
    #             " AND ".join(f"({p})" for p in where_parts) if where_parts else None
    #         )

    #         order_by = config.order_by or f"{inc_col}, :id"

    #         total_rows = 0
    #         max_hwm = hwm_value
    #         max_hwm_id = hwm_id
    #         renamed_inc_col = self.SYSTEM_FIELD_RENAMES.get(inc_col, inc_col)

    #         for page_num, batch in enumerate(
    #             self.client.paginate(
    #                 domain=domain,
    #                 dataset_id=dataset_id,
    #                 columns=config.columns,
    #                 where=combined_where,
    #                 order_by=order_by,
    #                 include_system_fields=True,
    #             ),
    #             start=1,
    #         ):
    #             renamed_batch = self._rename_system_fields(batch)
    #             rows = self.engine.ingest_batch(
    #                 renamed_batch,
    #                 target_table,
    #                 target_schema=target_schema,
    #                 conflict_column=None,
    #                 conflict_action="NOTHING",
    #             )
    #             total_rows += rows

    #             batch_hwm, batch_id = self._extract_max_hwm(
    #                 renamed_batch, renamed_inc_col
    #             )
    #             if batch_hwm and (
    #                 max_hwm is None
    #                 or batch_hwm > max_hwm
    #                 or (batch_hwm == max_hwm and batch_id > (max_hwm_id or ""))
    #             ):
    #                 max_hwm = batch_hwm
    #                 max_hwm_id = batch_id

    #             self.logger.info(
    #                 "Page %d: fetched %d, upserted %d (total: %d, hwm: %s|%s)",
    #                 page_num,
    #                 len(renamed_batch),
    #                 rows,
    #                 total_rows,
    #                 max_hwm,
    #                 max_hwm_id,
    #             )

    #         run.rows_ingested = total_rows
    #         run.high_water_mark = (
    #             f"{max_hwm}|{max_hwm_id}" if max_hwm and max_hwm_id else max_hwm
    #         )

    #     return total_rows

    @staticmethod
    def _extract_max_hwm(
        batch: list[dict[str, Any]], column: str
    ) -> tuple[str | None, str | None]:
        """Return (max_column_value, id_at_max) from a batch."""
        values = [
            (row[column], str(row.get("socrata_id", "")))
            for row in batch
            if row.get(column) is not None
        ]
        if not values:
            return None, None
        best = max(values, key=lambda x: (x[0], x[1]))
        return str(best[0]), best[1]

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def preview(
        self,
        dataset_id: str,
        limit: int = 5,
        columns: list[str] | None = None,
        include_system_fields: bool = True,
    ) -> list[dict[str, Any]]:
        meta = self._get_metadata(dataset_id)
        return self.client.query(
            domain=meta.domain,
            dataset_id=dataset_id,
            select=", ".join(columns) if columns else None,
            limit=limit,
            include_system_fields=include_system_fields,
        )

    def print_ddl(
        self,
        dataset_id: str,
        schema: str = "raw_data",
        table_name: str | None = None,
    ) -> None:
        meta = self._get_metadata(dataset_id)
        print(meta.generate_ddl(schema=schema, table_name=table_name))
