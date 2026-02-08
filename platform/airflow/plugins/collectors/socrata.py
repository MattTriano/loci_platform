from __future__ import annotations

import logging
import re
import tempfile
from dataclasses import dataclass
from functools import cached_property
from pathlib import Path
from typing import Any, Iterator, Optional

import requests

from collectors.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class SocrataClient:
    """
    Executes SoQL queries against a Socrata domain and yields results.

    Unlike the previous SocrataIngestor, this class:
      - Has no PostgresEngine dependency (pure API client)
      - Does not require domain at init (accepts it per-call)
      - Focuses on two operations: paginated iteration and one-shot queries
    """

    def __init__(
        self,
        app_token: Optional[str] = None,
        page_size: int = 10_000,
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
        order_by: str = ":id",
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
            if where:
                params["$where"] = where

            batch = self._request(domain, dataset_id, params)
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
        return self._request(domain, dataset_id, params)

    def _request(
        self, domain: str, dataset_id: str, params: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Make a single request to the Socrata JSON endpoint."""
        url = f"https://{domain}/resource/{dataset_id}.json"
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

    Changes from original:
      - Automatically creates an IngestionTracker if none is provided.
      - Renamed methods: full_refresh() and incremental_update().
      - Geospatial files are loaded via _load_geojson_features() which
        reads GeoJSON and calls engine.ingest_batch() instead of a
        non-existent engine.ingest_geospatial() method.
    """

    SOURCE_NAME = "socrata"

    def __init__(
        self,
        engine: Any,
        tracker: IngestionTracker | None = None,
        app_token: str | None = None,
        download_dir: Path | None = None,
        page_size: int = 10_000,
    ) -> None:
        self.engine = engine
        self.tracker = tracker or IngestionTracker(engine=engine)
        self.app_token = app_token
        self.download_dir = download_dir or Path(tempfile.gettempdir())
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

    def full_refresh(
        self,
        dataset_id: str,
        target_table: str,
        target_schema: str = "raw_data",
        conflict_key: list[str] | None = None,
        mode: str = "upsert",
    ) -> int:
        """
        Download the full dataset export and load it into PostgreSQL.

        This is a full table refresh — the entire dataset is downloaded and
        loaded in one shot.

        Args:
            dataset_id:    Socrata 4x4 identifier.
            target_table:  Fully-qualified table name (e.g. "raw_data.permits").
            target_schema: Schema name for routing.
            conflict_key:  Columns for ON CONFLICT upsert.
            mode:          "upsert" | "replace" | "append"

        Returns:
            Number of rows ingested.
        """
        meta = self._get_metadata(dataset_id)
        run_metadata = {
            "mode": mode,
            "download_format": meta.download_format,
            "download_url": meta.data_download_url,
            "domain": meta.domain,
        }

        with self.tracker.track(
            self.SOURCE_NAME, dataset_id, target_table, metadata=run_metadata
        ) as run:
            filepath = self._download_file(meta)
            self.logger.info("Downloaded %s to %s", dataset_id, filepath)

            if meta.is_geospatial:
                if mode == "replace":
                    self.engine.execute(f"TRUNCATE TABLE {target_table}")
                rows, failed = self.engine.ingest_geojson(
                    filepath=filepath,
                    target_table=target_table,
                    conflict_key=conflict_key if mode == "upsert" else None,
                )
            else:
                rows = self._ingest_csv_with_conflict_handling(
                    filepath=filepath,
                    target_table=target_table,
                    conflict_key=conflict_key,
                    mode=mode,
                )

            run.rows_ingested = rows

            try:
                filepath.unlink()
            except OSError:
                pass

        return rows

    # def _detect_geometry_column(self, target_table: str) -> Optional[str]:
    #     """
    #     Query the target table's information_schema to find a PostGIS geometry
    #     column.  Returns the column name, or None if no geometry column exists.
    #     """
    #     # Split schema.table for the query
    #     parts = target_table.split(".", 1)
    #     if len(parts) == 2:
    #         schema, table = parts
    #     else:
    #         schema, table = "public", parts[0]

    #     df = self.engine.query(
    #         """
    #         SELECT f_geometry_column
    #         FROM geometry_columns
    #         WHERE f_table_schema = %(schema)s
    #           AND f_table_name   = %(table)s
    #         LIMIT 1
    #         """,
    #         {"schema": schema, "table": table},
    #     )
    #     if df.empty:
    #         return None
    #     return str(df.iloc[0]["f_geometry_column"])

    # @staticmethod
    # def _geojson_to_wkt(geojson_geom: dict) -> str:
    #     """Convert a GeoJSON geometry dict to WKT using Shapely."""
    #     from shapely.geometry import shape

    #     return shape(geojson_geom).wkt

    # @staticmethod
    # def _sanitize_utf8(value: str) -> str:
    #     """
    #     Remove or replace characters that will fail PostgreSQL UTF-8 COPY.

    #     Handles two scenarios:
    #       1. Surrogate-escaped strings from file I/O with errors='surrogateescape'
    #          (lone surrogates like \\udcXX).
    #       2. C1 control characters (U+0080–U+009F) that come from Windows-1252
    #          data misinterpreted as UTF-8 — these are valid Unicode but often
    #          represent encoding errors in practice.

    #     Replaces problematic characters with the Unicode replacement character.
    #     """
    #     # First pass: handle surrogate escapes from file I/O
    #     value = value.encode("utf-8", errors="surrogateescape").decode(
    #         "utf-8", errors="replace"
    #     )
    #     # Second pass: strip C1 control characters (0x80-0x9F) which are
    #     # almost always encoding artifacts, not intentional content
    #     return value.translate(
    #         {c: "\ufffd" for c in range(0x80, 0xA0)}
    #     )

    # def _load_geojson_features(
    #     self,
    #     filepath: Path,
    #     target_table: str,
    #     conflict_key: list[str] | None = None,
    #     batch_size: int = 5_000,
    # ) -> int:
    #     """
    #     Read a GeoJSON file and ingest its features via engine.ingest_batch().

    #     Each GeoJSON Feature's properties are flattened into a dict.  The
    #     geometry is converted to WKT and stored under the target table's
    #     actual geometry column name (auto-detected from ``geometry_columns``).

    #     If the target table has no PostGIS geometry column the geometry is
    #     silently dropped.
    #     """
    #     with open(filepath, "r") as f:
    #         geojson = json.load(f)

    #     features = geojson.get("features", [])
    #     if not features:
    #         self.logger.warning("No features found in %s", filepath)
    #         return 0

    #     geom_col = self._detect_geometry_column(target_table)
    #     if geom_col:
    #         self.logger.info(
    #             "Detected geometry column '%s' in %s", geom_col, target_table
    #         )
    #     else:
    #         self.logger.warning(
    #             "No geometry column found in %s — geometry will be dropped",
    #             target_table,
    #         )

    #     total_rows = 0
    #     batch: list[dict[str, Any]] = []

    #     for feature in features:
    #         row = dict(feature.get("properties", {}))
    #         geom = feature.get("geometry")

    #         if geom is not None and geom_col is not None:
    #             row[geom_col] = self._geojson_to_wkt(geom)

    #         batch.append(row)

    #         if len(batch) >= batch_size:
    #             rows = self.engine.ingest_batch(
    #                 batch,
    #                 target_table,
    #                 conflict_column=conflict_key,
    #                 conflict_action="UPDATE" if conflict_key else "NOTHING",
    #             )
    #             total_rows += rows
    #             batch = []

    #     if batch:
    #         rows = self.engine.ingest_batch(
    #             batch,
    #             target_table,
    #             conflict_column=conflict_key,
    #             conflict_action="UPDATE" if conflict_key else "NOTHING",
    #         )
    #         total_rows += rows

    #     self.logger.info(
    #         "Loaded %d features from %s into %s", total_rows, filepath, target_table
    #     )
    #     return total_rows

    def _download_file(self, meta: SocrataTableMetadata) -> Path:
        url = meta.data_download_url
        suffix = ".geojson" if meta.is_geospatial else ".csv"
        filename = f"{meta.table_id}{suffix}"
        filepath = self.download_dir / filename

        resp = requests.get(url, stream=True, timeout=300)
        resp.raise_for_status()

        with open(filepath, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8192):
                f.write(chunk)

        return filepath

    def _ingest_csv_with_conflict_handling(
        self,
        filepath: Path,
        target_table: str,
        conflict_key: list[str] | None,
        mode: str,
    ) -> int:
        if mode == "replace":
            self.engine.execute(f"TRUNCATE TABLE {target_table}")
            return self.engine.ingest_csv(filepath, target_table)

        if mode == "append" or conflict_key is None:
            return self.engine.ingest_csv(filepath, target_table)

        return self._upsert_from_csv(filepath, target_table, conflict_key)

    def _upsert_from_csv(
        self,
        filepath: Path,
        target_table: str,
        conflict_key: list[str],
    ) -> int:
        staging_table = f"{target_table}_staging"

        col_query = f"""
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema || '.' || table_name = '{target_table}'
              AND column_name != 'ingested_at'
            ORDER BY ordinal_position
        """
        col_df = self.engine.query(col_query)
        all_columns = col_df["column_name"].tolist()

        conflict_cols = ", ".join(f'"{c}"' for c in conflict_key)
        update_cols = [c for c in all_columns if c not in conflict_key]
        update_set = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
        insert_cols = ", ".join(f'"{c}"' for c in all_columns)

        upsert_sql = f"""
            INSERT INTO {target_table} ({insert_cols}, "ingested_at")
            SELECT {insert_cols}, current_timestamp AT TIME ZONE 'UTC'
            FROM {staging_table}
            ON CONFLICT ({conflict_cols}) DO UPDATE SET
                {update_set},
                "ingested_at" = current_timestamp AT TIME ZONE 'UTC'
        """

        with self.engine.transaction():
            self.engine.execute(f"DROP TABLE IF EXISTS {staging_table}")
            self.engine.execute(
                f"CREATE TEMP TABLE {staging_table} (LIKE {target_table} EXCLUDING ALL)"
            )
            self.engine.execute(
                f'ALTER TABLE {staging_table} DROP COLUMN IF EXISTS "ingested_at"'
            )

            with open(filepath, "r") as f:
                with self.engine.connection.cursor() as cur:
                    cur.copy_expert(
                        f"COPY {staging_table} FROM STDIN WITH CSV HEADER", f
                    )

            count_df = self.engine.query(f"SELECT count(*) AS n FROM {staging_table}")
            staged_rows = int(count_df.iloc[0]["n"])
            self.engine.execute(upsert_sql)
            self.engine.execute(f"DROP TABLE IF EXISTS {staging_table}")

        return staged_rows

    def incremental_update(
        self,
        dataset_id: str,
        target_table: str,
        config: IncrementalConfig,
        high_water_mark_override: str | None = None,
    ) -> int:
        """
        Run an incremental paginated ingest with automatic high-water mark
        management and idempotent upsert.

        Args:
            dataset_id:               Socrata 4x4 identifier.
            target_table:             Fully-qualified table.
            config:                   IncrementalConfig.
            high_water_mark_override: Use instead of stored HWM.

        Returns:
            Total rows ingested.
        """
        hwm = high_water_mark_override or self.tracker.get_high_water_mark(
            self.SOURCE_NAME, dataset_id
        )
        if hwm:
            self.logger.info("Resuming from high_water_mark: %s", hwm)
        else:
            self.logger.info("No prior high_water_mark — full incremental load")

        meta = self._get_metadata(dataset_id)
        domain = meta.domain

        run_metadata = {
            "mode": "incremental",
            "incremental_column": config.incremental_column,
            "conflict_key": config.conflict_key,
            "prior_high_water_mark": hwm,
            "domain": domain,
        }

        with self.tracker.track(
            self.SOURCE_NAME, dataset_id, target_table, metadata=run_metadata
        ) as run:
            where_parts = []
            if hwm:
                where_parts.append(f"{config.incremental_column} > '{hwm}'")
            if config.where:
                where_parts.append(config.where)

            combined_where = " AND ".join(where_parts) if where_parts else None
            order_by = config.order_by or f"{config.incremental_column}, :id"

            total_rows = 0
            max_hwm = hwm

            for page_num, batch in enumerate(
                self.client.paginate(
                    domain=domain,
                    dataset_id=dataset_id,
                    columns=config.columns,
                    where=combined_where,
                    order_by=order_by,
                ),
                start=1,
            ):
                rows = self.engine.ingest_batch(
                    batch,
                    target_table,
                    conflict_column=config.conflict_key,
                    conflict_action="UPDATE",
                )
                total_rows += rows

                batch_max = self._extract_max_hwm(batch, config.incremental_column)
                if batch_max and (max_hwm is None or batch_max > max_hwm):
                    max_hwm = batch_max

                self.logger.info(
                    "Page %d: fetched %d, upserted %d (total: %d, hwm: %s)",
                    page_num,
                    len(batch),
                    rows,
                    total_rows,
                    max_hwm,
                )

            run.rows_ingested = total_rows
            run.high_water_mark = max_hwm

        return total_rows

    @staticmethod
    def _extract_max_hwm(batch: list[dict[str, Any]], column: str) -> Optional[str]:
        values = [row[column] for row in batch if row.get(column) is not None]
        if not values:
            return None
        return str(max(values))

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def preview(
        self,
        dataset_id: str,
        limit: int = 5,
        columns: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        meta = self._get_metadata(dataset_id)
        return self.client.query(
            domain=meta.domain,
            dataset_id=dataset_id,
            select=", ".join(columns) if columns else None,
            limit=limit,
        )

    def print_ddl(
        self,
        dataset_id: str,
        schema: str = "raw_data",
        table_name: str | None = None,
    ) -> None:
        meta = self._get_metadata(dataset_id)
        print(meta.generate_ddl(schema=schema, table_name=table_name))
