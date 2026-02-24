from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from functools import cached_property

import requests

from loci.collectors.config import SCD2Config


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
        "location": "geometry(Point, 4326)",
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

        self._catalog_metadata: dict | None = None
        self._views_metadata: dict | None = None
        self._columns: list[SocrataColumnInfo] | None = None

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
        url = self.VIEWS_API_TEMPLATE.format(domain=self.domain, dataset_id=self.dataset_id)
        self.logger.info("Fetching views metadata for %s from %s", self.dataset_id, self.domain)
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json()

    @cached_property
    def columns(self) -> list[SocrataColumnInfo]:
        if self._columns is None:
            self._columns = self._parse_columns()
        return self._columns

    @cached_property
    def column_rename_map(self) -> dict[str, str]:
        return {col.name: col.field_name for col in self.columns if col.name != col.field_name}

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
            if col.get("fieldName", "").startswith(":@computed_region"):
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

        self.logger.info("Parsed %d columns from catalog API for %s", len(parsed), self.dataset_id)
        return parsed

    def generate_ddl(
        self,
        schema: str = "raw_data",
        table_name: str | None = None,
        include_ingested_at: bool = True,
        include_comments: bool = True,
        scd2_config: SCD2Config | None = None,
    ) -> str:
        """
        Generate a CREATE TABLE statement from the dataset's Socrata metadata.

        Args:
            schema:           Target schema name.
            table_name:       Target table name. Defaults to a sanitized version
                              of the dataset name from the metadata.
            include_ingested_at: If True, appends an 'ingested_at' column.
            include_comments: If True, adds COMMENT ON COLUMN statements for
                              columns that have descriptions in the metadata.
            scd2_config:      If provided, adds record_hash, valid_from, valid_to
                              columns plus a unique constraint and current-version
                              index based on the entity key.

        Returns:
            The full DDL string.
        """
        SYSTEM_COLUMNS = [
            '"socrata_id" text',
            '"socrata_updated_at" timestamptz',
            '"socrata_created_at" timestamptz',
            '"socrata_version" text',
        ]

        SCD2_COLUMNS = [
            '"record_hash" text not null',
            "\"valid_from\" timestamptz not null default (now() at time zone 'utc')",
            '"valid_to" timestamptz',
        ]
        if table_name is None:
            table_name = self._default_table_name()

        fqn = f"{schema}.{table_name}"
        col_defs = [col.ddl_fragment for col in self.columns]

        if include_ingested_at:
            col_defs.append(
                "    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC')"
            )

        col_defs.extend(f"    {c}" for c in SYSTEM_COLUMNS)

        if scd2_config:
            col_defs.extend(f"    {c}" for c in SCD2_COLUMNS)

        ddl = f"create table if not exists {fqn} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);\n"

        if scd2_config:
            ek_cols = ", ".join(scd2_config.entity_key)
            constraint_name = f"uq_{table_name}_entity_hash"
            index_name = f"ix_{table_name}_current"

            ddl += (
                f"\nalter table {fqn}\n"
                f"    add constraint {constraint_name}\n"
                f"    unique ({ek_cols}, record_hash);\n"
            )
            ddl += (
                f"\ncreate index if not exists {index_name}\n"
                f"    on {fqn} ({ek_cols})\n"
                f"    where valid_to is null;\n"
            )

        if include_comments:
            for col in self.columns:
                if col.description:
                    escaped = col.description.replace("'", "''")
                    ddl += f"\ncomment on column {fqn}.\"{col.field_name}\" is '{escaped}';"

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
            (not self.has_data_columns) and (self.has_geo_type_view or self.has_map_type_display)
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
