# /loci_platform/platform/airflow/dags/loci/collectors/ckan/collector.py
from __future__ import annotations

import csv
import logging
import re
from pathlib import Path
from typing import Any

from loci.collectors.ckan.client import CKANClient
from loci.collectors.ckan.metadata import CKANResource
from loci.collectors.ckan.spec import CKANDatasetSpec
from loci.collectors.exceptions import SchemaDriftError
from loci.parsers.csv_parser import parse_csv
from loci.parsers.geojson import parse_geojson
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


class CKANCollector:
    """
    High-level orchestrator for CKAN dataset ingestion.

    Usage:
        collector = CKANCollector(engine=engine)

        # Collect (CKAN is full-refresh-only; force is accepted for interface
        # parity with the other collectors but does not change behavior):
        spec = CKANDatasetSpec(
            name="food_inspections",
            base_url="https://data.cityofchicago.org",
            dataset_id="4ijn-s7e5",
            target_table="food_inspections",
            entity_key=["inspection_id"],
            resource_format="CSV",
        )
        summary = collector.collect(spec, force=True)

        # Generate DDL for a new table:
        print(collector.generate_ddl(spec))
    """

    SOURCE_NAME = "ckan"

    # Columns that CKAN adds internally — never part of the actual data.
    # _id: auto-increment row ID, reset on every DataPusher re-import.
    # _full_text: tsvector column used for full-text search in DataStore.
    CKAN_INTERNAL_COLUMNS = {"_id", "_full_text"}

    PIPELINE_COLUMNS = {
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }

    # CKAN DataStore type -> Postgres DDL type
    DATASTORE_TYPE_MAP = {
        "text": "text",
        "int": "integer",
        "int4": "integer",
        "int8": "bigint",
        "float": "double precision",
        "float8": "double precision",
        "numeric": "numeric",
        "bool": "boolean",
        "json": "jsonb",
        "jsonb": "jsonb",
        "date": "date",
        "time": "time",
        "timestamp": "timestamptz",
        "timestamptz": "timestamptz",
    }

    def __init__(
        self,
        engine: Any,
        tracker: IngestionTracker | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.engine = engine
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.logger = logger or logging.getLogger("ckan_collector")

        self._client_cache: dict[str, CKANClient] = {}

    def _get_client(self, base_url: str) -> CKANClient:
        """Return a cached CKANClient for the given portal URL."""
        if base_url not in self._client_cache:
            self._client_cache[base_url] = CKANClient(base_url)
        return self._client_cache[base_url]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: CKANDatasetSpec, force: bool = False) -> dict:
        """
        Collect a dataset and merge into the target table.

        CKAN collection is full-refresh-only — it downloads resource files
        and SCD2-merges, with no incremental path — so `force` is accepted
        for interface parity with the other collectors but does not change
        behavior: every run is a full refresh.

        Returns a summary dict (spec_name, mode, rows_merged).
        """
        if not force:
            self.logger.info(
                "%s: CKAN collection is full-refresh-only; running a full refresh "
                "(force is ignored).",
                spec.name,
            )
        rows_merged = self.full_refresh(spec)
        summary = {
            "spec_name": spec.name,
            "mode": "full_refresh",
            "rows_merged": rows_merged,
        }
        self.logger.info("Collection complete for %r: %s", spec.name, summary)
        return summary

    # ------------------------------------------------------------------
    # Resource resolution
    # ------------------------------------------------------------------

    def _resolve_resources(self, spec: CKANDatasetSpec) -> list[CKANResource]:
        """Resolve the spec's resource selection to a list of CKANResource objects.

        If resource_ids is set, fetches each resource by ID.
        Otherwise, filters by resource_format on the dataset.
        Raises ValueError if no resources are found.
        """
        client = self._get_client(spec.base_url)
        meta = client.metadata

        if spec.resource_ids:
            resources = [meta.get_resource(rid) for rid in spec.resource_ids]
        else:
            resources = meta.find_resources(spec.dataset_id, spec.resource_format)

        if not resources:
            fmt = spec.resource_format or "(by ID)"
            raise ValueError(
                f"No resources found for dataset {spec.dataset_id!r} "
                f"with format {fmt} on {spec.base_url}"
            )

        self.logger.info(
            "Resolved %d resource(s) for %s: %s",
            len(resources),
            spec.dataset_id,
            [r.id for r in resources],
        )
        return resources

    # ------------------------------------------------------------------
    # Full refresh
    # ------------------------------------------------------------------

    def full_refresh(self, spec: CKANDatasetSpec) -> int:
        """
        Download all matching resources, parse them, and ingest via staged_ingest.

        If the spec has an entity_key, uses SCD2 merge. Otherwise, the
        staging table is merged with INSERT ... ON CONFLICT DO NOTHING
        (append-only — no entity key means no way to identify duplicates
        beyond the full row hash).

        Returns the number of rows merged into the target table.
        """
        client = self._get_client(spec.base_url)
        resources = self._resolve_resources(spec)
        fqn = f"{spec.target_schema}.{spec.target_table}"

        run_metadata = {
            "mode": "full_refresh",
            "base_url": spec.base_url,
            "dataset_id": spec.dataset_id,
            "resource_ids": [r.id for r in resources],
            "resource_formats": [r.format for r in resources],
            "entity_key": spec.entity_key,
        }

        staged_ingest_kwargs: dict[str, Any] = {}
        if spec.entity_key:
            staged_ingest_kwargs["entity_key"] = spec.entity_key

        with self.tracker.track(
            self.SOURCE_NAME, spec.dataset_id, fqn, metadata=run_metadata
        ) as run:
            with self.engine.staged_ingest(
                target_table=spec.target_table,
                target_schema=spec.target_schema,
                metadata_columns=self.PIPELINE_COLUMNS,
                hash_exclude_columns=self.PIPELINE_COLUMNS | self.CKAN_INTERNAL_COLUMNS,
                **staged_ingest_kwargs,
            ) as stager:
                for resource in resources:
                    self._ingest_resource(client, resource, stager, spec)

            run.rows_staged = stager.rows_staged
            run.rows_merged = stager.rows_merged
            run.rows_ingested = stager.rows_merged

        return stager.rows_merged

    def _ingest_resource(
        self,
        client: CKANClient,
        resource: CKANResource,
        stager: Any,
        spec: CKANDatasetSpec,
    ) -> None:
        """Download and parse a single resource, writing batches to the stager."""
        fmt = (resource.format or "").upper()
        if not resource.url:
            raise ValueError(f"Resource {resource.id} has no download URL")

        suffix = client._suffix_for_format(fmt)
        filepath = client.download_to_tempfile(resource.url, suffix=suffix)

        try:
            if fmt == "GEOJSON":
                geometry_column = self.engine._get_geometry_column(
                    spec.target_table, spec.target_schema
                )
                batches, geojson_result = parse_geojson(filepath, geometry_column=geometry_column)
            else:
                batches = parse_csv(filepath)

            table_columns = self._get_table_columns(spec.target_table, spec.target_schema)
            name_map: dict[str, str] | None = None

            for batch in batches:
                batch = self._strip_ckan_internal_columns(batch)

                # Build the raw -> normalized name map from the first batch's keys
                # and reuse it for all subsequent batches from this resource.
                if name_map is None and batch:
                    name_map = self._normalize_column_names(list(batch[0].keys()))
                    # Validate the resource's columns against the table before
                    # writing. Warn-first: flip raise_on_drift=True to enforce.
                    self._preflight_column_check(
                        set(name_map.values()), spec.target_table, spec.target_schema
                    )

                if name_map:
                    batch = self._rename_batch_keys(batch, name_map)

                batch = self._filter_to_table_columns(batch, table_columns)
                stager.write_batch(batch)

        finally:
            filepath.unlink(missing_ok=True)

    # ------------------------------------------------------------------
    # Schema drift
    # ------------------------------------------------------------------

    def _preflight_column_check(
        self,
        source_columns: set[str],
        target_table: str,
        target_schema: str,
        raise_on_drift: bool = False,
    ) -> None:
        """Compare a resource's normalized columns against the target table.

        Logs columns present in the table but absent from the source. For
        columns present in the source but not the table (drift), raises
        SchemaDriftError when raise_on_drift is True, or logs a warning when
        False (the warn-first default). Skips silently when the table does
        not exist yet — staged_ingest will surface that.
        """
        table_columns = self._get_table_columns(target_table, target_schema)
        if not table_columns:
            return

        data_columns_in_table = {c.lower() for c in table_columns} - self.PIPELINE_COLUMNS
        source_columns = source_columns - self.PIPELINE_COLUMNS - self.CKAN_INTERNAL_COLUMNS

        new_in_source = source_columns - data_columns_in_table
        missing_from_source = data_columns_in_table - source_columns

        if missing_from_source:
            self.logger.warning(
                "Columns in %s.%s but not in source: %s",
                target_schema,
                target_table,
                missing_from_source,
            )

        if new_in_source:
            message = (
                f"Source has columns not in {target_schema}.{target_table}: "
                f"{new_in_source}. Add them via migration, then re-run."
            )
            if raise_on_drift:
                raise SchemaDriftError(message)
            self.logger.warning("Schema drift (warn-only): %s", message)

    # ------------------------------------------------------------------
    # Table column helpers
    # ------------------------------------------------------------------

    def _get_table_columns(self, target_table: str, target_schema: str) -> set[str]:
        df = self.engine.query(
            """
            select column_name
            from information_schema.columns
            where table_schema = %(schema)s and table_name = %(table)s
            """,
            {"schema": target_schema, "table": target_table},
        )
        return set(df["column_name"])

    def _filter_to_table_columns(
        self, rows: list[dict[str, Any]], table_columns: set[str]
    ) -> list[dict[str, Any]]:
        dropped = set()
        for row in rows:
            extra = set(row.keys()) - table_columns
            for k in extra:
                del row[k]
            dropped.update(extra)
        if dropped:
            self.logger.warning("Dropped columns not in target table: %s", dropped)
        return rows

    @staticmethod
    def _strip_ckan_internal_columns(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Remove CKAN internal columns (_id, _full_text) from rows."""
        for row in rows:
            for col in CKANCollector.CKAN_INTERNAL_COLUMNS:
                row.pop(col, None)
        return rows

    @staticmethod
    def _rename_batch_keys(
        rows: list[dict[str, Any]], name_map: dict[str, str]
    ) -> list[dict[str, Any]]:
        """Rename dict keys in each row using the raw -> normalized mapping.

        Keys not in the mapping are dropped (they produced empty normalized
        names, e.g. a header of whitespace only).
        """
        return [{name_map[k]: v for k, v in row.items() if k in name_map} for row in rows]

    @classmethod
    def _exclude_internal(cls, names: set[str]) -> set[str]:
        """Remove CKAN internal column names from a set of column names."""
        return names - cls.CKAN_INTERNAL_COLUMNS

    @staticmethod
    def _normalize_column_name(raw: str) -> str:
        """Normalize a single column name: lowercase, non-alphanumerics -> '_', trim '_'."""
        return re.sub(r"[^a-z0-9]+", "_", raw.lower()).strip("_")

    def _normalize_column_names(self, raw_names: list[str]) -> dict[str, str]:
        """Build an ordered raw -> normalized mapping with collision handling.

        When two raw names normalize to the same value, the later one is
        suffixed with '_2', '_3', etc. A warning is logged listing the raw
        names involved.

        Empty normalized names (e.g. from a header of '   ') are skipped.
        """
        mapping: dict[str, str] = {}
        taken: dict[str, list[str]] = {}  # normalized -> [raw names that produced it]

        for raw in raw_names:
            base = self._normalize_column_name(raw)
            if not base:
                continue

            if base not in taken:
                taken[base] = [raw]
                mapping[raw] = base
                continue

            # Collision: suffix _2, _3, ...
            taken[base].append(raw)
            suffix = len(taken[base])
            candidate = f"{base}_{suffix}"
            # Very unlikely but possible: the suffixed name also clashes.
            while candidate in taken:
                suffix += 1
                candidate = f"{base}_{suffix}"
            taken[candidate] = [raw]
            mapping[raw] = candidate

        collisions = {n: rs for n, rs in taken.items() if len(rs) > 1}
        if collisions:
            self.logger.warning("Column name collisions after normalization: %s", collisions)

        return mapping

    # ------------------------------------------------------------------
    # DDL generation
    # ------------------------------------------------------------------

    def generate_ddl(self, spec: CKANDatasetSpec) -> str:
        """
        Generate a CREATE TABLE statement for a CKANDatasetSpec.

        Column discovery strategy:
        1. If any resolved resource has DataStore enabled, use DataStore
           field metadata (gives us column names and types).
        2. Otherwise, download a sample of the first resource file and
           read CSV headers (all columns typed as text).

        If the spec has an entity_key, SCD2 columns (record_hash, valid_from,
        valid_to) and a unique constraint + partial index are included.
        """
        client = self._get_client(spec.base_url)
        resources = self._resolve_resources(spec)

        columns = self._discover_columns(client, resources, spec)
        self._validate_columns_across_resources(client, resources, spec, columns)

        return self._build_ddl(spec, columns)

    def _discover_columns(
        self,
        client: CKANClient,
        resources: list[CKANResource],
        spec: CKANDatasetSpec,
    ) -> list[tuple[str, str]]:
        """Return a list of (column_name, pg_type) for the dataset.

        Tries DataStore first, falls back to file header scanning.
        Column names are normalized: lowercased, non-alphanumerics replaced
        with '_', collisions suffixed with '_2', '_3', etc.
        """
        # Try DataStore on the first resource
        first = resources[0]
        if first.datastore_active or client.metadata.has_datastore(first.id):
            self.logger.info("Using DataStore fields for column discovery on %s", first.id)
            raw_columns = self._columns_from_datastore(client, first.id)
        else:
            self.logger.info("DataStore not available, scanning file for column discovery")
            raw_columns = self._columns_from_file(client, first)

        name_map = self._normalize_column_names([name for name, _ in raw_columns])
        # Preserve order and types; drop any that normalized to empty.
        return [(name_map[name], pg_type) for name, pg_type in raw_columns if name in name_map]

    def _columns_from_datastore(
        self, client: CKANClient, resource_id: str
    ) -> list[tuple[str, str]]:
        """Get columns from DataStore field metadata.

        CKAN internal columns (_id, _full_text) are excluded — they are
        DataStore implementation details, not part of the dataset.
        """
        fields = client.metadata.get_datastore_fields(resource_id)
        columns = []
        for field in fields:
            name = field["id"]
            if name in self.CKAN_INTERNAL_COLUMNS:
                continue
            ckan_type = field.get("type", "text")
            pg_type = self.DATASTORE_TYPE_MAP.get(ckan_type, "text")
            columns.append((name, pg_type))
        return columns

    def _columns_from_file(
        self, client: CKANClient, resource: CKANResource
    ) -> list[tuple[str, str]]:
        """Download a resource file and infer columns from headers.

        For CSV: reads the header row, all columns typed as text.
        For GeoJSON: reads the first feature's properties, all typed as text,
        plus a 'geom' geometry column.
        """
        fmt = (resource.format or "").upper()
        suffix = client._suffix_for_format(fmt)
        filepath = client.download_to_tempfile(resource.url, suffix=suffix)

        try:
            if fmt == "GEOJSON":
                return self._columns_from_geojson_file(filepath)
            else:
                return self._columns_from_csv_file(filepath)
        finally:
            filepath.unlink(missing_ok=True)

    def _columns_from_csv_file(self, filepath: Path) -> list[tuple[str, str]]:
        """Read CSV headers and return (name, 'text') pairs.

        CKAN internal columns (_id, _full_text) are excluded — they can
        appear in CSV exports from CKAN's DataStore.
        """
        with open(filepath, encoding="utf-8", newline="") as f:
            reader = csv.reader(f)
            headers = next(reader)
        return [
            (h.strip(), "text")
            for h in headers
            if h.strip() and h.strip() not in self.CKAN_INTERNAL_COLUMNS
        ]

    def _columns_from_geojson_file(self, filepath: Path) -> list[tuple[str, str]]:
        """Read the first GeoJSON feature's properties for column names.

        CKAN internal columns (_id, _full_text) are excluded — they can
        appear in GeoJSON exports from CKAN's DataStore.
        """
        import ijson

        with open(filepath, "rb") as f:
            for feat in ijson.items(f, "features.item"):
                props = feat.get("properties") or {}
                columns = [(k, "text") for k in props.keys() if k not in self.CKAN_INTERNAL_COLUMNS]
                columns.append(("geom", "geometry"))
                return columns

        raise ValueError(f"No features found in {filepath}")

    def _validate_columns_across_resources(
        self,
        client: CKANClient,
        resources: list[CKANResource],
        spec: CKANDatasetSpec,
        columns: list[tuple[str, str]],
    ) -> None:
        """Check that all resources have the same columns. Fails hard on mismatch."""
        if len(resources) <= 1:
            return

        expected_names = {name for name, _ in columns}

        for resource in resources[1:]:
            if resource.datastore_active or client.metadata.has_datastore(resource.id):
                fields = client.metadata.get_datastore_fields(resource.id)
                raw_names = [f["id"] for f in fields if f["id"] not in self.CKAN_INTERNAL_COLUMNS]
                other_names = set(self._normalize_column_names(raw_names).values())
            else:
                # For file-based resources, we'd need to download and check headers.
                # Only do this if the resource format is CSV (cheap to read one line).
                fmt = (resource.format or "").upper()
                if fmt in ("CSV", "TSV"):
                    suffix = client._suffix_for_format(fmt)
                    filepath = client.download_to_tempfile(resource.url, suffix=suffix)
                    try:
                        with open(filepath, encoding="utf-8", newline="") as f:
                            reader = csv.reader(f)
                            headers = next(reader)
                        raw_names = [
                            h.strip()
                            for h in headers
                            if h.strip() and h.strip() not in self.CKAN_INTERNAL_COLUMNS
                        ]
                        other_names = set(self._normalize_column_names(raw_names).values())
                    finally:
                        filepath.unlink(missing_ok=True)
                else:
                    self.logger.warning(
                        "Cannot validate columns for non-CSV resource %s (%s), skipping",
                        resource.id,
                        resource.format,
                    )
                    continue

            if other_names != expected_names:
                extra = other_names - expected_names
                missing = expected_names - other_names
                raise ValueError(
                    f"Column mismatch between resources. "
                    f"Resource {resource.id} has extra columns {extra} "
                    f"and is missing columns {missing} compared to {resources[0].id}."
                )

    def _build_ddl(
        self,
        spec: CKANDatasetSpec,
        columns: list[tuple[str, str]],
    ) -> str:
        """Assemble the CREATE TABLE DDL string."""
        fqn = f"{spec.target_schema}.{spec.target_table}"

        col_defs = []
        for name, pg_type in columns:
            if pg_type == "geometry":
                col_defs.append(f'    "{name}" geometry')
            else:
                col_defs.append(f'    "{name}" {pg_type}')

        # Ingestion metadata
        col_defs.append(
            "    \"ingested_at\" timestamptz not null default (now() at time zone 'UTC')"
        )

        # SCD2 columns (always included — entity_key controls whether the
        # unique constraint and index are added)
        if spec.entity_key:
            col_defs.append('    "record_hash" text not null')
            col_defs.append(
                "    \"valid_from\" timestamptz not null default (now() at time zone 'UTC')"
            )
            col_defs.append('    "valid_to" timestamptz')

        ddl = f"create table if not exists {fqn} (\n"
        ddl += ",\n".join(col_defs)
        ddl += "\n);\n"

        if spec.entity_key:
            ek_cols = ", ".join(f'"{c}"' for c in spec.entity_key)
            constraint_name = f"uq_{spec.target_table}_entity_hash"
            index_name = f"ix_{spec.target_table}_current"

            ddl += (
                f"\nalter table {fqn}\n"
                f"    add constraint {constraint_name}\n"
                f'    unique ({ek_cols}, "record_hash");\n'
            )
            ddl += (
                f"\ncreate index if not exists {index_name}\n"
                f"    on {fqn} ({ek_cols})\n"
                f"    where valid_to is null;\n"
            )

        return ddl

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def preview(
        self,
        spec: CKANDatasetSpec,
        limit: int = 5,
    ) -> list[dict[str, Any]]:
        """Return a few rows from the first resolved resource.

        Tries DataStore first for a quick preview. Falls back to
        downloading the file and reading the first few rows.
        """
        client = self._get_client(spec.base_url)
        resources = self._resolve_resources(spec)
        first = resources[0]

        if first.datastore_active or client.metadata.has_datastore(first.id):
            return client.datastore_search(first.id, limit=limit)

        # File fallback: download and parse just enough rows. Branch on format
        # so GeoJSON isn't parsed as CSV.
        fmt = (first.format or "CSV").upper()
        suffix = client._suffix_for_format(fmt)
        filepath = client.download_to_tempfile(first.url, suffix=suffix)
        try:
            if fmt == "GEOJSON":
                batches, _ = parse_geojson(filepath, geometry_column="geom")
            else:
                batches = parse_csv(filepath)

            rows = []
            for batch in batches:
                rows.extend(batch)
                if len(rows) >= limit:
                    break
            return rows[:limit]
        finally:
            filepath.unlink(missing_ok=True)

    def print_ddl(self, spec: CKANDatasetSpec) -> None:
        """Generate and print DDL for easy copy-paste into a migration script."""
        print(self.generate_ddl(spec))
