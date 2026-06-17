# /loci_platform/platform/airflow/dags/loci/collectors/arcgishub/collector.py
"""
ArcGISHubCollector — collects data from ArcGIS Hub feature service layers
and ingests into Postgres via StagedIngest.

Usage:
    from loci.collectors.arcgis_hub.client import ArcGISHubClient
    from loci.collectors.arcgis_hub.metadata import ArcGISHubMetadata
    from loci.collectors.arcgis_hub.spec import ArcGISHubDatasetSpec
    from loci.collectors.arcgis_hub.collector import ArcGISHubCollector

    spec = ArcGISHubDatasetSpec(
        name="tps_arrests",
        item_id="4702e79fd2404f7d93dd9866f45d7ec2",
        target_table="tps_arrests",
        entity_key=["Event_Unique_Id"],
        incremental_column="last_edited_date",
    )

    client = ArcGISHubClient("https://data.tps.ca")
    collector = ArcGISHubCollector(client=client, engine=engine)
    summary = collector.collect(spec)
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from datetime import UTC, datetime
from typing import Any

from loci.collectors.arcgishub.client import ArcGISHubClient
from loci.collectors.arcgishub.metadata import ArcGISHubMetadata
from loci.collectors.arcgishub.spec import ArcGISHubDatasetSpec
from loci.collectors.exceptions import SchemaDriftError
from loci.tracking.ingestion_tracker import IngestionTracker

logger = logging.getLogger(__name__)


# ArcGIS field type -> Postgres type. Types not in this map are treated as text.
ARCGIS_TO_PG_TYPE = {
    "esriFieldTypeOID": "bigint",
    "esriFieldTypeInteger": "integer",
    "esriFieldTypeSmallInteger": "smallint",
    "esriFieldTypeBigInteger": "bigint",
    "esriFieldTypeDouble": "double precision",
    "esriFieldTypeSingle": "real",
    "esriFieldTypeString": "text",
    "esriFieldTypeDate": "timestamptz",
    "esriFieldTypeGUID": "text",
    "esriFieldTypeGlobalID": "text",
}

# ArcGIS geometry type -> PostGIS geometry type.
ARCGIS_TO_PG_GEOMETRY = {
    "esriGeometryPoint": "Point",
    "esriGeometryMultipoint": "MultiPoint",
    "esriGeometryPolyline": "MultiLineString",
    "esriGeometryPolygon": "MultiPolygon",
}

# Source field names that collide with the collector's geometry column.
# These are renamed to _orig_{name} in both DDL and row flattening.
GEOMETRY_COLLISION_NAMES = {"geom", "geog"}


class ArcGISHubCollector:
    """
    Collects ArcGIS Hub feature service data and ingests into Postgres.

    Parameters
    ----------
    client : ArcGISHubClient
    engine : PostgresEngine
    metadata : ArcGISHubMetadata, optional
        If not provided, one is constructed from `client`.
    tracker : IngestionTracker, optional
        If not provided, one is constructed from `engine`.
    logger : logging.Logger, optional
        If not provided, a module-level logger is used.
    """

    SOURCE_NAME = "arcgis_hub"

    METADATA_COLUMNS = {
        "ingested_at",
        "record_hash",
        "valid_from",
        "valid_to",
    }

    # Excluded from the SCD2 record hash: OBJECTID is not stable across
    # service refreshes and including it would cause spurious new versions.
    HASH_EXCLUDE_COLUMNS = METADATA_COLUMNS | {"objectid"}

    def __init__(
        self,
        client: ArcGISHubClient,
        engine: Any,
        metadata: ArcGISHubMetadata | None = None,
        tracker: IngestionTracker | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.client = client
        self.engine = engine
        self.metadata = metadata or ArcGISHubMetadata(client)
        self.tracker = tracker or IngestionTracker(engine=self.engine)
        self.logger = logger or logging.getLogger("arcgis_hub_collector")

        # Cache of layer info keyed by "item_id:layer_index"; populated on
        # first access so we don't re-hit the server for every helper call.
        self._layer_info_cache: dict[str, dict[str, Any]] = {}

        # Cache of /layers response keyed by service_url.
        self._layers_cache: dict[str, list[dict[str, Any]]] = {}

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def collect(self, spec: ArcGISHubDatasetSpec, force: bool = False) -> dict:
        """
        Collect and ingest the dataset defined by `spec`.

        Parameters
        ----------
        spec : ArcGISHubDatasetSpec
        force : bool
            If True, ignore incremental high-water mark and do a full
            refresh.

        Returns
        -------
        dict
            Summary with spec_name, rows_staged, rows_merged, errors.
        """
        summary = {
            "spec_name": spec.name,
            "rows_staged": 0,
            "rows_merged": 0,
            "errors": [],
        }

        if not force and self._already_ingested(spec):
            self.logger.info("Skipping %s (already current, not forced)", spec.name)
            summary["skipped"] = True
            return summary

        try:
            layers = self._resolve_layers(spec)

            total_staged = 0
            total_merged = 0
            latest_hwm: str | None = None

            for layer_index, layer_name in layers:
                layer_info = self._get_layer_info(spec, layer_index)
                where = self._build_where(spec, force)
                source_fields = self._source_field_names(layer_info, spec)
                self._preflight_column_check(source_fields, spec)

                dataset_id = f"{spec.item_id}:{layer_index}"
                fqn = f"{spec.target_schema}.{spec.target_table}"

                with self.tracker.track(
                    source=self.SOURCE_NAME,
                    dataset_id=dataset_id,
                    target_table=fqn,
                    metadata={
                        "item_id": spec.item_id,
                        "layer_index": layer_index,
                        "layer_name": layer_name,
                        "where": where,
                        "force": force,
                        "incremental_column": spec.incremental_column,
                    },
                ) as run:
                    staged, merged, new_hwm = self._ingest(
                        spec,
                        layer_info,
                        where,
                        layer_name,
                    )
                    run.rows_staged = staged
                    run.rows_merged = merged
                    if new_hwm is not None:
                        run.high_water_mark = new_hwm

                total_staged += staged
                total_merged += merged
                if new_hwm and (latest_hwm is None or new_hwm > latest_hwm):
                    latest_hwm = new_hwm

            summary["rows_staged"] = total_staged
            summary["rows_merged"] = total_merged

        except Exception as e:
            self.logger.error("Collection failed for %s: %s", spec.name, e)
            summary["errors"].append(str(e))
            raise

        self.logger.info("Collection complete for %s: %s", spec.name, summary)
        return summary

    def generate_ddl(self, spec: ArcGISHubDatasetSpec) -> str:
        """Generate a CREATE TABLE + constraint + index script for `spec`."""
        layers = self._resolve_layers(spec)
        layer_infos = [self._get_layer_info(spec, idx) for idx, _name in layers]

        if len(layer_infos) > 1:
            self._check_field_overlap(layer_infos, spec)

        return _build_ddl(spec, layer_infos)

    def print_ddl(self, spec: ArcGISHubDatasetSpec) -> None:
        """Print DDL for easy copy-paste into a migration."""
        print(self.generate_ddl(spec))

    # ------------------------------------------------------------------
    # Layer discovery
    # ------------------------------------------------------------------

    def _resolve_layers(self, spec: ArcGISHubDatasetSpec) -> list[tuple[int, str]]:
        """Return a list of (layer_index, layer_name) to collect.

        Handles int, list[int], and "all".
        """
        if isinstance(spec.layer_index, int):
            layer_info = self._get_layer_info(spec, spec.layer_index)
            name = layer_info.get("name", str(spec.layer_index))
            return [(spec.layer_index, name)]

        available = self._list_layers(spec)

        if spec.layer_index == "all":
            return available

        # list[int]
        available_by_id = {idx: name for idx, name in available}
        result = []
        for idx in spec.layer_index:
            if idx not in available_by_id:
                raise ValueError(
                    f"Layer {idx} not found in service. Available: {[i for i, _ in available]}"
                )
            result.append((idx, available_by_id[idx]))
        return result

    def _list_layers(self, spec: ArcGISHubDatasetSpec) -> list[tuple[int, str]]:
        """Fetch available layers from the /layers endpoint."""
        service_url = self._resolve_service_url(spec)

        if service_url in self._layers_cache:
            raw_layers = self._layers_cache[service_url]
        else:
            payload = self.client.get_json(f"{service_url}/layers", params={"f": "json"})
            if "error" in payload:
                err = payload["error"]
                raise RuntimeError(
                    f"ArcGIS /layers error: {err.get('code')} - {err.get('message')}"
                )
            raw_layers = payload.get("layers") or []
            self._layers_cache[service_url] = raw_layers

        return [(layer["id"], layer.get("name", str(layer["id"]))) for layer in raw_layers]

    def _check_field_overlap(
        self,
        layer_infos: list[dict[str, Any]],
        spec: ArcGISHubDatasetSpec,
    ) -> None:
        """Raise ValueError if field overlap across layers is too low."""
        field_sets = []
        for info in layer_infos:
            names = {f["name"].lower() for f in info["fields"]}
            field_sets.append(names)

        all_fields = set().union(*field_sets)
        shared_fields = set.intersection(*field_sets)

        if not all_fields:
            return

        overlap = len(shared_fields) / len(all_fields)
        if overlap < spec.min_field_overlap:
            raise ValueError(
                f"Field overlap across layers is {overlap:.0%} "
                f"({len(shared_fields)}/{len(all_fields)}), "
                f"below threshold of {spec.min_field_overlap:.0%}. "
                f"Only in some layers: {all_fields - shared_fields}"
            )

        self.logger.info(
            "Field overlap: %d/%d (%.0f%%) — %d fields only in some layers: %s",
            len(shared_fields),
            len(all_fields),
            overlap * 100,
            len(all_fields - shared_fields),
            all_fields - shared_fields or "none",
        )

    # ------------------------------------------------------------------
    # Layer info
    # ------------------------------------------------------------------

    def _get_layer_info(self, spec: ArcGISHubDatasetSpec, layer_index: int) -> dict[str, Any]:
        """Fetch and cache layer metadata (fields, geometry type, SRID, max record count).

        Tries the direct /{layer_index} endpoint first. If that returns
        an error, falls back to finding the layer in the /layers response.
        """
        cache_key = f"{spec.item_id}:{layer_index}"
        if cache_key in self._layer_info_cache:
            return self._layer_info_cache[cache_key]

        service_url = self._resolve_service_url(spec)
        layer_url = f"{service_url}/{layer_index}"

        layer = self.client.get_json(layer_url, params={"f": "json"})

        # ArcGIS returns errors as HTTP 200 with {"error": {...}}.
        if "error" in layer:
            self.logger.warning(
                "Direct layer endpoint %s returned error, falling back to /layers",
                layer_url,
            )
            layer = self._get_layer_from_layers_endpoint(spec, layer_index)

        fields = layer.get("fields") or []
        if not fields:
            raise RuntimeError(f"No fields found for layer {layer_index} at {service_url}")

        max_record_count = layer.get("maxRecordCount", 1000)
        geometry_type = layer.get("geometryType")
        spatial_ref = layer.get("spatialReference") or {}
        srid = spatial_ref.get("latestWkid") or spatial_ref.get("wkid") or 4326

        info = {
            "service_url": service_url,
            "layer_url": layer_url,
            "name": layer.get("name", str(layer_index)),
            "fields": fields,
            "max_record_count": max_record_count,
            "geometry_type": geometry_type,
            "srid": 4326,
            "native_srid": srid,  # keep for reference/logging
            "oid_field": _find_oid_field(fields),
            "date_fields": {f["name"] for f in fields if f.get("type") == "esriFieldTypeDate"},
        }

        self.logger.info(
            "Layer %s: %d fields, maxRecordCount=%d, geometry=%s, native_srid=%d (requesting 4326)",
            layer_url,
            len(fields),
            max_record_count,
            geometry_type,
            srid,
        )

        self._layer_info_cache[cache_key] = info
        return info

    def _get_layer_from_layers_endpoint(
        self, spec: ArcGISHubDatasetSpec, layer_index: int
    ) -> dict[str, Any]:
        """Find a specific layer in the /layers response."""
        available = self._list_layers(spec)
        service_url = self._resolve_service_url(spec)
        raw_layers = self._layers_cache[service_url]

        for layer in raw_layers:
            if layer["id"] == layer_index:
                return layer

        raise ValueError(
            f"Layer {layer_index} not found in /layers response. "
            f"Available: {[idx for idx, _ in available]}"
        )

    def _resolve_service_url(self, spec: ArcGISHubDatasetSpec) -> str:
        """Extract the feature service URL from the Hub item metadata."""
        item = self.metadata.get_dataset(spec.item_id)
        props = item.get("properties") or {}
        url = props.get("url")
        if not url:
            # Fall back to top-level url (different Hub versions organize
            # this field differently).
            url = item.get("url")
        if not url:
            raise ValueError(
                f"Could not find feature service URL in metadata for item {spec.item_id!r}"
            )
        return url.rstrip("/")

    @staticmethod
    def _source_field_names(
        layer_info: dict[str, Any],
        spec: ArcGISHubDatasetSpec,
    ) -> set[str]:
        """Names of fields the collector will produce in row dicts.

        Starts from the layer's field list, lowercased so downstream code
        (and Postgres) sees consistent casing. Renames geometry-colliding
        fields. Adds `geom` if the layer has geometry, and the layer column
        if configured.
        """
        has_geometry = bool(layer_info.get("geometry_type"))
        names: set[str] = set()

        for f in layer_info["fields"]:
            name = f["name"].lower()
            if has_geometry and name in GEOMETRY_COLLISION_NAMES:
                name = _rename_collision(name, names)
            names.add(name)

        if has_geometry:
            names.add("geom")
        if spec.layer_column:
            names.add(spec.layer_column.lower())
        return names

    # ------------------------------------------------------------------
    # Idempotency
    # ------------------------------------------------------------------

    def _already_ingested(self, spec: ArcGISHubDatasetSpec) -> bool:
        """Check if current data exists for this spec.

        In incremental mode, we never skip — incremental runs are
        always additive. In full-refresh mode (no incremental_column),
        we skip if any current rows exist.
        """
        if spec.incremental_column:
            return False

        fqn = f"{spec.target_schema}.{spec.target_table}"
        try:
            df = self.engine.query(f"select 1 from {fqn} limit 1")
            return not df.empty
        except Exception:
            # Table doesn't exist yet
            return False

    # ------------------------------------------------------------------
    # Schema drift
    # ------------------------------------------------------------------

    def _preflight_column_check(
        self,
        source_columns: set[str],
        spec: ArcGISHubDatasetSpec,
    ) -> None:
        """Raise SchemaDriftError if source has columns missing from target."""
        try:
            table_columns = self._get_table_columns(spec.target_table, spec.target_schema)
        except Exception:
            # Table doesn't exist yet — DDL hasn't been applied. Let the
            # actual COPY fail with a clearer error.
            return

        data_columns_in_table = {c.lower() for c in table_columns} - self.METADATA_COLUMNS

        new_in_source = source_columns - data_columns_in_table
        missing_from_source = data_columns_in_table - source_columns

        if missing_from_source:
            self.logger.warning(
                "Columns in %s.%s but not in source: %s",
                spec.target_schema,
                spec.target_table,
                missing_from_source,
            )

        if new_in_source:
            raise SchemaDriftError(
                f"Source has columns not in {spec.target_schema}.{spec.target_table}: "
                f"{new_in_source}. Add them via migration, then re-run."
            )

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

    # ------------------------------------------------------------------
    # Incremental
    # ------------------------------------------------------------------

    def _build_where(self, spec: ArcGISHubDatasetSpec, force: bool) -> str:
        """Combine spec.where with an incremental filter if applicable."""
        base = spec.where or "1=1"

        if force or not spec.incremental_column:
            return base

        hwm_epoch_ms = self._get_high_water_mark(spec)
        if hwm_epoch_ms is None:
            self.logger.info("No prior high-water mark for %s; doing full refresh", spec.name)
            return base

        self.logger.info(
            "Resuming %s from %s > %d (%s)",
            spec.name,
            spec.incremental_column,
            hwm_epoch_ms,
            datetime.fromtimestamp(hwm_epoch_ms / 1000, tz=UTC).isoformat(),
        )
        incremental = f"{spec.incremental_column} > {hwm_epoch_ms}"
        return f"({base}) AND ({incremental})"

    def _get_high_water_mark(self, spec: ArcGISHubDatasetSpec) -> int | None:
        """Return max(incremental_column) from target table as epoch ms, or None."""
        fqn = f"{spec.target_schema}.{spec.target_table}"
        col = spec.incremental_column
        try:
            where_clause = ""
            if spec.entity_key:
                where_clause = 'where "valid_to" is null and'
            else:
                where_clause = "where"
            df = self.engine.query(
                f'select extract(epoch from max("{col}")) * 1000 as hwm_ms '
                f'from {fqn} {where_clause} "{col}" is not null'
            )
            if df.empty:
                return None
            val = df["hwm_ms"].iloc[0]
            return int(val) if val is not None else None
        except Exception:
            return None

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    def _ingest(
        self,
        spec: ArcGISHubDatasetSpec,
        layer_info: dict[str, Any],
        where: str,
        layer_name: str,
    ) -> tuple[int, int, str | None]:
        """Paginate the feature service, flatten rows, and stage them.

        Returns (rows_staged, rows_merged, new_high_water_mark_iso).
        """
        staged_ingest_kwargs: dict[str, Any] = {
            "metadata_columns": self.METADATA_COLUMNS,
            "hash_exclude_columns": self.HASH_EXCLUDE_COLUMNS,
        }
        if spec.entity_key:
            staged_ingest_kwargs["entity_key"] = spec.entity_key

        new_hwm_iso: str | None = None

        with self.engine.staged_ingest(
            target_table=spec.target_table,
            target_schema=spec.target_schema,
            **staged_ingest_kwargs,
        ) as stager:
            for page_num, batch in enumerate(
                self._paginate_features(spec, layer_info, where), start=1
            ):
                if not batch:
                    continue

                rows = [self._flatten_feature(feat, layer_info, spec, layer_name) for feat in batch]
                stager.write_batch(rows)

                # Track high-water mark progress across pages
                if spec.incremental_column:
                    batch_hwm = _max_iso(rows, spec.incremental_column)
                    if batch_hwm and (new_hwm_iso is None or batch_hwm > new_hwm_iso):
                        new_hwm_iso = batch_hwm

                self.logger.info(
                    "Page %d: fetched %d (staged total: %d)",
                    page_num,
                    len(batch),
                    stager.rows_staged,
                )

        return stager.rows_staged, stager.rows_merged, new_hwm_iso

    def _paginate_features(
        self,
        spec: ArcGISHubDatasetSpec,
        layer_info: dict[str, Any],
        where: str,
    ) -> Iterator[list[dict[str, Any]]]:
        """Yield pages of raw ArcGIS features from the layer query endpoint.

        Uses the server-advertised maxRecordCount. Orders by OBJECTID to
        get stable paging (ArcGIS does not guarantee a stable order
        otherwise, which can cause duplicate or missed rows under
        concurrent writes).
        """
        query_url = f"{layer_info['layer_url']}/query"
        max_records = layer_info["max_record_count"]
        oid_field = layer_info["oid_field"] or "OBJECTID"

        out_fields = ",".join(spec.out_fields) if spec.out_fields else "*"

        offset = 0
        while True:
            params = {
                "f": "json",
                "where": where,
                "outFields": out_fields,
                "returnGeometry": "true",
                "outSR": layer_info["srid"],
                "orderByFields": oid_field,
                "resultOffset": offset,
                "resultRecordCount": max_records,
            }
            payload = self.client.get_json(query_url, params=params)

            # ArcGIS returns errors as a nested {"error": {...}} payload
            # with HTTP 200. Detect and raise.
            if "error" in payload:
                err = payload["error"]
                raise RuntimeError(f"ArcGIS query error {err.get('code')}: {err.get('message')}")

            features = payload.get("features") or []
            if not features:
                return

            yield features

            # Hub feature services set exceededTransferLimit=true when there
            # are more pages. Fall back to len-based check for older servers.
            if payload.get("exceededTransferLimit") is False:
                return
            if len(features) < max_records:
                return
            offset += len(features)

    def _flatten_feature(
        self,
        feat: dict[str, Any],
        layer_info: dict[str, Any],
        spec: ArcGISHubDatasetSpec,
        layer_name: str,
    ) -> dict[str, Any]:
        """Convert one ArcGIS feature into a row dict.

        - Attribute names are lowercased.
        - Source fields that collide with the geometry column name are
          renamed (e.g. "geom" -> "_orig_geom").
        - Date fields (epoch ms) are converted to ISO UTC strings.
        - Geometry is converted to EWKT with the layer's SRID.
        - Layer name is added if layer_column is configured.
        """
        attrs = feat.get("attributes") or {}
        srid = layer_info["srid"]
        date_fields_lower = {f.lower() for f in layer_info["date_fields"]}
        has_geometry = bool(layer_info.get("geometry_type"))

        row: dict[str, Any] = {}
        for k, v in attrs.items():
            key = k.lower()
            if has_geometry and key in GEOMETRY_COLLISION_NAMES:
                key = _rename_collision(key, set(row.keys()))
            if key in date_fields_lower and v is not None:
                row[key] = _epoch_ms_to_iso(v)
            else:
                row[key] = v

        geometry_type = layer_info.get("geometry_type")
        if geometry_type:
            row["geom"] = _geometry_to_ewkt(feat.get("geometry"), geometry_type, srid)

        if spec.layer_column:
            row[spec.layer_column.lower()] = layer_name

        return row


# ----------------------------------------------------------------------
# Module-level helpers
# ----------------------------------------------------------------------


def _find_oid_field(fields: list[dict[str, Any]]) -> str | None:
    for f in fields:
        if f.get("type") == "esriFieldTypeOID":
            return f["name"]
    return None


def _rename_collision(name: str, existing: set[str]) -> str:
    """Rename a field that collides with the geometry column.

    Tries _orig_{name}, then _orig_{name}_2, etc.
    """
    candidate = f"_orig_{name}"
    if candidate not in existing:
        return candidate
    n = 2
    while f"{candidate}_{n}" in existing:
        n += 1
    return f"{candidate}_{n}"


def _epoch_ms_to_iso(val: Any) -> str | None:
    """Convert an ArcGIS date (epoch milliseconds) to an ISO UTC string."""
    if val is None:
        return None
    try:
        return datetime.fromtimestamp(int(val) / 1000, tz=UTC).isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def _max_iso(rows: list[dict[str, Any]], column: str) -> str | None:
    col = column.lower()
    values = [r.get(col) for r in rows if r.get(col) is not None]
    return max(values) if values else None


# def _geometry_to_ewkt(geom: dict[str, Any] | None, geometry_type: str, srid: int) -> str | None:
#     """Convert an ArcGIS geometry object to an EWKT string.

#     Handles Point, Multipoint, Polyline, and Polygon. Unknown types
#     return None and are logged by the caller if needed.
#     """
#     if not geom:
#         return None

#     prefix = f"SRID={srid};"

#     if geometry_type == "esriGeometryPoint":
#         x, y = geom.get("x"), geom.get("y")
#         if x is None or y is None:
#             return None
#         return f"{prefix}POINT({x} {y})"

#     if geometry_type == "esriGeometryMultipoint":
#         pts = geom.get("points") or []
#         if not pts:
#             return None
#         inner = ", ".join(f"({p[0]} {p[1]})" for p in pts)
#         return f"{prefix}MULTIPOINT({inner})"

#     if geometry_type == "esriGeometryPolyline":
#         paths = geom.get("paths") or []
#         if not paths:
#             return None
#         parts = []
#         for path in paths:
#             coords = ", ".join(f"{p[0]} {p[1]}" for p in path)
#             parts.append(f"({coords})")
#         return f"{prefix}MULTILINESTRING({', '.join(parts)})"

#     if geometry_type == "esriGeometryPolygon":
#         rings = geom.get("rings") or []
#         if not rings:
#             return None
#         parts = []
#         for ring in rings:
#             coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
#             parts.append(f"(({coords}))")
#         return f"{prefix}MULTIPOLYGON({', '.join(parts)})"

#     return None


def _geometry_to_ewkt(geom: dict[str, Any] | None, geometry_type: str, srid: int) -> str | None:
    if not geom:
        return None

    prefix = f"SRID={srid};"

    if geometry_type == "esriGeometryPoint":
        x, y = geom.get("x"), geom.get("y")
        if x is None or y is None:
            return None
        return f"{prefix}POINT({x} {y})"

    if geometry_type == "esriGeometryMultipoint":
        pts = geom.get("points") or []
        if not pts:
            return None
        inner = ", ".join(f"({p[0]} {p[1]})" for p in pts)
        return f"{prefix}MULTIPOINT({inner})"

    if geometry_type == "esriGeometryPolyline":
        paths = geom.get("paths") or []
        if not paths:
            return None
        parts = []
        for path in paths:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in path)
            parts.append(f"({coords})")
        return f"{prefix}MULTILINESTRING({', '.join(parts)})"

    if geometry_type == "esriGeometryPolygon":
        rings = geom.get("rings") or []
        if not rings:
            return None

        # ArcGIS convention: outer rings are clockwise, holes are
        # counter-clockwise. Group each hole with the preceding
        # outer ring.
        polygons: list[list[str]] = []
        for ring in rings:
            coords = ", ".join(f"{p[0]} {p[1]}" for p in ring)
            if _ring_is_clockwise(ring):
                # New outer ring starts a new polygon
                polygons.append([f"({coords})"])
            else:
                # Hole — attach to the current polygon
                if polygons:
                    polygons[-1].append(f"({coords})")
                else:
                    # Hole before any outer ring; treat as outer
                    polygons.append([f"({coords})"])

        parts = [f"({', '.join(ring_list)})" for ring_list in polygons]
        return f"{prefix}MULTIPOLYGON({', '.join(parts)})"

    return None


def _ring_is_clockwise(ring: list[list[float]]) -> bool:
    """Check if a ring is clockwise using the shoelace formula sign.

    ArcGIS convention: clockwise = outer ring, counter-clockwise = hole.
    """
    total = 0.0
    for i in range(len(ring) - 1):
        x1, y1 = ring[i][0], ring[i][1]
        x2, y2 = ring[i + 1][0], ring[i + 1][1]
        total += (x2 - x1) * (y2 + y1)
    return total > 0


def _pg_type_for(field: dict[str, Any]) -> str:
    return ARCGIS_TO_PG_TYPE.get(field.get("type"), "text")


def _build_ddl(
    spec: ArcGISHubDatasetSpec,
    layer_infos: list[dict[str, Any]],
) -> str:
    """Produce CREATE TABLE + unique constraint + partial index for spec.

    When multiple layer_infos are provided, produces the union of all
    fields. Fields only present in some layers will be nullable.
    """
    fqn = f"{spec.target_schema}.{spec.target_table}"

    # Collect fields across all layers. For type conflicts, the first
    # layer's type wins (they should be consistent given the overlap check).
    # Track which fields appear in all layers vs. only some.
    all_field_sets = []
    merged_fields: dict[str, dict[str, Any]] = {}  # lowered name -> field dict
    has_geometry = False
    geometry_type = None
    srid = 4326

    for info in layer_infos:
        layer_names: set[str] = set()
        for f in info["fields"]:
            name = f["name"].lower()
            layer_names.add(name)
            if name not in merged_fields:
                merged_fields[name] = f
        all_field_sets.append(layer_names)

        if info.get("geometry_type"):
            has_geometry = True
            geometry_type = info["geometry_type"]
            srid = info["srid"]

    shared_fields = set.intersection(*all_field_sets) if all_field_sets else set()

    columns: list[str] = []
    seen: set[str] = set()
    for name, f in merged_fields.items():
        if name in seen:
            continue
        # Rename fields that collide with the geometry column
        col_name = name
        if has_geometry and name in GEOMETRY_COLLISION_NAMES:
            col_name = _rename_collision(name, seen)
        seen.add(col_name)
        columns.append(f'"{col_name}" {_pg_type_for(f)}')

    # Geometry column
    if has_geometry:
        pg_geom = ARCGIS_TO_PG_GEOMETRY.get(geometry_type, "Geometry")
        if "geom" not in seen:
            columns.append(f'"geom" geometry({pg_geom}, {srid})')

    # Layer column
    if spec.layer_column:
        col = spec.layer_column.lower()
        if col not in seen:
            columns.append(f'"{col}" text')

    # Provenance column — always included regardless of mode
    columns.append("\"ingested_at\" timestamptz not null default (now() at time zone 'UTC')")

    # SCD2 versioning columns — only when entity_key is defined
    if spec.entity_key:
        columns.extend(
            [
                '"record_hash" text not null',
                "\"valid_from\" timestamptz not null default (now() at time zone 'UTC')",
                '"valid_to" timestamptz',
            ]
        )

    lines = [f"create table {fqn} ("]
    lines.append("    " + ",\n    ".join(columns))
    lines.append(");")

    # Unique constraint on (entity_key, record_hash)
    if spec.entity_key:
        ek_cols = ", ".join(f'"{c.lower()}"' for c in spec.entity_key)
        constraint_name = f"uq_{spec.target_table}_entity_hash"
        lines.append("")
        lines.append(f"alter table {fqn}")
        lines.append(f"    add constraint {constraint_name}")
        lines.append(f'    unique ({ek_cols}, "record_hash");')

        index_name = f"ix_{spec.target_table}_current"
        lines.append("")
        lines.append(f"create index {index_name}")
        lines.append(f"    on {fqn} ({ek_cols})")
        lines.append('    where "valid_to" is null;')

    # GIST index on geometry
    if has_geometry:
        lines.append("")
        lines.append(f"create index ix_{spec.target_table}_geom")
        lines.append(f'    on {fqn} using gist ("geom");')

    return "\n".join(lines)
