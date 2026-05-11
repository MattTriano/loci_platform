# loci_platform/platform/airflow/dags/loci/exports/geojson_export.py
"""
Export mart tables to GeoJSON files for the bike map web app.

Each export config defines a mart table, the geometry source (either a
PostGIS geometry column or lat/lng columns), and which properties to
include. The exporter queries PostGIS, builds a GeoJSON FeatureCollection,
and writes it to disk.

Per-city export configs live on each city's CityBuildSpec; this module
is city-agnostic.

Usage from an Airflow task:

    from loci.exports.geojson_export import GeoJsonExporter

    exporter = GeoJsonExporter(engine, schema=cfg.marts_schema, output_dir=...)
    exporter.export_all(spec.geojson_exports)
"""

from __future__ import annotations

import json
import logging
import math
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from loci.db.core import PostgresEngine

logger = logging.getLogger(__name__)


@dataclass
class GeoJSONExportConfig:
    """Defines how to export a single mart table to GeoJSON.

    The schema is not stored here; it's resolved by the exporter from
    EnvConfig at run time so it can't drift between configs in a run.
    """

    name: str
    """Output filename (without .geojson extension)."""

    table: str
    """Mart table name."""

    geometry_column: str | None = None
    """PostGIS geometry column name. If set, coordinates are extracted from
    this column via ST_X/ST_Y (for points) or ST_AsGeoJSON (for other types)."""

    latitude_column: str | None = None
    """Column containing latitude (used when geometry_column is None)."""

    longitude_column: str | None = None
    """Column containing longitude (used when geometry_column is None)."""

    properties: list[str] | None = None
    """Columns to include as GeoJSON properties. None means all non-geometry
    columns."""

    where: str | None = None
    """Optional SQL WHERE clause (without the WHERE keyword)."""

    order_by: str | None = None
    """Optional SQL ORDER BY clause (without the ORDER BY keyword)."""

    limit: int | None = None
    """Optional row limit."""


class GeoJsonExporter:
    def __init__(self, engine: PostgresEngine, schema: str, output_dir: Path):
        self.engine = engine
        self.schema = schema
        self.output_dir = output_dir
        self.prep_output_dir()

    def prep_output_dir(self) -> None:
        self.output_dir = Path(self.output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def _build_query(self, cfg: GeoJSONExportConfig) -> str:
        """Build a SQL SELECT for the given export config."""
        select_cols = []

        if cfg.geometry_column:
            # Extract lon/lat from the PostGIS geometry for GeoJSON coordinates.
            # For point geometries, ST_X/ST_Y is simplest. For lines/polygons
            # we'd use ST_AsGeoJSON, but all current marts are points.
            select_cols.append(
                f"ST_X({cfg.geometry_column}) AS __lng, ST_Y({cfg.geometry_column}) AS __lat"
            )
        elif cfg.latitude_column and cfg.longitude_column:
            select_cols.append(f"{cfg.latitude_column} AS __lat, {cfg.longitude_column} AS __lng")
        else:
            raise ValueError(
                f"Export config '{cfg.name}' must specify either geometry_column "
                f"or both latitude_column and longitude_column."
            )

        if cfg.properties:
            for col in cfg.properties:
                select_cols.append(f'"{col}"')
        else:
            select_cols.append("*")

        sql = f"SELECT {', '.join(select_cols)} FROM {self.schema}.{cfg.table}"

        if cfg.where:
            sql += f" WHERE {cfg.where}"
        if cfg.order_by:
            sql += f" ORDER BY {cfg.order_by}"
        if cfg.limit:
            sql += f" LIMIT {cfg.limit}"

        return sql

    def _row_to_feature(self, row: dict[str, Any]) -> dict | None:
        """Convert a query result row to a GeoJSON Feature dict.

        Returns None if coordinates are missing or NaN.
        """
        lat = row.pop("__lat", None)
        lng = row.pop("__lng", None)

        # Reject rows with missing or non-finite coordinates
        if lat is None or lng is None:
            return None
        try:
            lat, lng = float(lat), float(lng)
        except (TypeError, ValueError):
            return None
        if math.isnan(lat) or math.isnan(lng) or math.isinf(lat) or math.isinf(lng):
            return None

        # Convert non-serializable types; null out NaN/Infinity floats
        properties = {}
        for k, v in row.items():
            if v is None:
                properties[k] = None
            elif isinstance(v, float):
                properties[k] = None if (math.isnan(v) or math.isinf(v)) else v
            elif isinstance(v, (str, int, bool)):
                properties[k] = v
            else:
                properties[k] = str(v)

        return {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [lng, lat],
            },
            "properties": properties,
        }

    def export_to_geojson(self, cfg: GeoJSONExportConfig) -> Path:
        """Export a single mart table to a GeoJSON file.

        Args:
            cfg:        Export configuration.

        Returns:
            Path to the written file.
        """
        output_path = self.output_dir / f"{cfg.name}.geojson"

        sql = self._build_query(cfg)
        logger.info("Exporting %s.%s → %s", self.schema, cfg.table, output_path)

        df = self.engine.query(sql)
        rows = df.to_dict("records")

        features = []
        skipped = 0
        for row in rows:
            feat = self._row_to_feature(row)
            if feat is not None:
                features.append(feat)
            else:
                skipped += 1

        collection = {
            "type": "FeatureCollection",
            "features": features,
        }

        with open(output_path, "w") as f:
            json.dump(collection, f, separators=(",", ":"))

        logger.info(
            "Wrote %s: %d features (%d skipped, no coordinates)",
            output_path.name,
            len(features),
            skipped,
        )

        return output_path

    def export_all(self, configs: list[GeoJSONExportConfig]) -> list[Path]:
        """Export all configured mart tables to GeoJSON files.

        Args:
            configs: List of export configurations.

        Returns:
            List of paths to written files.
        """
        paths = []
        errors = []
        for cfg in configs:
            try:
                paths.append(self.export_to_geojson(cfg))
            except Exception as e:
                logger.exception("Failed to export %s", cfg.name)
                errors.append((cfg.name, e))
        if errors:
            names = ", ".join(name for name, _ in errors)
            raise RuntimeError(f"Failed to export {len(errors)} layer(s): {names}")
        return paths
