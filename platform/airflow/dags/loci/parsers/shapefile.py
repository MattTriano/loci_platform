"""
Streaming shapefile parser.

Reads shapefiles feature-by-feature using fiona, converts geometry to
WKB hex via shapely, and yields batches of row dicts — matching the
interface of parse_geojson and parse_csv.

Handles zipped shapefiles directly (fiona supports reading from .zip).

Usage:
    from parsers.shapefile_parser import parse_shapefile

    batches, result = parse_shapefile(
        "tl_2024_17_tract.zip",
        geometry_column="geom",
    )

    with engine.staged_ingest("tracts", "raw_data", ...) as stager:
        for batch in batches:
            stager.write_batch(batch)

    print(result.features_parsed, result.features_failed)
"""

from __future__ import annotations

import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ParseResult:
    """Accumulated metadata from a parse run."""

    features_parsed: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)
    unknown_properties: dict[str, int] = field(default_factory=dict)
    source_crs: str | None = None

    @property
    def features_failed(self) -> int:
        return len(self.failures)


def parse_shapefile(
    filepath: str | Path,
    geometry_column: str = "geom",
    srid: int = 4326,
    batch_size: int = 5000,
    layer: str | None = None,
    lowercase_columns: bool = True,
) -> tuple[Iterator[list[dict[str, Any]]], ParseResult]:
    """
    Stream-parse a shapefile into batches of row dicts.

    Geometry values are WKB hex strings. Reads feature-by-feature via
    fiona so memory usage stays bounded regardless of file size.

    Supports reading directly from .zip files (e.g. tl_2024_17_tract.zip).

    Args:
        filepath:           Path to .shp or .zip file.
        geometry_column:    Name of the geometry column in the target table.
        srid:               Spatial reference ID for the WKB output.
        batch_size:         Rows per yielded batch.
        layer:              Layer name within the file (for multi-layer archives).
                            If None, reads the first/only layer.
        lowercase_columns:  If True, lowercase all property column names.
                            TIGER shapefiles use uppercase (STATEFP, GEOID, etc.)
                            which you may want to normalize.

    Returns:
        (batch_iterator, result) — iterate the batches, then inspect result
        for failure/warning details.
    """
    filepath = Path(filepath)
    result = ParseResult()

    def _generate() -> Iterator[list[dict[str, Any]]]:
        import fiona
        from shapely import wkb as shapely_wkb
        from shapely.geometry import (
            LineString as ShapelyLineString,
        )
        from shapely.geometry import (
            MultiLineString,
            MultiPoint,
            MultiPolygon,
            shape,
        )
        from shapely.geometry import (
            Point as ShapelyPoint,
        )
        from shapely.geometry import (
            Polygon as ShapelyPolygon,
        )

        # fiona reads .zip files with a "zip://" prefix
        if filepath.suffix == ".zip":
            path_str = f"zip://{filepath}"
        else:
            path_str = str(filepath)

        open_kwargs = {}
        if layer is not None:
            open_kwargs["layer"] = layer

        columns: list[str] | None = None
        column_set: set[str] | None = None
        batch: list[dict[str, Any]] = []
        feature_index = 0

        with fiona.open(path_str, "r", **open_kwargs) as src:
            # Record source CRS for reference
            if src.crs:
                result.source_crs = str(src.crs)
                logger.info(
                    "Shapefile CRS: %s (%d features reported)",
                    result.source_crs,
                    len(src),
                )

            for feat in src:
                try:
                    props = dict(feat.get("properties") or {})
                    geom = feat.get("geometry")
                    if geom is None:
                        raise ValueError("Feature has no geometry")

                    geom_shape = shape(geom)

                    # Promote single geometries to Multi so they match
                    # Multi-typed PostGIS columns (which is the safe default
                    # since shapefiles often mix single and multi geometries)
                    if isinstance(geom_shape, ShapelyPoint):
                        geom_shape = MultiPoint([geom_shape])
                    elif isinstance(geom_shape, ShapelyLineString):
                        geom_shape = MultiLineString([geom_shape])
                    elif isinstance(geom_shape, ShapelyPolygon):
                        geom_shape = MultiPolygon([geom_shape])

                    wkb_hex = shapely_wkb.dumps(
                        geom_shape,
                        hex=True,
                        srid=srid,
                    )

                    if lowercase_columns:
                        props = {k.lower(): v for k, v in props.items()}

                    # Lock in columns from the first valid feature
                    if columns is None:
                        columns = list(props.keys()) + [geometry_column]
                        column_set = set(props.keys())
                        logger.info(
                            "Discovered %d columns from first feature in %s",
                            len(columns),
                            filepath.name,
                        )

                    # Check for unknown properties
                    for key in props:
                        if key not in column_set:
                            result.unknown_properties[key] = (
                                result.unknown_properties.get(key, 0) + 1
                            )

                    # Build row: known property columns + geometry
                    row = {c: props.get(c) for c in columns[:-1]}
                    row[geometry_column] = wkb_hex

                    batch.append(row)
                    result.features_parsed += 1

                    if len(batch) >= batch_size:
                        yield batch
                        batch = []

                except Exception as e:
                    logger.debug("Skipping feature %d: %s", feature_index, e)
                    result.failures.append(
                        {
                            "index": feature_index,
                            "error": str(e),
                        }
                    )

                feature_index += 1

        if batch:
            yield batch

        if result.failures:
            logger.warning(
                "Skipped %d of %d features in %s (first error: %s)",
                result.features_failed,
                feature_index,
                filepath.name,
                result.failures[0]["error"],
            )

        if result.unknown_properties:
            logger.warning(
                "Found %d unknown properties in %s: %s",
                len(result.unknown_properties),
                filepath.name,
                list(result.unknown_properties.keys()),
            )

    return _generate(), result
