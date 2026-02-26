"""
Streaming OpenStreetMap PBF parser.

Reads .osm.pbf files element-by-element using pyosmium, converts geometry
to WKB hex via shapely, and yields batches of row dicts — matching the
interface of parse_geojson and parse_shapefile.

Supports three element types (one per call):
  - nodes:     Point geometries with tags as JSONB.
  - ways:      LineString/Polygon geometries with tags as JSONB.
                 Uses pyosmium's node location cache for geometry construction.
  - relations: No geometry reconstruction; stores member list as JSONB.

Usage:
    from parsers.pbf_parser import parse_pbf

    # Ingest nodes
    batches, result = parse_pbf(
        "illinois-latest.osm.pbf",
        element_type="nodes",
        geometry_column="geom",
    )
    with engine.staged_ingest("osm_nodes", "raw_data", ...) as stager:
        for batch in batches:
            stager.write_batch(batch)
    print(result.features_parsed, result.features_failed)

    # Ingest ways (needs node location cache — use location_storage
    # param to control memory vs disk tradeoff)
    batches, result = parse_pbf(
        "illinois-latest.osm.pbf",
        element_type="ways",
        geometry_column="geom",
        location_storage="sparse_file_array,/tmp/osm_node_cache.nodecache",
    )

    # Ingest relations (no geometry, just members + tags)
    batches, result = parse_pbf(
        "illinois-latest.osm.pbf",
        element_type="relations",
    )
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

VALID_ELEMENT_TYPES = ("nodes", "ways", "relations")


@dataclass
class ParseResult:
    """Accumulated metadata from a parse run."""

    features_parsed: int = 0
    failures: list[dict[str, Any]] = field(default_factory=list)
    element_type: str = ""

    @property
    def features_failed(self) -> int:
        return len(self.failures)


def parse_pbf(
    filepath: str | Path,
    element_type: str = "nodes",
    geometry_column: str = "geom",
    srid: int = 4326,
    batch_size: int = 5000,
    location_storage: str = "flex_mem",
) -> tuple[Iterator[list[dict[str, Any]]], ParseResult]:
    """
    Stream-parse an OSM PBF file into batches of row dicts.

    One element type per call. Geometry values are WKB hex strings
    (nodes and ways only). Tags are stored as a JSON string (for
    JSONB columns — use json.dumps with sort_keys=True for deterministic
    hashing in SCD2 pipelines).

    Args:
        filepath:          Path to .osm.pbf file.
        element_type:      One of "nodes", "ways", or "relations".
        geometry_column:   Name of the geometry column in the target table.
        srid:              Spatial reference ID for the WKB output.
        batch_size:        Rows per yielded batch.
        location_storage:  pyosmium index type for caching node locations
                           when parsing ways. The default "flex_mem" is good
                           for small-to-medium files. For large files on
                           memory-constrained machines, use a disk-backed
                           store, e.g.:
                             "sparse_file_array,/tmp/node_cache.nodecache"
                           Ignored for nodes and relations.

    Returns:
        (batch_iterator, result) — iterate the batches, then inspect result
        for failure/warning details.
    """
    if element_type not in VALID_ELEMENT_TYPES:
        raise ValueError(f"element_type must be one of {VALID_ELEMENT_TYPES}, got {element_type!r}")

    filepath = Path(filepath)
    result = ParseResult(element_type=element_type)

    if element_type == "nodes":
        return _generate_nodes(filepath, geometry_column, srid, batch_size, result), result
    elif element_type == "ways":
        return _generate_ways(
            filepath, geometry_column, srid, batch_size, location_storage, result
        ), result
    else:
        return _generate_relations(filepath, batch_size, result), result


def _generate_nodes(
    filepath: Path,
    geometry_column: str,
    srid: int,
    batch_size: int,
    result: ParseResult,
) -> Iterator[list[dict[str, Any]]]:
    import osmium
    from shapely import wkb as shapely_wkb
    from shapely.geometry import Point

    batch: list[dict[str, Any]] = []

    # Read only nodes from the file for efficiency.
    for obj in osmium.FileProcessor(str(filepath), osmium.osm.NODE):
        try:
            tags = dict(obj.tags)
            point = Point(obj.lon, obj.lat)
            wkb_hex = shapely_wkb.dumps(point, hex=True, srid=srid)

            row = {
                "osm_id": obj.id,
                "tags": json.dumps(tags, sort_keys=True),
                geometry_column: wkb_hex,
            }

            batch.append(row)
            result.features_parsed += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        except Exception as e:
            logger.debug("Skipping node %d: %s", obj.id, e)
            result.failures.append({"osm_id": obj.id, "error": str(e)})

    if batch:
        yield batch

    _log_summary("nodes", filepath, result)


def _generate_ways(
    filepath: Path,
    geometry_column: str,
    srid: int,
    batch_size: int,
    location_storage: str,
    result: ParseResult,
) -> Iterator[list[dict[str, Any]]]:
    import osmium
    from shapely import wkb as shapely_wkb
    from shapely.geometry import LineString, Polygon

    batch: list[dict[str, Any]] = []

    # We need all entities read from the file (nodes + ways) so the
    # location cache can see node coordinates, but we filter to only
    # process way objects in our loop.
    fp = osmium.FileProcessor(str(filepath)).with_locations(location_storage)

    for obj in fp:
        if not obj.is_way():
            continue

        try:
            tags = dict(obj.tags)

            coords = [(n.lon, n.lat) for n in obj.nodes if n.location.valid()]

            if len(coords) < 2:
                raise ValueError(f"Way has {len(coords)} valid coordinates, need at least 2")

            # Closed ways with 4+ coordinates (3+ unique points) become
            # polygons. Open ways become linestrings.
            if obj.is_closed() and len(coords) >= 4:
                geom = Polygon(coords)
            else:
                geom = LineString(coords)

            wkb_hex = shapely_wkb.dumps(geom, hex=True, srid=srid)

            row = {
                "osm_id": obj.id,
                "tags": json.dumps(tags, sort_keys=True),
                geometry_column: wkb_hex,
            }

            batch.append(row)
            result.features_parsed += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        except Exception as e:
            logger.debug("Skipping way %d: %s", obj.id, e)
            result.failures.append({"osm_id": obj.id, "error": str(e)})

    if batch:
        yield batch

    _log_summary("ways", filepath, result)


def _generate_relations(
    filepath: Path,
    batch_size: int,
    result: ParseResult,
) -> Iterator[list[dict[str, Any]]]:
    import osmium

    batch: list[dict[str, Any]] = []

    for obj in osmium.FileProcessor(str(filepath), osmium.osm.RELATION):
        try:
            tags = dict(obj.tags)

            members = [
                {
                    "type": m.type,
                    "ref": m.ref,
                    "role": m.role,
                }
                for m in obj.members
            ]

            row = {
                "osm_id": obj.id,
                "tags": json.dumps(tags, sort_keys=True),
                "members": json.dumps(members),
            }

            batch.append(row)
            result.features_parsed += 1

            if len(batch) >= batch_size:
                yield batch
                batch = []

        except Exception as e:
            logger.debug("Skipping relation %d: %s", obj.id, e)
            result.failures.append({"osm_id": obj.id, "error": str(e)})

    if batch:
        yield batch

    _log_summary("relations", filepath, result)


def _log_summary(element_type: str, filepath: Path, result: ParseResult) -> None:
    logger.info(
        "Parsed %d %s from %s",
        result.features_parsed,
        element_type,
        filepath.name,
    )
    if result.failures:
        logger.warning(
            "Skipped %d %s in %s (first error: %s)",
            result.features_failed,
            element_type,
            filepath.name,
            result.failures[0]["error"],
        )
