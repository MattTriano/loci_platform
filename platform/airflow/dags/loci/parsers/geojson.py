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

    @property
    def features_failed(self) -> int:
        return len(self.failures)


def parse_geojson(
    filepath: str | Path,
    geometry_column: str = "geom",
    srid: int = 4326,
    batch_size: int = 5000,
) -> tuple[Iterator[list[dict[str, Any]]], ParseResult]:
    """
    Stream-parse a GeoJSON file into batches of row dicts.

    Geometry values are WKB hex strings. Columns are discovered from
    the first valid feature. Features with extra properties are ingested
    (extras are omitted from the row but recorded in ParseResult).
    Features with missing properties get None for those keys.

    Args:
        filepath:        Path to .geojson file.
        geometry_column: Name of the geometry column in the target table.
        srid:            Spatial reference ID for the WKB output.
        batch_size:      Rows per yielded batch.

    Returns:
        (batch_iterator, result) — iterate the batches, then inspect result
        for failure/warning details.

    Usage:
        batches, result = parse_geojson("parcels.geojson", geometry_column="geom")

        with engine.staged_ingest("parcels", "raw_data", ...) as stager:
            for batch in batches:
                stager.write_batch(batch)

        print(result.features_parsed, result.features_failed)
        print(result.unknown_properties)
    """
    filepath = Path(filepath)
    result = ParseResult()

    def _generate() -> Iterator[list[dict[str, Any]]]:
        import ijson
        from shapely import wkb as shapely_wkb
        from shapely.geometry import shape

        columns: list[str] | None = None
        column_set: set[str] | None = None
        batch: list[dict[str, Any]] = []
        feature_index = 0

        with open(filepath, "rb") as f:
            for feat in ijson.items(f, "features.item"):
                try:
                    props = feat.get("properties") or {}
                    geom = feat.get("geometry")
                    if geom is None:
                        raise ValueError("Feature has no geometry")

                    wkb_hex = shapely_wkb.dumps(shape(geom), hex=True, srid=srid)

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
