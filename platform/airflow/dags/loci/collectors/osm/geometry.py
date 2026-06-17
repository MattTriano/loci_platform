# /loci_platform/platform/airflow/dags/loci/collectors/osm/geometry.py
"""
Geometry assembly for OSM elements.

Converts OSM element dicts (as returned by the Overpass API with
`out geom;`) into Shapely geometries, then to WKT strings ready for
ingestion into a PostGIS geometry column.

The rules:
- node                       -> Point
- open way                   -> LineString
- closed way                 -> Polygon if area-tagged, else LineString
- relation type=multipolygon -> Polygon or MultiPolygon (assembled)
- relation other             -> None (not currently materialized)

Closed-way polygon detection follows the canonical heuristic from
https://github.com/tyrasd/osm-polygon-features. An explicit `area=yes`
or `area=no` tag overrides everything else.
"""

from __future__ import annotations

import logging

from shapely.geometry import LineString, MultiPolygon, Point, Polygon
from shapely.ops import linemerge

logger = logging.getLogger(__name__)


# ----------------------------------------------------------------------
# Area-tag rules (from osm-polygon-features)
# ----------------------------------------------------------------------

# A way with one of these tag keys is a polygon if its value passes the
# rule. Rule formats:
#   ("all", None)            -> any value (other than "no") makes it an area
#   ("whitelist", {values})  -> only these values make it an area
#   ("blacklist", {values})  -> any value EXCEPT these makes it an area
_AREA_RULES: dict[str, tuple[str, frozenset[str] | None]] = {
    "building": ("all", None),
    "building:part": ("all", None),
    "landuse": ("all", None),
    "amenity": ("all", None),
    "leisure": ("all", None),
    "area": ("all", None),
    "boundary": ("all", None),
    "place": ("all", None),
    "shop": ("all", None),
    "tourism": ("all", None),
    "historic": ("all", None),
    "public_transport": ("all", None),
    "office": ("all", None),
    "military": ("all", None),
    "ruins": ("all", None),
    "area:highway": ("all", None),
    "craft": ("all", None),
    "golf": ("all", None),
    "indoor": ("all", None),
    "highway": ("whitelist", frozenset({"services", "rest_area", "escape", "elevator"})),
    "waterway": ("whitelist", frozenset({"riverbank", "dock", "boatyard", "dam"})),
    "barrier": (
        "whitelist",
        frozenset({"city_wall", "ditch", "hedge", "retaining_wall", "wall", "spikes"}),
    ),
    "railway": ("whitelist", frozenset({"station", "turntable", "roundhouse", "platform"})),
    "power": ("whitelist", frozenset({"plant", "substation", "generator", "transformer"})),
    "natural": ("blacklist", frozenset({"coastline", "cliff", "ridge", "arete", "tree_row"})),
    "man_made": ("blacklist", frozenset({"cutline", "embankment", "pipeline"})),
    "aeroway": ("blacklist", frozenset({"taxiway"})),
}


def is_area(tags: dict[str, str]) -> bool:
    """
    Decide whether a closed way should be treated as a Polygon.

    The explicit `area` tag takes precedence: `area=yes` -> polygon,
    `area=no` -> linestring. Otherwise, walk the tag rules from
    osm-polygon-features.
    """
    area_tag = tags.get("area")
    if area_tag == "yes":
        return True
    if area_tag == "no":
        return False

    for key, (rule_type, values) in _AREA_RULES.items():
        if key not in tags:
            continue
        value = tags[key]
        if value == "no":
            continue

        if rule_type == "all":
            return True
        if rule_type == "whitelist" and value in values:
            return True
        if rule_type == "blacklist" and value not in values:
            return True

    return False


# ----------------------------------------------------------------------
# Element -> geometry
# ----------------------------------------------------------------------


def element_to_wkt(element: dict) -> str | None:
    """
    Convert an Overpass JSON element to a WKT geometry string, or None
    if no geometry can be assembled.

    Logs a warning and returns None on assembly failures (broken
    multipolygon relations, etc.) — never raises.
    """
    try:
        geom = _element_to_shape(element)
    except Exception as exc:
        logger.warning(
            "Failed to build geometry for %s/%s: %s",
            element.get("type"),
            element.get("id"),
            exc,
        )
        return None

    if geom is None:
        return None

    return geom.wkt


def _element_to_shape(element: dict):
    """Dispatch on element type. Returns a Shapely geometry or None."""
    elem_type = element.get("type")
    tags = element.get("tags") or {}

    if elem_type == "node":
        return _node_point(element)

    if elem_type == "way":
        return _way_geometry(element, tags)

    if elem_type == "relation":
        if tags.get("type") == "multipolygon":
            return _multipolygon_from_relation(element)
        # Other relation types (route, boundary, etc.) get NULL geometry.
        return None

    logger.warning("Unknown element type: %r", elem_type)
    return None


def _node_point(element: dict) -> Point | None:
    lon = element.get("lon")
    lat = element.get("lat")
    if lon is None or lat is None:
        return None
    return Point(lon, lat)


def _way_geometry(element: dict, tags: dict) -> Polygon | LineString | None:
    coords = _coords_from_geometry(element.get("geometry") or [])
    if len(coords) < 2:
        return None

    closed = coords[0] == coords[-1]
    if closed and len(coords) >= 4 and is_area(tags):
        return Polygon(coords)
    return LineString(coords)


def _coords_from_geometry(geometry: list[dict]) -> list[tuple[float, float]]:
    """Extract (lon, lat) pairs from an Overpass `geometry` array."""
    return [(p["lon"], p["lat"]) for p in geometry if "lon" in p and "lat" in p]


# ----------------------------------------------------------------------
# Multipolygon assembly
# ----------------------------------------------------------------------


def _multipolygon_from_relation(element: dict) -> Polygon | MultiPolygon | None:
    """
    Assemble a Polygon or MultiPolygon from a multipolygon relation.

    Steps:
        1. Group member ways by role into outer/inner LineStrings.
        2. linemerge each group to close split rings.
        3. Build outer Polygons.
        4. Assign each inner ring as a hole to the outer that contains it.
        5. Return Polygon (single outer) or MultiPolygon (multiple).

    Raises on assembly failure; the caller (element_to_wkt) catches and
    logs.
    """
    members = element.get("members") or []
    outer_lines: list[LineString] = []
    inner_lines: list[LineString] = []

    for member in members:
        if member.get("type") != "way":
            continue
        coords = _coords_from_geometry(member.get("geometry") or [])
        if len(coords) < 2:
            continue
        line = LineString(coords)
        role = member.get("role")
        if role == "outer" or role == "":
            outer_lines.append(line)
        elif role == "inner":
            inner_lines.append(line)
        # Other roles ignored.

    if not outer_lines:
        raise ValueError("multipolygon relation has no outer ways")

    outer_rings = _close_rings(outer_lines)
    inner_rings = _close_rings(inner_lines) if inner_lines else []

    if not outer_rings:
        raise ValueError("multipolygon relation has no closed outer rings")

    # Build outer Polygons (no holes yet) so we can use them for containment tests.
    outer_polys = [Polygon(ring) for ring in outer_rings]

    # Assign each inner ring to the first outer that contains it.
    holes_per_outer: list[list[list[tuple[float, float]]]] = [[] for _ in outer_polys]
    for inner in inner_rings:
        inner_poly = Polygon(inner)
        assigned = False
        for i, outer_poly in enumerate(outer_polys):
            if outer_poly.contains(inner_poly):
                holes_per_outer[i].append(inner)
                assigned = True
                break
        if not assigned:
            logger.warning(
                "Inner ring not contained by any outer in relation %s; dropping it",
                element.get("id"),
            )

    final_polys = [
        Polygon(shell=outer_rings[i], holes=holes_per_outer[i]) for i in range(len(outer_polys))
    ]

    if len(final_polys) == 1:
        return final_polys[0]
    return MultiPolygon(final_polys)


def _close_rings(lines: list[LineString]) -> list[list[tuple[float, float]]]:
    """
    Merge a set of LineStrings into closed rings.

    Uses shapely.ops.linemerge to join lines that share endpoints.
    Lines that are already closed are kept as-is. Lines that can't be
    closed (dangling endpoints) are skipped with a warning.

    Returns a list of ring coordinate sequences.
    """
    if not lines:
        return []

    merged = linemerge(lines)

    # linemerge returns a single LineString or a MultiLineString.
    if isinstance(merged, LineString):
        candidates = [merged]
    else:
        candidates = list(merged.geoms)

    rings: list[list[tuple[float, float]]] = []
    for line in candidates:
        coords = list(line.coords)
        if len(coords) < 4:
            continue
        if coords[0] != coords[-1]:
            logger.warning("Open ring after linemerge (skipped): %d points", len(coords))
            continue
        rings.append(coords)

    return rings
