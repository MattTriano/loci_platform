# loci_platform/platform/airflow/dags/loci/exports/graph_export.py
"""
Export a stress-weighted routing graph to a gzip-compressed binary file.

Reads <city>_bike_stress_weighted_segments (one row per undirected segment)
and expands each segment into one or two directed edges based on its
direction column.

The output format is documented in services/routing/docs/graph-format.md
and consumed by the Rust routing-core library (Lambda + CLI).

The exporter uses a `nx.MultiDiGraph` as scratch storage so that
parallel segments (multiple OSM ways connecting the same pair of
intersection nodes — divided highways, frontage roads, bike paths
running parallel to a road) are preserved instead of overwriting each
other. The pure helpers `build_node_table`, `build_edges_and_strings`,
and `build_segment_geometries` operate on a MultiDiGraph and are
testable without a database.
"""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path
from typing import Any

import networkx as nx
from loci.db.core import PostgresEngine
from loci.exports.graph_format import (
    NULL_STR_IDX,
    WriteEdge,
    WriteNode,
    WriteSegmentGeometry,
    f32_or_nan,
    write_graph,
)

logger = logging.getLogger(__name__)

# Output S3 key for the binary format. Kept here so callers
# (loci.environments, the Airflow deploy task, tofu env vars) can
# import a single constant rather than duplicating the path string.
GRAPH_S3_KEY = "graph/routing_graph.bin.gz"


class RoutingGraphExporter:
    """Builds a stress-weighted routing graph and writes it to a local file.

    Parameters
    ----------
    engine : PostgresEngine
    city : str
    marts_schema : str
        Schema where <city>_bike_stress_weighted_segments lives.
    batch_size : int
    min_component_size : int
        Weakly connected components smaller than this are dropped.
    """

    _SEGMENT_QUERY = """
        select
            segment_id,
            start_node_id,
            end_node_id,
            direction,
            name,
            highway,
            infra_type,
            length_m,
            physical_cost,
            crash_cost,
            intersection_cost_at_start,
            intersection_cost_at_end,
            elevation_cost_forward,
            elevation_cost_backward,
            start_is_intersection,
            end_is_intersection,
            highway_class,
            infra_tier,
            base_stress_per_meter,
            surface_penalty,
            enclosed_penalty,
            lighting_penalty,
            ST_AsGeoJSON(ST_Simplify(geom, 0.00005)) as geom_geojson,
            ST_X(ST_StartPoint(geom)) as start_lon,
            ST_Y(ST_StartPoint(geom)) as start_lat,
            ST_X(ST_EndPoint(geom)) as end_lon,
            ST_Y(ST_EndPoint(geom)) as end_lat
        from {marts_schema}.{city}_bike_stress_weighted_segments
        where physical_cost is not null
        order by way_id, start_position
    """

    # Floor for the A* heuristic. Computed from per-meter components
    # only — intersection_cost is per-endpoint and is NOT included
    # here. If it were, the heuristic could overestimate distance-only
    # cost for edges arriving at cheap intersections, breaking
    # admissibility.
    _HEURISTIC_FLOOR_QUERY = """
        select min((physical_cost + coalesce(crash_cost, 0)) / length_m) as floor
        from {marts_schema}.{city}_bike_stress_weighted_segments
        where length_m > 0 and physical_cost is not null
    """

    def __init__(
        self,
        engine: PostgresEngine,
        city: str,
        marts_schema: str,
        batch_size: int = 50_000,
        min_component_size: int = 75,
    ):
        self.engine = engine
        self.city = city
        self.marts_schema = marts_schema
        self.batch_size = batch_size
        self.min_component_size = min_component_size

    def export(self, output_path: Path) -> Path:
        output_path = Path(output_path)
        logger.info("Building routing graph → %s", output_path)

        G = self._build_graph()
        G = self._filter_small_components(G)

        logger.info(
            "Serializing graph to binary format (%d nodes, %d edges)",
            G.number_of_nodes(),
            G.number_of_edges(),
        )
        self._write_binary(G, output_path)

        size_mb = output_path.stat().st_size / 1_048_576
        logger.info("Wrote %s (%.1f MB compressed)", output_path, size_mb)
        return output_path

    # ------------------------------------------------------------------
    # Graph construction
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_geojson_coords(geom_geojson: str | None) -> tuple | None:
        """Parse a GeoJSON LineString's coordinates as a tuple of (lon, lat) tuples."""
        if not geom_geojson:
            return None
        try:
            geom = json.loads(geom_geojson)
            coords = geom.get("coordinates")
            if coords is None:
                return None
            return tuple((c[0], c[1]) for c in coords)
        except (json.JSONDecodeError, TypeError, IndexError):
            return None

    def _build_graph(self) -> nx.MultiDiGraph:
        """Stream segments and expand into directed edges.

        Uses MultiDiGraph as scratch storage so parallel segments
        (multiple ways between the same pair of intersection nodes)
        are preserved. The component-filtering step uses
        `nx.weakly_connected_components`, which works identically on
        MultiDiGraph. The final serialization is in `_write_binary`.

        Geometry is stored once per undirected segment in
        G.graph['segment_geometry'] keyed by segment_id (string).
        """
        query = self._SEGMENT_QUERY.format(city=self.city, marts_schema=self.marts_schema)

        G: nx.MultiDiGraph = nx.MultiDiGraph()
        segment_geometry: dict[str, tuple] = {}
        G.graph["segment_geometry"] = segment_geometry

        segment_count = 0
        edge_count = 0

        for batch in self.engine.query_batches(query, batch_size=self.batch_size):
            for row in batch:
                start_node = row["start_node_id"]
                end_node = row["end_node_id"]
                direction = row["direction"]
                segment_id = row["segment_id"]

                geom_coords = self._parse_geojson_coords(row["geom_geojson"])
                if geom_coords is not None:
                    segment_geometry[segment_id] = geom_coords

                if start_node not in G:
                    G.add_node(
                        start_node,
                        x=row["start_lon"],
                        y=row["start_lat"],
                        is_intersection=bool(row["start_is_intersection"]),
                    )
                if end_node not in G:
                    G.add_node(
                        end_node,
                        x=row["end_lon"],
                        y=row["end_lat"],
                        is_intersection=bool(row["end_is_intersection"]),
                    )

                base_attrs = {
                    "segment_id": segment_id,
                    "length_m": _f(row["length_m"]),
                    "name": row["name"],
                    "highway": row["highway"],
                    "infra_type": row["infra_type"],
                    "physical_cost": _f(row["physical_cost"]),
                    "crash_cost": _f(row["crash_cost"]),
                    "intersection_cost_at_start": _f(row["intersection_cost_at_start"]),
                    "intersection_cost_at_end": _f(row["intersection_cost_at_end"]),
                    "elevation_cost_forward": _f(row["elevation_cost_forward"]),
                    "elevation_cost_backward": _f(row["elevation_cost_backward"]),
                    # Carried as graph attributes for the heuristic-floor
                    # pass and possible future use; not written to the
                    # binary edge record (which carries only the composed
                    # stress_cost and its physical/intersection/crash/elevation parts).
                    "highway_class": row["highway_class"],
                    "infra_tier": row["infra_tier"],
                    "base_stress_per_meter": _f(row["base_stress_per_meter"]),
                    "surface_penalty": _f(row["surface_penalty"]),
                    "enclosed_penalty": _f(row["enclosed_penalty"]),
                    "lighting_penalty": _f(row["lighting_penalty"]),
                }

                if direction in ("forward", "bidirectional"):
                    G.add_edge(start_node, end_node, forward=True, **base_attrs)
                    edge_count += 1
                if direction in ("backward", "bidirectional"):
                    G.add_edge(end_node, start_node, forward=False, **base_attrs)
                    edge_count += 1

                segment_count += 1

            logger.info(
                "Loaded %d segments -> %d directed edges so far",
                segment_count,
                edge_count,
            )

        floor_row = self.engine.query(
            self._HEURISTIC_FLOOR_QUERY.format(
                city=self.city,
                marts_schema=self.marts_schema,
            )
        )
        if floor_row.empty or floor_row["floor"].iloc[0] is None:
            logger.warning(
                "Could not compute heuristic floor; routing will use a "
                "conservative default and may be slower than necessary"
            )
            G.graph["heuristic_floor"] = 0.0
        else:
            # 0.95 safety margin so the heuristic stays admissible even
            # if the cost formula changes slightly between graph export
            # and Lambda load.
            raw_floor = float(floor_row["floor"].iloc[0])
            G.graph["heuristic_floor"] = raw_floor * 0.95
            logger.info(
                "Heuristic floor: %.4f cost-per-meter (raw min: %.4f)",
                G.graph["heuristic_floor"],
                raw_floor,
            )

        logger.info(
            "Graph complete: %d nodes, %d edges, %d unique segment geometries",
            G.number_of_nodes(),
            G.number_of_edges(),
            len(segment_geometry),
        )
        return G

    def _filter_small_components(self, G: nx.MultiDiGraph) -> nx.MultiDiGraph:
        if self.min_component_size <= 1:
            return G

        components = sorted(nx.weakly_connected_components(G), key=len, reverse=True)
        # (node_count, edge_count) per component, biggest first.
        component_stats = [(len(c), G.subgraph(c).number_of_edges()) for c in components]

        logger.info("Found %d weakly connected components", len(components))
        for i, (n_nodes, n_edges) in enumerate(component_stats[:10]):
            logger.info("  Component %d: %d nodes, %d edges", i, n_nodes, n_edges)
        if len(component_stats) > 10:
            smaller = component_stats[10:]
            logger.info(
                "  ... and %d smaller components (max %d nodes, %d nodes total)",
                len(smaller),
                max(n for n, _ in smaller),
                sum(n for n, _ in smaller),
            )

        before_nodes = G.number_of_nodes()
        before_edges = G.number_of_edges()

        small = [c for c in components if len(c) < self.min_component_size]
        nodes_to_remove = set().union(*small) if small else set()
        G.remove_nodes_from(nodes_to_remove)

        referenced = {data["segment_id"] for _, _, data in G.edges(data=True)}
        seg_geom = G.graph.get("segment_geometry", {})
        for sid in list(seg_geom):
            if sid not in referenced:
                del seg_geom[sid]

        logger.info(
            "Component filtering: removed %d components (%d nodes, %d edges) "
            "smaller than %d nodes. Graph: %d nodes, %d edges remaining.",
            len(small),
            before_nodes - G.number_of_nodes(),
            before_edges - G.number_of_edges(),
            self.min_component_size,
            G.number_of_nodes(),
            G.number_of_edges(),
        )

        kept = [c for c in components if len(c) >= self.min_component_size]
        if len(kept) > 1:
            kept_stats = [(len(c), G.subgraph(c).number_of_edges()) for c in kept]
            logger.warning(
                "Graph has %d large components after filtering (sizes: %s). "
                "Routes between components will fail. This is usually an OSM data "
                "problem — check for missing bridges/tunnels, broken way connectivity, "
                "or a bbox that splits the network.",
                len(kept),
                kept_stats,
            )

        return G

    # ------------------------------------------------------------------
    # Binary serialization
    # ------------------------------------------------------------------

    def _write_binary(self, G: nx.MultiDiGraph, output_path: Path) -> None:
        """Walk the NetworkX graph and emit the binary format.

        Composition of the pure helpers below — exists to wire them
        together and own the gzip / file handle. The helpers are
        directly testable with hand-built MultiDiGraphs.
        """
        nodes, osm_to_idx = build_node_table(G)
        edges, strings = build_edges_and_strings(G, osm_to_idx)
        geom_records = build_segment_geometries(G.graph.get("segment_geometry", {}), strings)

        with gzip.open(output_path, "wb") as gz:
            write_graph(
                gz,
                heuristic_floor=G.graph.get("heuristic_floor", 0.0),
                strings=list(strings),
                nodes=nodes,
                edges=edges,
                segment_geometries=geom_records,
            )


# ----------------------------------------------------------------------
# Pure helpers — no DB, no file I/O. Operate on a MultiDiGraph built
# either by `_build_graph` or by a test fixture.
# ----------------------------------------------------------------------


def build_node_table(
    G: nx.MultiDiGraph,
) -> tuple[list[WriteNode], dict[int, int]]:
    """Assign NodeIdx 0..N-1 in G.nodes iteration order.

    Returns the WriteNode list (in NodeIdx order) and a mapping from
    OSM node id to its assigned NodeIdx. `is_intersection` is read
    from the node attribute set during graph construction; nodes
    without the attribute default to False.
    """
    osm_to_idx: dict[int, int] = {}
    nodes: list[WriteNode] = []
    for osm_id, attrs in G.nodes(data=True):
        osm_to_idx[osm_id] = len(nodes)
        nodes.append(
            WriteNode(
                osm_id=osm_id,
                lon=float(attrs["x"]),
                lat=float(attrs["y"]),
                is_intersection=bool(attrs.get("is_intersection", False)),
            )
        )
    return nodes, osm_to_idx


def build_edges_and_strings(
    G: nx.MultiDiGraph,
    osm_to_idx: dict[int, int],
) -> tuple[list[WriteEdge], dict[str, int]]:
    """Build WriteEdge records sorted by source_node_idx, with a
    deduplicated string table.

    Per-direction stress composition:
      forward edge:  physical_cost + crash_cost + intersection_cost_at_end + elevation_cost_forward
      backward edge: physical_cost + crash_cost + intersection_cost_at_start + elevation_cost_backward

    The returned `strings` dict preserves first-insertion order; its
    keys form the canonical string table (the writer takes `list(strings)`
    to get the ordered table). Indices are the dict values.

    Edge attribute fields that are missing on the NX graph default to
    `None`, which `f32_or_nan` maps to NaN for the binary format.
    """
    strings: dict[str, int] = {}

    def intern(s: str | None, *, nullable: bool) -> int:
        if s is None:
            if not nullable:
                raise ValueError("non-nullable string field had None value")
            return NULL_STR_IDX
        existing = strings.get(s)
        if existing is not None:
            return existing
        idx = len(strings)
        strings[s] = idx
        return idx

    edges: list[WriteEdge] = []
    # Iterate all edges including parallels. We don't need the
    # MultiDiGraph key — it's just a disambiguator for parallel edges.
    for u_osm, v_osm, data in G.edges(data=True):
        forward = bool(data.get("forward", True))

        # Per-direction stress composition. Missing cost components
        # are treated as 0 here (not NaN) so summation is well-defined.
        physical = _coerce_cost(data.get("physical_cost"))
        crash = _coerce_cost(data.get("crash_cost"))
        if forward:
            intersection = _coerce_cost(data.get("intersection_cost_at_end"))
            elevation = _coerce_cost(data.get("elevation_cost_forward"))
        else:
            intersection = _coerce_cost(data.get("intersection_cost_at_start"))
            elevation = _coerce_cost(data.get("elevation_cost_backward"))
        stress_cost = physical + crash + intersection + elevation

        edges.append(
            WriteEdge(
                source_node_idx=osm_to_idx[u_osm],
                target_node_idx=osm_to_idx[v_osm],
                segment_id_str_idx=intern(data["segment_id"], nullable=False),
                name_str_idx=intern(data.get("name"), nullable=True),
                highway_str_idx=intern(data.get("highway"), nullable=True),
                infra_type_str_idx=intern(data.get("infra_type"), nullable=True),
                forward=forward,
                length_m=f32_or_nan(data.get("length_m")),
                stress_cost=f32_or_nan(stress_cost),
                physical_cost=f32_or_nan(data.get("physical_cost")),
                # `intersection_cost` is the value that was actually
                # applied for this edge's direction. Matches what's
                # baked into `stress_cost`.
                intersection_cost=f32_or_nan(intersection),
                crash_cost=f32_or_nan(data.get("crash_cost")),
                elevation_cost=f32_or_nan(elevation),
            )
        )

    # Stable sort by source_node_idx. Python's sort is stable, so
    # parallel edges from the same source preserve their insertion order.
    edges.sort(key=lambda e: e.source_node_idx)
    return edges, strings


def build_segment_geometries(
    segment_geometry: dict[str, tuple],
    strings: dict[str, int],
) -> list[WriteSegmentGeometry]:
    """Build geometry records for segments referenced by the string table.

    Segments not in `strings` (orphaned by component filtering before
    we got here, or never referenced by an edge) are skipped silently.
    """
    records: list[WriteSegmentGeometry] = []
    for seg_id, coords in segment_geometry.items():
        seg_idx = strings.get(seg_id)
        if seg_idx is None:
            continue
        records.append(
            WriteSegmentGeometry(
                segment_id_str_idx=seg_idx,
                coords=[(float(lon), float(lat)) for lon, lat in coords],
            )
        )
    return records


def _coerce_cost(value: Any) -> float:
    """Coerce a cost component value for arithmetic.

    None and NaN both map to 0.0 so the stress-cost summation produces
    a usable value when components are missing. (We don't propagate
    NaN through the sum because A* needs a finite cost to make
    progress.)
    """
    if value is None:
        return 0.0
    value = float(value)
    if value != value:  # NaN check
        return 0.0
    return value


def _f(value: Any) -> float | None:
    """Coerce numeric DB values (Decimal/None) to native float.

    Decimal values from psycopg are larger than floats and aren't
    JSON-serializable. None passes through so f32_or_nan can map it to NaN.
    """
    return float(value) if value is not None else None
