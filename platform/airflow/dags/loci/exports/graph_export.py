# loci_platform/platform/airflow/dags/loci/exports/graph_export.py
"""
Export a stress-weighted routing graph to a gzip-compressed binary file.

Reads <city>_bike_stress_weighted_segments (one row per undirected segment)
and expands each segment into one or two directed edges based on its
direction column.

The output format is documented in services/routing/docs/graph-format.md
and consumed by the Rust routing-core library (Lambda + CLI).

This module previously emitted a gzip-pickled NetworkX DiGraph; the
Rust reimplementation introduced its own binary format that doesn't
require Python on the read side. The query, component filtering, and
heuristic floor computation are unchanged.
"""

from __future__ import annotations

import gzip
import json
import logging
from pathlib import Path

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

# Output S3 key for the new format. Kept here so callers
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
            stress_cost,
            speed_factor,
            road_type_factor,
            infrastructure_factor,
            tunnel_factor,
            surface_factor,
            lighting_factor,
            physical_cost,
            intersection_cost,
            crash_cost,
            ST_AsGeoJSON(ST_Simplify(geom, 0.00005)) as geom_geojson,
            ST_X(ST_StartPoint(geom)) as start_lon,
            ST_Y(ST_StartPoint(geom)) as start_lat,
            ST_X(ST_EndPoint(geom)) as end_lon,
            ST_Y(ST_EndPoint(geom)) as end_lat
        from {marts_schema}.{city}_bike_stress_weighted_segments
        where stress_cost is not null
        order by way_id, start_position
    """

    _HEURISTIC_FLOOR_QUERY = """
        select min(stress_cost / length_m) as floor
        from {marts_schema}.{city}_bike_stress_weighted_segments
        where length_m > 0 and stress_cost is not null
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
    # Graph construction (unchanged from the gzip-pickle implementation)
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

    @staticmethod
    def _normalize_highway(highway: str | None) -> str | None:
        """Normalize the OSM highway value.

        Historically the value sometimes arrived as a stringified list
        (e.g. "['residential', 'tertiary']") for ways with multiple tags.
        The current OSM collector should produce a single string per
        segment; this normalization is defensive and logs a warning if
        it ever fires, so we can confirm whether the upstream fix held
        and the helper can be retired.
        """
        if highway is None or not highway.startswith("["):
            return highway

        logger.warning(
            "highway value arrived as stringified list, normalizing: %r", highway
        )
        # Strip "[", "]", quotes, then take the first comma-separated value.
        cleaned = highway.strip("[]'\" ").split("'")[0].split(",")[0].strip()
        return cleaned or None

    def _build_graph(self) -> nx.DiGraph:
        """Stream segments and expand into directed edges.

        Builds a NetworkX DiGraph as scratch storage so the existing
        component-filtering step (which uses
        nx.weakly_connected_components) can run unchanged. The final
        serialization walks this graph and emits the binary format.

        Geometry is stored once per undirected segment in
        G.graph['segment_geometry'] keyed by segment_id (string).
        """
        query = self._SEGMENT_QUERY.format(city=self.city, marts_schema=self.marts_schema)

        G = nx.DiGraph()
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
                    G.add_node(start_node, x=row["start_lon"], y=row["start_lat"])
                if end_node not in G:
                    G.add_node(end_node, x=row["end_lon"], y=row["end_lat"])

                base_attrs = {
                    "segment_id": segment_id,
                    "length_m": _f(row["length_m"]),
                    "stress_cost": _f(row["stress_cost"]),
                    "name": row["name"],
                    "highway": self._normalize_highway(row["highway"]),
                    "infra_type": row["infra_type"],
                    "speed_factor": _f(row["speed_factor"]),
                    "road_type_factor": _f(row["road_type_factor"]),
                    "infrastructure_factor": _f(row["infrastructure_factor"]),
                    "tunnel_factor": _f(row["tunnel_factor"]),
                    "surface_factor": _f(row["surface_factor"]),
                    "lighting_factor": _f(row["lighting_factor"]),
                    "physical_cost": _f(row["physical_cost"]),
                    "intersection_cost": _f(row["intersection_cost"]),
                    "crash_cost": _f(row["crash_cost"]),
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

    def _filter_small_components(self, G: nx.DiGraph) -> nx.DiGraph:
        if self.min_component_size <= 1:
            return G

        components = list(nx.weakly_connected_components(G))
        before_nodes = G.number_of_nodes()
        before_edges = G.number_of_edges()

        small = [c for c in components if len(c) < self.min_component_size]
        nodes_to_remove = set().union(*small) if small else set()
        G.remove_nodes_from(nodes_to_remove)

        # Prune geometries for segments no longer referenced by any edge
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
        return G

    # ------------------------------------------------------------------
    # Binary serialization
    # ------------------------------------------------------------------

    def _write_binary(self, G: nx.DiGraph, output_path: Path) -> None:
        """Walk the NetworkX graph and emit the binary format.

        Steps:
          1. Assign a contiguous NodeIdx (0..N-1) to each NetworkX node.
             OSM node IDs become the `osm_id` field on each node record.
          2. Build a deduplicated string table from all segment_ids,
             names, highway values, and infra_type values.
          3. Build WriteEdge records ordered by source NodeIdx (CSR order).
             A backward edge in NetworkX is identified by its `forward=False`
             attribute; the writer sets the EDGE_FLAG_FORWARD bit accordingly.
          4. Build WriteSegmentGeometry records from G.graph['segment_geometry'].
          5. Write everything via write_graph, gzipping the output.
        """
        # 1. NodeIdx assignment
        osm_to_idx: dict[int, int] = {}
        nodes: list[WriteNode] = []
        for osm_id, attrs in G.nodes(data=True):
            osm_to_idx[osm_id] = len(nodes)
            nodes.append(WriteNode(osm_id=osm_id, lon=attrs["x"], lat=attrs["y"]))

        # 2. String table — deduplicated. We use a dict to preserve
        # first-insertion order, which makes the output deterministic
        # for a given input. Index 0 goes to whatever string is seen first.
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

        # 3. Edges — collect with source NodeIdx, then sort by it.
        # NetworkX's edge iteration order is insertion order, which is
        # whatever order rows came back from the SELECT. We re-sort
        # explicitly here to enforce the CSR invariant the writer
        # validates.
        edge_records: list[WriteEdge] = []
        for u_osm, v_osm, data in G.edges(data=True):
            edge_records.append(
                WriteEdge(
                    source_node_idx=osm_to_idx[u_osm],
                    target_node_idx=osm_to_idx[v_osm],
                    segment_id_str_idx=intern(data["segment_id"], nullable=False),
                    name_str_idx=intern(data.get("name"), nullable=True),
                    highway_str_idx=intern(data.get("highway"), nullable=True),
                    infra_type_str_idx=intern(data.get("infra_type"), nullable=True),
                    forward=bool(data.get("forward", True)),
                    length_m=f32_or_nan(data.get("length_m")),
                    stress_cost=f32_or_nan(data.get("stress_cost")),
                    speed_factor=f32_or_nan(data.get("speed_factor")),
                    road_type_factor=f32_or_nan(data.get("road_type_factor")),
                    infrastructure_factor=f32_or_nan(data.get("infrastructure_factor")),
                    tunnel_factor=f32_or_nan(data.get("tunnel_factor")),
                    surface_factor=f32_or_nan(data.get("surface_factor")),
                    lighting_factor=f32_or_nan(data.get("lighting_factor")),
                    physical_cost=f32_or_nan(data.get("physical_cost")),
                    intersection_cost=f32_or_nan(data.get("intersection_cost")),
                    crash_cost=f32_or_nan(data.get("crash_cost")),
                )
            )
        edge_records.sort(key=lambda e: e.source_node_idx)

        # 4. Segment geometries
        geom_records: list[WriteSegmentGeometry] = []
        for seg_id, coords in G.graph.get("segment_geometry", {}).items():
            seg_idx = strings.get(seg_id)
            if seg_idx is None:
                # The segment had geometry but no edge referenced it —
                # shouldn't happen after component pruning, but skip
                # safely rather than emit a dangling geometry.
                continue
            geom_records.append(
                WriteSegmentGeometry(
                    segment_id_str_idx=seg_idx,
                    coords=[(float(lon), float(lat)) for lon, lat in coords],
                )
            )

        # 5. Write — strings dict is ordered, so list(strings) is the
        # canonical order matching the assigned indices.
        with gzip.open(output_path, "wb") as gz:
            write_graph(
                gz,
                heuristic_floor=G.graph.get("heuristic_floor", 0.0),
                strings=list(strings),
                nodes=nodes,
                edges=edge_records,
                segment_geometries=geom_records,
            )


def _f(value):
    """Coerce numeric DB values (Decimal/None) to native float.

    Decimal values from psycopg are larger than floats and aren't
    JSON-serializable. None passes through so f32_or_nan can map it to NaN.
    """
    return float(value) if value is not None else None
