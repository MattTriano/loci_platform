# loci_platform/platform/airflow/dags/loci/exports/graph_export.py
"""
Export a stress-weighted routing graph to a gzip-pickled NetworkX DiGraph file.

Reads chicago_bike_stress_weighted_segments (one row per undirected segment)
and expands each segment into one or two directed edges based on its
direction column.

The graph stores only what the Lambda routing function needs:
    - Node attributes: lat (y), lon (x)
    - Edge attributes: length_m, stress_cost, name, highway, geometry_coords
"""

from __future__ import annotations

import gzip
import json
import logging
import pickle
from pathlib import Path

import networkx as nx
from loci.db.core import PostgresEngine

logger = logging.getLogger(__name__)


class RoutingGraphExporter:
    """Builds a stress-weighted routing graph and writes it to a local file.

    Parameters
    ----------
    engine : PostgresEngine
    city : str
    marts_schema : str
        Schema where chicago_bike_stress_weighted_segments lives.
    batch_size : int
    min_component_size : int
        Weakly connected components smaller than this are dropped.
    """

    _SEGMENT_QUERY = """
        select
            segment_id,
            way_id,
            start_node_id,
            end_node_id,
            direction,
            name,
            highway,
            length_m,
            stress_cost,
            ST_AsGeoJSON(ST_Simplify(geom, 0.00005)) as geom_geojson,
            ST_X(ST_StartPoint(geom)) as start_lon,
            ST_Y(ST_StartPoint(geom)) as start_lat,
            ST_X(ST_EndPoint(geom)) as end_lon,
            ST_Y(ST_EndPoint(geom)) as end_lat
        from {marts_schema}.{city}_bike_stress_weighted_segments
        where stress_cost is not null
        order by way_id, start_position
    """

    _CRS_QUERY = """
        select srtext
        from spatial_ref_sys
        where srid = (
            select ST_SRID(geom) as srid
            from {marts_schema}.{city}_bike_stress_weighted_segments
            where geom is not null
            limit 1
        )
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
            "Serializing graph (%d nodes, %d edges)",
            G.number_of_nodes(),
            G.number_of_edges(),
        )
        compressed = gzip.compress(pickle.dumps(G, protocol=pickle.HIGHEST_PROTOCOL))
        output_path.write_bytes(compressed)
        size_mb = output_path.stat().st_size / 1_048_576
        logger.info("Wrote %s (%.1f MB compressed)", output_path, size_mb)

        return output_path

    @staticmethod
    def _parse_geojson_coords(geom_geojson: str | None) -> list | None:
        if not geom_geojson:
            return None
        try:
            geom = json.loads(geom_geojson)
            return geom.get("coordinates")
        except (json.JSONDecodeError, TypeError):
            return None

    def _build_graph(self) -> nx.DiGraph:
        """Stream segments and expand into directed edges."""
        query = self._SEGMENT_QUERY.format(city=self.city, marts_schema=self.marts_schema)

        G = nx.DiGraph()
        segment_count = 0
        edge_count = 0

        for batch in self.engine.query_batches(query, batch_size=self.batch_size):
            for row in batch:
                start_node = row["start_node_id"]
                end_node = row["end_node_id"]
                direction = row["direction"]
                geom_coords = self._parse_geojson_coords(row["geom_geojson"])

                # Add nodes (idempotent — duplicate add_node calls are no-ops
                # if attributes match, but x/y are stable so this is fine).
                if start_node not in G:
                    G.add_node(start_node, x=row["start_lon"], y=row["start_lat"])
                if end_node not in G:
                    G.add_node(end_node, x=row["end_lon"], y=row["end_lat"])

                base_attrs = {
                    "segment_id": row["segment_id"],
                    "way_id": row["way_id"],
                    "length_m": row["length_m"],
                    "stress_cost": row["stress_cost"],
                    "name": row["name"],
                    "highway": row["highway"],
                }

                # Forward edge: geometry runs start_node → end_node.
                if direction in ("forward", "bidirectional"):
                    G.add_edge(
                        start_node,
                        end_node,
                        geometry_coords=geom_coords,
                        **base_attrs,
                    )
                    edge_count += 1

                # Backward edge: reverse the geometry coords so the edge's
                # geometry flows from from_node → to_node.
                if direction in ("backward", "bidirectional"):
                    reversed_coords = list(reversed(geom_coords)) if geom_coords else None
                    G.add_edge(
                        end_node,
                        start_node,
                        geometry_coords=reversed_coords,
                        **base_attrs,
                    )
                    edge_count += 1

                segment_count += 1

            logger.info(
                "Loaded %d segments → %d directed edges so far",
                segment_count,
                edge_count,
            )

        crs_row = self.engine.query(
            self._CRS_QUERY.format(city=self.city, marts_schema=self.marts_schema)
        )
        if crs_row.empty:
            logger.warning(
                "Could not look up CRS srtext; graph will be exported without CRS metadata"
            )
        else:
            G.graph["crs"] = crs_row["srtext"].iloc[0]

        logger.info(
            "Graph complete: %d nodes, %d edges (from %d segments)",
            G.number_of_nodes(),
            G.number_of_edges(),
            segment_count,
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
