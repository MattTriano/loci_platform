# loci_platform/platform/airflow/dags/loci/exports/graph_export.py
"""
Export a stress-weighted routing graph to a gzip-pickled NetworkX DiGraph file.

Queries mart__chicago_bike_stress_weighted_edges and chicago_osmnx_bike_network_nodes from
the marts schema, builds a NetworkX DiGraph, serializes it with gzip pickle,
and writes it to a local path.

The graph stores only what the Lambda routing function needs:
    - Node attributes: lat (y), lon (x)
    - Edge attributes: key, length_m, stress_cost, name, highway,
      geometry_coords, plus the raw stress-cost components
      (speed_factor, road_type_factor, infrastructure_factor,
      tunnel_factor, surface_factor, lighting_factor,
      crash_score_per_meter, traffic_control_penalty) so the Lambda
      can return a per-segment cost breakdown.

Memory notes
------------
- geometry_coords is stored as a tuple of tuples rather than a list of
  lists. Tuples have less per-object overhead than lists and pickle to
  fewer bytes.
- name and highway strings are interned via sys.intern() so that the
  thousands of edges sharing values like "residential" or "Milwaukee
  Avenue" point to a single string object instead of one per edge.

Usage from an Airflow task:

    from loci.exports.graph_export import RoutingGraphExporter

    exporter = RoutingGraphExporter(engine, marts_schema="dbt_loci_marts")
    output_path = exporter.export(output_path=Path("/tmp/routing_graph.pkl.gz"))
"""

from __future__ import annotations

import gzip
import json
import logging
import pickle
import sys
from pathlib import Path

import networkx as nx
from loci.db.core import PostgresEngine

logger = logging.getLogger(__name__)


class RoutingGraphExporter:
    """Builds a stress-weighted routing graph and writes it to a local file.

    Parameters
    ----------
    engine : PostgresEngine
    marts_schema : str
        dbt marts schema name. Caller is responsible for determining this
        from the target environment.
    batch_size : int
        Rows per batch when streaming edges from the database.
    min_component_size : int
        Weakly connected components smaller than this are dropped before
        serialization. This removes isolated subgraphs (parking lots,
        dead-end service roads, tile boundary fragments) that are
        unreachable from the main network and would never appear in a
        real route. Default is 75. Set to 1 to disable filtering.
    """

    _EDGE_QUERY = """
        select
            e.u, e.v, e.key, e.name, e.highway, e.length_m, e.stress_cost,
            e.speed_factor,
            e.road_type_factor,
            e.infrastructure_factor,
            e.tunnel_factor,
            e.surface_factor,
            e.lighting_factor,
            e.crash_score_per_meter,
            e.traffic_control_penalty,
            ST_AsGeoJSON(ST_Simplify(e.geom, 0.00005)) as geom_geojson,
            n_u.latitude  as u_lat,
            n_u.longitude as u_lon,
            n_v.latitude  as v_lat,
            n_v.longitude as v_lon
        from {marts_schema}.{city}_bike_stress_weighted_edges e
        join raw_data.{city}_osmnx_bike_network_nodes n_u
            on n_u.osmid = e.u
            and n_u.valid_to is null
        join raw_data.{city}_osmnx_bike_network_nodes n_v
            on n_v.osmid = e.v
            and n_v.valid_to is null
        where e.stress_cost is not null
        order by e.u, e.v, e.key """

    _CRS_QUERY = """
        select srtext
        from spatial_ref_sys
        where srid = (
            select Find_SRID('{marts_schema}', '{city}_bike_stress_weighted_edges', 'geom')
        ) """

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
        """Build the routing graph and write it to output_path as a gzip pickle.

        Parameters
        ----------
        output_path : Path
            Destination file path. Parent directory must exist.

        Returns
        -------
        Path to the written file.
        """
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
    def _parse_geojson_coords(geom_geojson: str | None) -> tuple | None:
        """Parse a GeoJSON geometry string into a coordinate tuple.

        Returns the coordinates as a tuple of (lon, lat) tuples
        (e.g. ((lon, lat), ...)) or None if the input is null or
        unparseable. Tuples use less memory than lists when stored
        as edge attributes across hundreds of thousands of edges.
        Parsing at export time avoids repeated json.loads calls at
        request time in the Lambda.
        """
        if not geom_geojson:
            return None
        try:
            geom = json.loads(geom_geojson)
            coords = geom.get("coordinates")
            if coords is None:
                return None
            return tuple(tuple(c) for c in coords)
        except (json.JSONDecodeError, TypeError):
            return None

    @staticmethod
    def _intern_or_none(value):
        """sys.intern() the value if it's a non-empty string, else return as-is.

        Many edges share values like 'residential' or 'Milwaukee Avenue'.
        Interning lets all those edges point to the same underlying
        string object, cutting memory use meaningfully on a city graph.
        """
        if isinstance(value, str) and value:
            return sys.intern(value)
        return value

    def _build_graph(self) -> nx.DiGraph:
        """Stream edges from the database and build a NetworkX DiGraph."""
        query = self._EDGE_QUERY.format(city=self.city, marts_schema=self.marts_schema)

        G = nx.DiGraph()
        edge_count = 0

        for batch in self.engine.query_batches(query, batch_size=self.batch_size):
            for row in batch:
                u, v = row["u"], row["v"]

                if u not in G:
                    G.add_node(u, x=row["u_lon"], y=row["u_lat"])  # x=lon, y=lat
                if v not in G:
                    G.add_node(v, x=row["v_lon"], y=row["v_lat"])

                G.add_edge(
                    u,
                    v,
                    key=row["key"],
                    length_m=row["length_m"],
                    stress_cost=row["stress_cost"],
                    name=self._intern_or_none(row["name"]),
                    highway=self._intern_or_none(row["highway"]),
                    geometry_coords=self._parse_geojson_coords(row["geom_geojson"]),
                    speed_factor=row["speed_factor"],
                    road_type_factor=row["road_type_factor"],
                    infrastructure_factor=row["infrastructure_factor"],
                    tunnel_factor=row["tunnel_factor"],
                    surface_factor=row["surface_factor"],
                    lighting_factor=row["lighting_factor"],
                    crash_score_per_meter=row["crash_score_per_meter"],
                    traffic_control_penalty=row["traffic_control_penalty"],
                )
                edge_count += 1

            logger.info("Loaded %d edges", edge_count)

        crs_row = self.engine.query(
            self._CRS_QUERY.format(city=self.city, marts_schema=self.marts_schema)
        )
        G.graph["crs"] = crs_row["srtext"][0]

        logger.info(
            "Graph complete: %d nodes, %d edges",
            G.number_of_nodes(),
            G.number_of_edges(),
        )
        return G

    def _filter_small_components(self, G: nx.DiGraph) -> nx.DiGraph:
        """Remove weakly connected components smaller than min_component_size.

        Uses weak connectivity (ignores edge direction) so that subgraphs
        reachable only in one direction are still considered connected.
        """
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
