"""
Export a safety-weighted routing graph to a gzip-pickled NetworkX DiGraph file.

Queries mart__bike_safety_weighted_edges and osmnx_bike_network_nodes from
the marts schema, builds a NetworkX DiGraph, serializes it with gzip pickle,
and writes it to a local path.

The graph stores only what the Lambda routing function needs:
    - Node attributes: lat, lon
    - Edge attributes: key, length_m, safety_cost

Usage from an Airflow task:

    from loci.exports.graph_export import RoutingGraphExporter

    exporter = RoutingGraphExporter(engine)
    output_path = exporter.export(output_path=Path("/tmp/routing_graph.pkl.gz"))

Configuration via environment variables:
    MARTS_SCHEMA_NAME  — dbt marts schema (e.g. dbt_loci_marts)
"""

from __future__ import annotations

import gzip
import logging
import os
import pickle
from pathlib import Path

import networkx as nx
from loci.db.core import PostgresEngine

logger = logging.getLogger(__name__)


class RoutingGraphExporter:
    """Builds a safety-weighted routing graph and writes it to a local file.

    Parameters
    ----------
    engine : PostgresEngine
    batch_size : int
        Rows per batch when streaming edges from the database.
    """

    _EDGE_QUERY = """
        select
            e.u,
            e.v,
            e.key,
            e.length_m,
            e.safety_cost,
            n_u.latitude  as u_lat,
            n_u.longitude as u_lon,
            n_v.latitude  as v_lat,
            n_v.longitude as v_lon
        from {marts_schema}.bike_safety_weighted_edges e
        join raw_data.osmnx_bike_network_nodes n_u
            on n_u.osmid = e.u
            and n_u.valid_to is null
        join raw_data.osmnx_bike_network_nodes n_v
            on n_v.osmid = e.v
            and n_v.valid_to is null
        where e.safety_cost is not null
        order by e.u, e.v, e.key """
    _CRS_QUERY = """
        select srtext
        from spatial_ref_sys
        where srid = (
            select Find_SRID('{marts_schema}', 'bike_safety_weighted_edges', 'geom')
        ) """

    def __init__(
        self, engine: PostgresEngine, batch_size: int = 50_000, marts_schema: str | None = None
    ):
        self.engine = engine
        self.batch_size = batch_size
        if marts_schema is not None:
            self.marts_schema = marts_schema
        else:
            self.marts_schema = os.environ["MARTS_SCHEMA_NAME"]

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

    def _build_graph(self) -> nx.DiGraph:
        """Stream edges from the database and build a NetworkX DiGraph."""
        query = self._EDGE_QUERY.format(marts_schema=self.marts_schema)

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
                    safety_cost=row["safety_cost"],
                )
                edge_count += 1

            logger.info("Loaded %d edges", edge_count)

        crs_row = self.engine.query(self._CRS_QUERY.format(marts_schema=self.marts_schema))
        G.graph["crs"] = crs_row["srtext"][0]

        logger.info(
            "Graph complete: %d nodes, %d edges",
            G.number_of_nodes(),
            G.number_of_edges(),
        )
        return G
