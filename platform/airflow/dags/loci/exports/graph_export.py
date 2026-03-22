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


# """
# graph_export.py — builds a safety-weighted NetworkX DiGraph from the
# mart__bike_safety_weighted_edges dbt model and uploads it to S3 as a
# gzip-pickled file.

# The graph stores the minimum attributes needed for routing and heuristic
# calculation on each node and edge. It is intentionally kept small to
# minimize Lambda cold-start time.

# Usage (called from an Airflow task):
#     from loci.exports.graph_export import export_routing_graph

#     export_routing_graph(
#         engine=postgis_engine,
#         bucket="loci-infra-dev-bike-map-routing-graph",
#         key="graph/routing_graph.pkl.gz",
#     )
# """

# import gzip
# import logging
# import pickle

# import boto3
# import networkx as nx

# log = logging.getLogger(__name__)

# # Columns pulled from the mart. Only what the Lambda needs.
# _EDGE_QUERY = """
# select
#     u,
#     v,
#     key,
#     length_m,
#     safety_cost,
#     -- Node coordinates are on the nodes table; we join them here so the
#     -- graph export is a single query.
#     n_u.latitude  as u_lat,
#     n_u.longitude as u_lon,
#     n_v.latitude  as v_lat,
#     n_v.longitude as v_lon
# from dbt_loci_marts.mart__bike_safety_weighted_edges e
# join raw_data.osmnx_bike_network_nodes n_u
#     on n_u.osmid = e.u
#     and n_u.valid_to is null
# join raw_data.osmnx_bike_network_nodes n_v
#     on n_v.osmid = e.v
#     and n_v.valid_to is null
# where e.safety_cost is not null
# """


# def export_routing_graph(
#     engine,
#     bucket: str,
#     key: str = "graph/routing_graph.pkl.gz",
#     batch_size: int = 50_000,
# ) -> dict:
#     """Build a routing graph from the safety-weighted edges mart and upload to S3.

#     Parameters
#     ----------
#     engine : PostgresEngine
#     bucket : str
#         S3 bucket to upload to.
#     key : str
#         S3 object key for the graph file.
#     batch_size : int
#         Rows per batch when streaming from the database.

#     Returns
#     -------
#     dict with keys: nodes, edges, bucket, key, compressed_bytes.
#     """
#     log.info("Building routing graph from mart__bike_safety_weighted_edges")

#     G = nx.DiGraph()
#     edge_count = 0

#     for batch in engine.query_batches(_EDGE_QUERY, batch_size=batch_size):
#         for row in batch:
#             u = row["u"]
#             v = row["v"]

#             # Add nodes with coordinates if not already present.
#             # We use setdefault-style logic to avoid overwriting coordinates
#             # from a previous batch with identical values.
#             if u not in G:
#                 G.add_node(u, lat=row["u_lat"], lon=row["u_lon"])
#             if v not in G:
#                 G.add_node(v, lat=row["v_lat"], lon=row["v_lon"])

#             G.add_edge(
#                 u,
#                 v,
#                 key=row["key"],
#                 length_m=row["length_m"],
#                 safety_cost=row["safety_cost"],
#             )
#             edge_count += 1

#         log.info("Loaded %d edges so far", edge_count)

#     log.info(
#         "Graph built: %d nodes, %d edges",
#         G.number_of_nodes(),
#         G.number_of_edges(),
#     )

#     # Serialize and compress
#     log.info("Serializing and compressing graph")
#     compressed = gzip.compress(pickle.dumps(G, protocol=pickle.HIGHEST_PROTOCOL))
#     log.info("Compressed size: %.1f MB", len(compressed) / 1_048_576)

#     # Upload to S3
#     log.info("Uploading to s3://%s/%s", bucket, key)
#     s3 = boto3.client("s3")
#     s3.put_object(
#         Bucket=bucket,
#         Key=key,
#         Body=compressed,
#         ContentType="application/octet-stream",
#     )
#     log.info("Upload complete")

#     return {
#         "nodes": G.number_of_nodes(),
#         "edges": G.number_of_edges(),
#         "bucket": bucket,
#         "key": key,
#         "compressed_bytes": len(compressed),
#     }

# def upload_file_to_s3(
#     local_path: Path,
#     bucket: str,
#     key: str,
#     logger: logging.Logger | None = None,
#     s3_client: str | None = None
# ) -> str:
#     """Upload a local file to S3.

#     Args:
#         local_path: Path to the local file.
#         bucket: S3 bucket name.
#         key: S3 object key (e.g. "routing_graph/costs.parquet").
#         logger: Optional logger instance. Falls back to module logger.
#         s3_client: Optional boto3 S3 client. Created if not provided.

#     Returns:
#         The s3:// URI of the uploaded file.
#     """
#     if logger is None:
#         logger = logging.getLogger(__name__)

#     if s3_client is None:
#         s3_client = boto3.client("s3")

#     s3_client.upload_file(local_path, bucket, key)

#     size_mb = os.path.getsize(local_path) / 1024 / 1024
#     uri = f"s3://{bucket}/{key}"
#     logger.info("Uploaded %s (%.1f MB)", uri, size_mb)

#     return uri
