"""
Lambda handler for the bike routing API.

Loads a safety-weighted NetworkX graph from S3 on cold start and keeps
it in memory across warm invocations. Uses A* search with a haversine
heuristic for efficient routing.

Environment variables:
    BIKE_MAP_GRAPH_BUCKET : S3 bucket containing the graph file.
    BIKE_MAP_GRAPH_KEY    : S3 key of the gzip-pickled graph file.
    BIKE_MAP_API_KEY      : Required value of the X-Api-Key request header.

Request (POST /route):
    {
        "origin":      [lat, lon],
        "destination": [lat, lon]
    }

Response:
    {
        "total_cost": float,
        "total_length_m": float,
        "nodes": [osmid, ...],
        "coordinates": [[lon, lat], ...]   # GeoJSON order, ready to render
    }
"""

import gzip
import json
import logging
import math
import os
import pickle

import boto3
import networkx as nx
from scipy.spatial import KDTree

log = logging.getLogger()
log.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start globals — loaded once, reused across warm invocations
# ---------------------------------------------------------------------------

_graph: nx.DiGraph | None = None
_kdtree: KDTree | None = None
_node_ids: list | None = None  # parallel list to KDTree: _node_ids[i] = osmid


def _load_graph() -> tuple[nx.DiGraph, KDTree, list]:
    """Download and deserialize the routing graph from S3."""
    bucket = os.environ["BIKE_MAP_GRAPH_BUCKET"]
    key = os.environ["BIKE_MAP_GRAPH_KEY"]

    log.info("Loading graph from s3://%s/%s", bucket, key)
    s3 = boto3.client("s3")
    obj = s3.get_object(Bucket=bucket, Key=key)
    compressed = obj["Body"].read()

    log.info("Deserializing graph (%d bytes compressed)", len(compressed))
    G = pickle.loads(gzip.decompress(compressed))
    log.info("Graph loaded: %d nodes, %d edges", G.number_of_nodes(), G.number_of_edges())

    # Build KD-tree over node coordinates for fast nearest-node lookup.
    # Nodes are stored as (osmid, {lat: ..., lon: ...}) in the graph.
    node_ids = list(G.nodes())
    coords = [(G.nodes[n]["y"], G.nodes[n]["x"]) for n in node_ids]
    kdtree = KDTree(coords)
    log.info("KD-tree built over %d nodes", len(node_ids))

    return G, kdtree, node_ids


def _ensure_loaded():
    global _graph, _kdtree, _node_ids
    if _graph is None:
        _graph, _kdtree, _node_ids = _load_graph()


# ---------------------------------------------------------------------------
# Routing helpers
# ---------------------------------------------------------------------------


def _nearest_node(lat: float, lon: float) -> int:
    """Return the osmid of the nearest graph node to (lat, lon)."""
    _, idx = _kdtree.query([lat, lon])
    return _node_ids[idx]


def _haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Approximate distance in meters between two lat/lon points."""
    r = 6_371_000
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlam = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlam / 2) ** 2
    return r * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _route(origin_lat: float, origin_lon: float, dest_lat: float, dest_lon: float) -> dict:
    """Find the safest route between two coordinates."""
    origin_node = _nearest_node(origin_lat, origin_lon)
    dest_node = _nearest_node(dest_lat, dest_lon)

    if origin_node == dest_node:
        node_data = _graph.nodes[origin_node]
        return {
            "total_cost": 0.0,
            "total_length_m": 0.0,
            "nodes": [origin_node],
            "coordinates": [[node_data["x"], node_data["y"]]],
        }

    def heuristic(u, v):
        # Admissible heuristic: haversine distance to destination.
        # safety_cost >= length_m for any edge, so this never overestimates.
        u_data = _graph.nodes[u]
        v_data = _graph.nodes[v]
        return _haversine_m(u_data["y"], u_data["x"], v_data["y"], v_data["x"])

    try:
        path = nx.astar_path(
            _graph,
            origin_node,
            dest_node,
            heuristic=heuristic,
            weight="safety_cost",
        )
    except nx.NetworkXNoPath:
        raise ValueError(f"No route found between {origin_node} and {dest_node}")

    # Accumulate cost and length along the path
    total_cost = 0.0
    total_length_m = 0.0
    for u, v in zip(path[:-1], path[1:], strict=True):
        edge_data = _graph[u][v]
        total_cost += edge_data.get("safety_cost", 0.0)
        total_length_m += edge_data.get("length_m", 0.0)

    coordinates = [[_graph.nodes[n]["x"], _graph.nodes[n]["y"]] for n in path]

    return {
        "total_cost": round(total_cost, 4),
        "total_length_m": round(total_length_m, 1),
        "nodes": path,
        "coordinates": coordinates,
    }


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


def lambda_handler(event: dict, context) -> dict:
    # API key check
    api_key = os.environ.get("BIKE_MAP_API_KEY", "")
    request_key = (event.get("headers") or {}).get("x-api-key", "")
    if not api_key or request_key != api_key:
        return {
            "statusCode": 401,
            "body": json.dumps({"error": "Unauthorized"}),
        }

    # Parse request body
    try:
        body = json.loads(event.get("body") or "{}")
        origin = body["origin"]  # [lat, lon]
        destination = body["destination"]  # [lat, lon]
        origin_lat, origin_lon = float(origin[0]), float(origin[1])
        dest_lat, dest_lon = float(destination[0]), float(destination[1])
    except (KeyError, ValueError, TypeError) as e:
        return {
            "statusCode": 400,
            "body": json.dumps({"error": f"Invalid request: {e}"}),
        }

    # Ensure graph is loaded (no-op on warm invocations)
    try:
        _ensure_loaded()
    except Exception:
        log.exception("Failed to load graph")
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Graph unavailable"}),
        }

    # Route
    try:
        result = _route(origin_lat, origin_lon, dest_lat, dest_lon)
    except ValueError as e:
        return {
            "statusCode": 422,
            "body": json.dumps({"error": str(e)}),
        }
    except Exception:
        log.exception("Routing failed")
        return {
            "statusCode": 500,
            "body": json.dumps({"error": "Routing failed"}),
        }

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json"},
        "body": json.dumps(result),
    }
