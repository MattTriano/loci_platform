"""
Lambda handler for the bike routing API.

Thin wrapper around the routing module. Loads the graph from S3 on cold
start and delegates routing to routing.find_route().

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
import os
import pickle

import boto3
from routing import build_kdtree, find_route

log = logging.getLogger()
log.setLevel(logging.INFO)

# ---------------------------------------------------------------------------
# Cold-start globals — loaded once, reused across warm invocations
# ---------------------------------------------------------------------------

_graph = None
_kdtree = None
_node_ids = None


def _load_graph():
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

    kdtree, node_ids = build_kdtree(G)
    log.info("KD-tree built over %d nodes", len(node_ids))

    return G, kdtree, node_ids


def _ensure_loaded():
    global _graph, _kdtree, _node_ids
    if _graph is None:
        _graph, _kdtree, _node_ids = _load_graph()


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
        origin = body["origin"]
        destination = body["destination"]
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
        result = find_route(
            _graph,
            _kdtree,
            _node_ids,
            origin_lat,
            origin_lon,
            dest_lat,
            dest_lon,
        )
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
