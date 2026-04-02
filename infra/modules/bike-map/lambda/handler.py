"""
Lambda handler for the bike routing API.

Thin wrapper around the routing module. Loads the graph from S3 on cold
start and delegates routing to routing.find_route().

Environment variables:
    BIKE_MAP_GRAPH_BUCKET    : S3 bucket containing the graph file.
    BIKE_MAP_GRAPH_KEY       : S3 key of the gzip-pickled graph file.
    BIKE_MAP_API_KEY_SSM_ARN : SSM parameter name for the API key (preferred).
    BIKE_MAP_API_KEY         : Fallback plaintext API key (for local testing).

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
_api_key = None


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


def _load_api_key() -> str:
    """Load the API key, preferring SSM Parameter Store over env var.

    SSM is preferred because the key is encrypted at rest via KMS and
    doesn't appear in Lambda console output or OpenTofu state as
    plaintext. The env var fallback supports local testing without SSM.
    """
    ssm_name = os.environ.get("BIKE_MAP_API_KEY_SSM_ARN", "")
    if ssm_name:
        log.info("Loading API key from SSM parameter: %s", ssm_name)
        ssm = boto3.client("ssm")
        resp = ssm.get_parameter(Name=ssm_name, WithDecryption=True)
        return resp["Parameter"]["Value"]

    log.info("SSM parameter not configured, using BIKE_MAP_API_KEY env var")
    return os.environ.get("BIKE_MAP_API_KEY", "")


def _ensure_loaded():
    global _graph, _kdtree, _node_ids, _api_key
    if _graph is None:
        _graph, _kdtree, _node_ids = _load_graph()
    if _api_key is None:
        _api_key = _load_api_key()


# ---------------------------------------------------------------------------
# Handler
# ---------------------------------------------------------------------------


def lambda_handler(event: dict, context) -> dict:
    # Warming ping — load the graph and API key but skip request processing.
    # EventBridge invokes the Lambda directly (not through API Gateway),
    # so there is no X-Api-Key header. This is safe because the Lambda
    # permission resource scopes invocation to the specific EventBridge
    # rule ARN — arbitrary callers cannot invoke the function this way.
    if event.get("source") == "warming-ping":
        _ensure_loaded()
        log.info("Warming ping handled, graph in memory")
        return {"statusCode": 200, "body": "warm"}

    # Ensure graph and API key are loaded (no-op on warm invocations)
    try:
        _ensure_loaded()
    except Exception:
        log.exception("Failed to load graph or API key")
        return {
            "statusCode": 503,
            "body": json.dumps({"error": "Service unavailable"}),
        }

    # API key check
    request_key = (event.get("headers") or {}).get("x-api-key", "")
    if not _api_key or request_key != _api_key:
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
