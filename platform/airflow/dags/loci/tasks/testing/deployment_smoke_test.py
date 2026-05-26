# loci_platform/platform/airflow/dags/loci/tasks/testing/deployment_smoke_test.py
"""Post-deploy smoke test for the bike map routing API.

Fetches the deployed config.json and POSTs a random in-bbox route to
the routing API to verify that the full deploy chain (S3 site, Lambda,
API Gateway, CloudFront, SSM key) is wired up correctly.
"""

import base64
import logging
import math
import os
import random

import requests
from loci.aws import get_boto_session
from loci.environments import EnvConfig
from loci.geo import BBox

# Approximate, fine for picking points within a city bbox
_METERS_PER_DEG_LAT = 111_320.0


def site_url_for(city: str, env: str) -> str:
    """Construct the deployed site URL for a given city/env.

    Reads the apex domain from BIKE_MAP_BASE_DOMAIN. Raises if unset
    so the smoke test fails loudly rather than hitting the wrong host.
    """
    base = os.environ.get("BIKE_MAP_BASE_DOMAIN")
    if not base:
        raise RuntimeError("BIKE_MAP_BASE_DOMAIN is not set")
    return f"https://{city}.{base}" if env == "prod" else f"https://{city}.{env}.{base}"


def _basic_auth_header(env: str, cfg: EnvConfig) -> dict[str, str]:
    """Fetch the shared non-prod basic auth credentials from SSM and return
    them as a request-header dict. Returns an empty dict for prod, since
    prod has no basic auth.
    """
    if env == "prod":
        return {}

    ssm = get_boto_session(cfg).client("ssm")
    resp = ssm.get_parameter(
        Name=f"/loci-infra/{env}/non-prod-auth/credentials",
        WithDecryption=True,
    )
    credentials = resp["Parameter"]["Value"]
    encoded = base64.b64encode(credentials.encode()).decode()
    return {"Authorization": f"Basic {encoded}"}


def _random_route_within(
    bbox: BBox,
    max_distance_miles: float,
    rng: random.Random,
    max_attempts: int = 50,
) -> tuple[tuple[float, float], tuple[float, float]]:
    max_meters = max_distance_miles * 1609.344

    for _ in range(max_attempts):
        o_lat = rng.uniform(bbox.south, bbox.north)
        o_lon = rng.uniform(bbox.west, bbox.east)

        lat_deg_per_m = 1 / _METERS_PER_DEG_LAT
        lon_deg_per_m = 1 / (_METERS_PER_DEG_LAT * math.cos(math.radians(o_lat)))
        d_lat_max = max_meters * lat_deg_per_m
        d_lon_max = max_meters * lon_deg_per_m

        for _ in range(20):
            d_lat = rng.uniform(-d_lat_max, d_lat_max)
            d_lon = rng.uniform(-d_lon_max, d_lon_max)
            meters = math.hypot(d_lat / lat_deg_per_m, d_lon / lon_deg_per_m)
            if meters > max_meters:
                continue
            dest_lat = o_lat + d_lat
            dest_lon = o_lon + d_lon
            if bbox.south <= dest_lat <= bbox.north and bbox.west <= dest_lon <= bbox.east:
                return (o_lat, o_lon), (dest_lat, dest_lon)

    raise RuntimeError("Could not find a valid origin/destination pair in bbox")


def smoke_test_deployed_routing(
    site_url: str,
    bbox: tuple[float, float, float, float],
    env: str,
    cfg: EnvConfig,
    logger: logging.Logger,
    max_distance_miles: float = 5.0,
    cold_start_timeout_s: int = 30,
    warm_timeout_s: int = 10,
    max_route_attempts: int = 5,
    seed: int | None = None,
) -> dict:
    """Verify the deployed site can route end-to-end.

    Fetches config.json from the site (same as a browser would), then
    POSTs to the routing API using exactly those values with random
    origin/destination points within the city bbox. Catches URL/path
    mismatches, bad API keys, Lambda failures, and stale CloudFront
    caches.

    For non-prod environments, fetches the shared basic auth credentials
    from SSM and includes them on every request to the site and the
    routing API. The routing API itself doesn't enforce basic auth, but
    sending the header to it is harmless — non-prod sites need the header
    for the config.json fetch, and reusing the same headers for the API
    POST keeps the code simple.

    Args:
        site_url: Base URL of the deployed site (no trailing slash).
        bbox: (south, west, north, east) in lat/lon degrees.
        env: Environment name (dev/staging/prod). Used to decide whether
            to fetch basic auth credentials.
        cfg: EnvConfig for the city, used to construct a boto session
            for reading credentials from SSM.
        logger: Task logger for progress and result reporting.
        max_distance_miles: Cap on origin-to-destination distance.
        cold_start_timeout_s: Request timeout. The Lambda cold start
            takes ~15s, so this needs headroom above that.
        seed: Optional seed for reproducible point selection. None
            means a fresh random pair each run.

    Returns:
        The parsed routing API response, on success.

    Raises:
        RuntimeError: If the API returns non-200 or an obviously
            invalid response (no coordinates, zero length).
    """
    rng = random.Random(seed)
    auth_headers = _basic_auth_header(env, cfg)

    config_resp = requests.get(f"{site_url}/config.json", headers=auth_headers, timeout=10)
    if config_resp.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch {site_url}/config.json: {config_resp.status_code} — "
            f"{config_resp.text[:500]}"
        )
    config = config_resp.json()
    api_url = config["routing_api_url"]
    api_key = config["routing_api_key"]
    logger.info("Posting to %s", api_url)

    request_headers = {
        **auth_headers,
        "Content-Type": "application/json",
        "X-Api-Key": api_key,
    }

    for attempt in range(1, max_route_attempts + 1):
        origin, destination = _random_route_within(bbox, max_distance_miles, rng)
        logger.info("Attempt %d: origin=%s destination=%s", attempt, origin, destination)

        timeout = cold_start_timeout_s if attempt == 1 else warm_timeout_s
        resp = requests.post(
            api_url,
            headers=request_headers,
            json={"origin": list(origin), "destination": list(destination)},
            timeout=timeout,
        )

        if resp.status_code != 200:
            raise RuntimeError(
                f"Smoke test failed: {resp.status_code} from {api_url} — {resp.text[:500]}"
            )

        data = resp.json()
        segments = data.get("segments")

        # Empty route on a 200 means both points snapped to the same node,
        # which happens when one or both land in water/parkland/etc. inside
        # the bbox. That's a sampling problem, not a deploy problem.
        if not segments or data.get("total_length_m", 0) <= 0:
            logger.warning("Attempt %d: empty route (likely off-graph snap), retrying", attempt)
            continue

        for i, seg in enumerate(segments):
            if not seg.get("coordinates"):
                raise RuntimeError(f"Segment {i} has no coordinates: {seg}")
            if seg.get("stress_cost") is None:
                raise RuntimeError(f"Segment {i} missing stress_cost: {seg}")

        total_points = sum(len(seg["coordinates"]) for seg in segments)
        logger.info(
            "Smoke test OK on attempt %d: %.0fm route, %d segments, %d total points",
            attempt,
            data["total_length_m"],
            len(segments),
            total_points,
        )
        return data

    raise RuntimeError(
        f"Smoke test could not find a routable origin/destination pair in bbox "
        f"after {max_route_attempts} attempts."
    )
