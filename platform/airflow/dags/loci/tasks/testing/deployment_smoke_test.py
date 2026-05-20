# loci_platform/platform/airflow/dags/loci/tasks/testing/deployment_smoke_test.py
"""Post-deploy smoke test for the bike map routing API.

Fetches the deployed config.json and POSTs a random in-bbox route to
the routing API to verify that the full deploy chain (S3 site, Lambda,
API Gateway, CloudFront, SSM key) is wired up correctly.
"""

import logging
import math
import os
import random

import requests
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
    logger: logging.Logger,
    max_distance_miles: float = 5.0,
    cold_start_timeout_s: int = 30,
    warm_timeout_s: int = 10,
    max_route_attempts: int = 5,
    seed: int | None = None,
) -> dict:
    """..."""
    rng = random.Random(seed)

    config = requests.get(f"{site_url}/config.json", timeout=10).json()
    api_url = config["routing_api_url"]
    api_key = config["routing_api_key"]
    logger.info("Posting to %s", api_url)

    for attempt in range(1, max_route_attempts + 1):
        origin, destination = _random_route_within(bbox, max_distance_miles, rng)
        logger.info("Attempt %d: origin=%s destination=%s", attempt, origin, destination)

        timeout = cold_start_timeout_s if attempt == 1 else warm_timeout_s
        resp = requests.post(
            api_url,
            headers={"Content-Type": "application/json", "X-Api-Key": api_key},
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
