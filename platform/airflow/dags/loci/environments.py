# loci_platform/platform/airflow/dags/loci/environments.py
"""
Per-environment, per-city configuration for the bike map pipeline.

Values live in environment variables, namespaced by env and city with an
uppercase suffix (e.g. BIKE_MAP_APP_FILE_BUCKET_DEV_CHICAGO). This module
maps them into a typed EnvConfig at task-run time.

Per-city values come from the bikeinfra tofu outputs (`bike_map_urls`,
`routing_lambda_arns`, `routing_graph_bucket_names`, etc.) — copy them
into the .env file after each `tofu apply`.

Per-env values (region, dbt schema) are not city-specific.

Usage:

    from loci.environments import get_env

    cfg = get_env("staging", "chicago")
    bucket = cfg.app_file_bucket
"""

from __future__ import annotations

import os
from dataclasses import dataclass

VALID_ENVS = ("dev", "staging", "prod")
VALID_CITIES = (
    "chicago",
    "detroit",
    "sf",
)  # extend as we onboard more cities

_MARTS_SCHEMA_BY_ENV = {
    "dev": "dbt_loci_marts",
    "staging": "staging_marts",
    "prod": "marts",
}


@dataclass(frozen=True)
class EnvConfig:
    name: str
    city: str
    aws_profile: str | None
    aws_region: str
    app_file_bucket: str
    cloudfront_dist_id: str
    routing_graph_bucket: str
    routing_graph_key: str
    routing_lambda_arn: str
    dbt_target: str
    marts_schema: str


def _require(key: str, suffix: str) -> str:
    full_key = f"{key}_{suffix}"
    val = os.environ.get(full_key)
    if not val:
        raise RuntimeError(
            f"Missing required environment variable: {full_key}. Check your .env file."
        )
    return val


def get_env(env: str, city: str) -> EnvConfig:
    """Load the EnvConfig for the given environment and city."""
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown env: {env!r}. Expected one of {VALID_ENVS}.")
    if city not in VALID_CITIES:
        raise ValueError(f"Unknown city: {city!r}. Expected one of {VALID_CITIES}.")

    env_suffix = env.upper()  # for env-only vars
    full_suffix = f"{env.upper()}_{city.upper()}"  # for env+city vars

    return EnvConfig(
        name=env,
        city=city,
        aws_profile=os.environ.get(f"AWS_PROFILE_{env_suffix}"),
        aws_region=_require("BIKE_MAP_AWS_REGION", env_suffix),
        app_file_bucket=_require("BIKE_MAP_APP_FILE_BUCKET", full_suffix),
        cloudfront_dist_id=_require("BIKE_MAP_CLOUDFRONT_DIST_ID", full_suffix),
        routing_graph_bucket=_require("BIKE_MAP_ROUTING_GRAPH_BUCKET", full_suffix),
        routing_graph_key=os.environ.get(
            f"BIKE_MAP_ROUTING_GRAPH_KEY_{full_suffix}",
            "graph/routing_graph.pkl.gz",
        ),
        routing_lambda_arn=_require("BIKE_MAP_ROUTING_LAMBDA_ARN", full_suffix),
        dbt_target=env,
        marts_schema=_MARTS_SCHEMA_BY_ENV[env],
    )
