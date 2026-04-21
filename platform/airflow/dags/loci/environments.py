"""
Per-environment configuration for the bike map pipeline.

Values live in environment variables, namespaced by env with an uppercase
suffix (e.g. BIKE_MAP_APP_FILE_BUCKET_DEV, BIKE_MAP_APP_FILE_BUCKET_STAGING).
This module maps them into a typed EnvConfig at task-run time.

Usage:

    from loci.environments import get_env

    cfg = get_env("staging")
    bucket = cfg.app_file_bucket
"""

from __future__ import annotations

import os
from dataclasses import dataclass

VALID_ENVS = ("dev", "staging", "prod")

# Must match what the dbt generate_schema_name macro produces.
# dev     → dbt_{user}_marts   (user='loci' per profiles.yml)
# staging → staging_marts
# prod    → marts
_MARTS_SCHEMA_BY_ENV = {
    "dev": "dbt_loci_marts",
    "staging": "staging_marts",
    "prod": "marts",
}


@dataclass(frozen=True)
class EnvConfig:
    name: str
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


def get_env(env: str) -> EnvConfig:
    """Load the EnvConfig for the given environment name."""
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown env: {env!r}. Expected one of {VALID_ENVS}.")

    suffix = env.upper()

    return EnvConfig(
        name=env,
        aws_profile=os.environ.get(f"AWS_PROFILE_{suffix}"),
        aws_region=os.environ.get("AWS_REGION", "us-east-1"),
        app_file_bucket=_require("BIKE_MAP_APP_FILE_BUCKET", suffix),
        cloudfront_dist_id=_require("BIKE_MAP_CLOUDFRONT_DIST_ID", suffix),
        routing_graph_bucket=_require("BIKE_MAP_ROUTING_GRAPH_BUCKET", suffix),
        routing_graph_key=os.environ.get(
            f"BIKE_MAP_ROUTING_GRAPH_KEY_{suffix}",
            "graph/routing_graph.pkl.gz",
        ),
        routing_lambda_arn=_require("BIKE_MAP_ROUTING_LAMBDA_ARN", suffix),
        dbt_target=env,
        marts_schema=_MARTS_SCHEMA_BY_ENV[env],
    )
