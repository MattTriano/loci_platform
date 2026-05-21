# loci_platform/platform/airflow/dags/loci/environments.py
"""
Per-environment, per-city configuration for the bike map pipeline.

Most values are read from SSM Parameter Store at task-run time, under
the path /loci/<env>/<city>/bike-map/. Only the bootstrap values needed
to *reach* SSM (AWS profile, region, environment name, dbt schema) come
from environment variables, namespaced by env: e.g. AWS_PROFILE_DEV,
BIKE_MAP_AWS_REGION_DEV, BIKE_MAP_ENVIRONMENT_DEV, MARTS_SCHEMA_NAME_DEV.

Per-city tofu outputs (bucket names, CloudFront dist ID, Lambda ARN)
and the routing API key are written to SSM by `tofu apply` and read
here. To onboard a new city: apply the tofu module for that city, then
add the city to VALID_CITIES and create a refresh DAG.

Usage:

    from loci.environments import get_env

    cfg = get_env("staging", "chicago")
    bucket = cfg.app_file_bucket

Repeated calls with the same (env, city) are cached for the lifetime
of the process, so DAG tasks share a single SSM round-trip.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from functools import cache

import boto3
from loci.exports.graph_export import GRAPH_S3_KEY

VALID_ENVS = ("dev", "staging", "prod")
VALID_CITIES = (
    "boston",
    "chicago",
    "dc",
    "denver",
    "detroit",
    "madison",
    "nola",
    "portland",
    "sf",
    "toronto",
)  # extend as we onboard more cities

_SSM_PREFIX = "/loci-infra"


@dataclass(frozen=True)
class LandingEnvConfig:
    """Env-scoped (non-city) config for the bike-map landing page.

    Loaded from SSM at /loci-infra/<env>/bike-map-landing/ by the
    bike-map-landing tofu module. Same role as EnvConfig but per-env
    rather than per-(env, city).
    """

    name: str
    aws_profile: str | None
    aws_region: str
    app_file_bucket: str
    cloudfront_dist_id: str
    zone_name: str
    cities: tuple[str, ...]


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
    routing_api_key: str
    routing_api_url: str
    log_endpoint: str
    dbt_target: str
    marts_schema: str


def _require_env(key: str, env_suffix: str) -> str:
    full_key = f"{key}_{env_suffix}"
    val = os.environ.get(full_key)
    if not val:
        raise RuntimeError(
            f"Missing required environment variable: {full_key}. Check your .env file."
        )
    return val


def _require_ssm(params: dict[str, str], key: str, path: str) -> str:
    val = params.get(key)
    if not val:
        raise RuntimeError(
            f"Missing required SSM parameter {key!r} under {path}. "
            f"Has the tofu module been applied for this (env, city)?"
        )
    return val


def load_parameters_by_path(session, path: str) -> dict[str, str]:
    """Load all SSM parameters under a path prefix.

    Returns a dict mapping the final path segment to the decrypted value.
    For example, with path "/loci/dev/chicago/bike-map" the returned dict
    has keys like "routing-api-key", "app-file-bucket", etc.

    Pages through results so we don't silently truncate at 10 params.
    """
    ssm = session.client("ssm")
    result: dict[str, str] = {}
    next_token: str | None = None

    while True:
        kwargs = {"Path": path, "WithDecryption": True, "Recursive": False}
        if next_token:
            kwargs["NextToken"] = next_token
        resp = ssm.get_parameters_by_path(**kwargs)

        for p in resp["Parameters"]:
            key = p["Name"].rsplit("/", 1)[-1]
            result[key] = p["Value"]

        next_token = resp.get("NextToken")
        if not next_token:
            return result


@cache
def get_env(env: str, city: str) -> EnvConfig:
    """Load the EnvConfig for the given environment and city.

    Cached per (env, city) for the lifetime of the process. SSM is hit
    once per pair across both the bike-map and route-logger prefixes;
    subsequent calls return the same EnvConfig.
    """
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown env: {env!r}. Expected one of {VALID_ENVS}.")
    if city not in VALID_CITIES:
        raise ValueError(f"Unknown city: {city!r}. Expected one of {VALID_CITIES}.")

    env_suffix = env.upper()
    aws_profile = os.environ.get(f"AWS_PROFILE_{env_suffix}")
    aws_region = _require_env("BIKE_MAP_AWS_REGION", env_suffix)
    marts_schema = _require_env("MARTS_SCHEMA_NAME", env_suffix)

    session_kwargs: dict = {"region_name": aws_region}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile
    session = boto3.Session(**session_kwargs)

    bike_map_path = f"{_SSM_PREFIX}/{env}/{city}/bike-map"
    route_logger_path = f"{_SSM_PREFIX}/{env}/{city}/route-logger"
    bike_map = load_parameters_by_path(session, bike_map_path)
    route_logger = load_parameters_by_path(session, route_logger_path)

    return EnvConfig(
        name=env,
        city=city,
        aws_profile=aws_profile,
        aws_region=aws_region,
        app_file_bucket=_require_ssm(bike_map, "app-file-bucket", bike_map_path),
        cloudfront_dist_id=_require_ssm(bike_map, "cloudfront-dist-id", bike_map_path),
        routing_graph_bucket=_require_ssm(bike_map, "routing-graph-bucket", bike_map_path),
        # The S3 key is defined alongside the exporter, since the writer
        # and the Lambda's environment variable need to agree on the same
        # path. Updating GRAPH_S3_KEY is a single-source change.
        routing_graph_key=GRAPH_S3_KEY,
        routing_lambda_arn=_require_ssm(bike_map, "routing-lambda-arn", bike_map_path),
        routing_api_key=_require_ssm(bike_map, "routing-api-key", bike_map_path),
        routing_api_url=_require_ssm(bike_map, "routing-api-url", bike_map_path),
        log_endpoint=_require_ssm(route_logger, "log-endpoint", route_logger_path),
        dbt_target=env,
        marts_schema=marts_schema,
    )


@cache
def get_env_apex(env: str) -> LandingEnvConfig:
    """Load the env-scoped landing config from SSM.

    Cached for the lifetime of the process. Mirrors get_env(env, city)
    but reads from the per-env landing prefix populated by the
    bike-map-landing tofu module.
    """
    if env not in VALID_ENVS:
        raise ValueError(f"Unknown env: {env!r}. Expected one of {VALID_ENVS}.")

    env_suffix = env.upper()
    aws_profile = os.environ.get(f"AWS_PROFILE_{env_suffix}")
    aws_region = _require_env("BIKE_MAP_AWS_REGION", env_suffix)

    session_kwargs: dict = {"region_name": aws_region}
    if aws_profile:
        session_kwargs["profile_name"] = aws_profile
    session = boto3.Session(**session_kwargs)

    landing_path = f"{_SSM_PREFIX}/{env}/bike-map-landing"
    landing = load_parameters_by_path(session, landing_path)

    # `cities` is jsonencode(var.cities) in tofu — decode it back into a
    # tuple so the frozen dataclass stays hashable.
    cities = tuple(json.loads(_require_ssm(landing, "cities", landing_path)))

    return LandingEnvConfig(
        name=env,
        aws_profile=aws_profile,
        aws_region=aws_region,
        app_file_bucket=_require_ssm(landing, "app-file-bucket", landing_path),
        cloudfront_dist_id=_require_ssm(landing, "cloudfront-dist-id", landing_path),
        zone_name=_require_ssm(landing, "zone-name", landing_path),
        cities=cities,
    )
