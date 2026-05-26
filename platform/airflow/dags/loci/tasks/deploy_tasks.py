# loci_platform/platform/airflow/dags/loci/tasks/deploy_tasks.py
"""
Airflow tasks for deploying the bike map to S3 + CloudFront.

Syncs all files from the local export directory to an S3 bucket and
creates a CloudFront cache invalidation.

Per-env configuration (buckets, CloudFront distribution, Lambda ARN,
AWS profile) comes from loci.environments.get_env.

Usage in a DAG:

    from loci.tasks.deploy_tasks import deploy_bike_map

    @dag(...)
    def my_dag():
        ...
        export_bike_map_geojson(env="dev", conn_id="gis_dwh_db") >> deploy_bike_map(env="dev")
"""

import gzip
import json
import time
from logging import Logger
from pathlib import Path

from airflow.sdk import task
from loci.aws import get_boto_session
from loci.deploy import upload_file_to_s3
from loci.environments import EnvConfig, LandingEnvConfig, get_env, get_env_apex

BIKE_MAP_LANDING_APP_DIR = "/opt/airflow/app-files/bike-map-landing"
BIKE_MAP_APP_DIR = "/opt/airflow/app-files/bike-map"
BIKE_MAP_EXPORT_DIR_BASE = "/opt/airflow/exports/bike-map"
ROUTING_LAMBDA_ZIP = Path("/opt/airflow/build/routing/routing-lambda.zip")
ROUTING_LAMBDA_S3_KEY = "lambda/routing_api.zip"

# Subdirectories to skip during the general S3 sync. These are handled
# by dedicated upload steps (e.g. _sync_bike_map_config).
SKIP_DIRS = {"config"}

CONTENT_TYPES = {
    ".html": "text/html",
    ".geojson": "application/geo+json",
    ".json": "application/json",
}


def _get_content_type(path: Path) -> str:
    """Return the Content-Type for a file based on its extension."""
    return CONTENT_TYPES.get(path.suffix, "application/octet-stream")


def _gzip_file_bytes(path: Path) -> bytes:
    """Read a file and return its gzip-compressed bytes."""
    with open(path, "rb") as f:
        return gzip.compress(f.read(), compresslevel=6)


def _sync_to_s3(local_dirs: list[str], cfg: EnvConfig, logger: Logger) -> int:
    """Upload files from multiple local directories to the S3 bucket.

    Each directory is synced relative to itself, preserving structure.
    Only files with extensions in CONTENT_TYPES are uploaded.
    Subdirectories listed in SKIP_DIRS are excluded.

    Returns the number of files uploaded.
    """
    s3 = get_boto_session(cfg).client("s3")
    count = 0

    for local_dir in local_dirs:
        local_path = Path(local_dir)
        if not local_path.exists():
            logger.warning("Directory does not exist, skipping: %s", local_dir)
            continue

        for file_path in sorted(local_path.rglob("*")):
            if not file_path.is_file():
                continue
            if file_path.suffix not in CONTENT_TYPES:
                continue

            rel = file_path.relative_to(local_path)
            if rel.parts[0] in SKIP_DIRS:
                continue

            key = str(rel)
            content_type = _get_content_type(file_path)

            if file_path.suffix == ".geojson":
                body = _gzip_file_bytes(file_path)
                logger.info(
                    "Uploading %s → s3://%s/%s (%s, gzip: %.1f MB → %.1f MB)",
                    file_path,
                    cfg.app_file_bucket,
                    key,
                    content_type,
                    file_path.stat().st_size / 1_048_576,
                    len(body) / 1_048_576,
                )
                s3.put_object(
                    Bucket=cfg.app_file_bucket,
                    Key=key,
                    Body=body,
                    ContentType=content_type,
                    ContentEncoding="gzip",
                )
            else:
                logger.info(
                    "Uploading %s → s3://%s/%s (%s)",
                    file_path,
                    cfg.app_file_bucket,
                    key,
                    content_type,
                )
                s3.upload_file(
                    str(file_path),
                    cfg.app_file_bucket,
                    key,
                    ExtraArgs={"ContentType": content_type},
                )
            count += 1

    return count


def _invalidate_cloudfront(cfg: EnvConfig, logger: Logger) -> str:
    """Create a CloudFront invalidation for all paths. Returns the invalidation ID."""
    cf = get_boto_session(cfg).client("cloudfront")
    resp = cf.create_invalidation(
        DistributionId=cfg.cloudfront_dist_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/*"]},
            "CallerReference": str(int(time.time())),
        },
    )
    invalidation_id = resp["Invalidation"]["Id"]
    logger.info("Created CloudFront invalidation %s", invalidation_id)
    return invalidation_id


def _sync_bike_map_config(cfg: EnvConfig, logger: Logger) -> dict:
    """Upload the environment-specific config.json to S3 with deploy-time values injected.

    Reads config/{env}/{city}.json from the app directory, fills in the
    routing API URL, routing API key, and route-logger endpoint from cfg,
    and uploads the result as config.json in the S3 bucket root.

    The committed config file has these three fields as named placeholders
    ("<routing_api_url>", "<routing_api_key>", "<log_endpoint>"); the real
    values are never on disk.
    """
    config_path = Path(BIKE_MAP_APP_DIR) / "config" / cfg.name / f"{cfg.city}.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Config file not found: {config_path}")

    with config_path.open() as f:
        config = json.load(f)
    config["routing_api_key"] = cfg.routing_api_key
    config["routing_api_url"] = cfg.routing_api_url
    config["log_endpoint"] = cfg.log_endpoint

    s3 = get_boto_session(cfg).client("s3")
    logger.info(
        "Uploading config → s3://%s/config.json (from %s)", cfg.app_file_bucket, config_path
    )
    s3.put_object(
        Bucket=cfg.app_file_bucket,
        Key="config.json",
        Body=json.dumps(config, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {
        "bucket": cfg.app_file_bucket,
        "environment": cfg.name,
        "city": cfg.city,
        "source": str(config_path),
    }


@task
def deploy_bike_map(env: str, city: str, task_logger: Logger) -> dict:
    """Sync bike map files to S3 and invalidate the CloudFront cache."""
    cfg = get_env(env, city)
    export_dir = str(Path(BIKE_MAP_EXPORT_DIR_BASE) / cfg.name / cfg.city)

    file_count = _sync_to_s3(
        [BIKE_MAP_APP_DIR, export_dir],
        cfg,
        task_logger,
    )
    task_logger.info("Uploaded %d files to s3://%s", file_count, cfg.app_file_bucket)

    conf_log = _sync_bike_map_config(cfg, task_logger)
    task_logger.info(
        "Uploaded %s env config to s3://%s",
        conf_log.get("environment", "missing_env"),
        conf_log.get("bucket", "missing_bucket"),
    )

    invalidation_id = _invalidate_cloudfront(cfg, task_logger)

    return {
        "bucket": cfg.app_file_bucket,
        "files_uploaded": file_count,
        "invalidation_id": invalidation_id,
    }


@task
def deploy_lambda(env: str, city: str, task_logger: Logger) -> dict:
    """Push the pre-built routing Lambda zip to S3 and update the function code.

    The zip is built outside Airflow by `make build` in services/routing/
    and mounted into the worker at /opt/airflow/build/routing/. See
    podman/orchestration/CHUNK7-NOTES.md for the compose mount.

    The mtime of the zip is logged so stale artifacts are visible in
    CloudWatch when debugging "I deployed but my change isn't there".
    """
    cfg = get_env(env, city)

    if not ROUTING_LAMBDA_ZIP.is_file():
        raise RuntimeError(
            f"routing-lambda.zip not found at {ROUTING_LAMBDA_ZIP}. "
            f"Run `make build` in services/routing/ and verify the "
            f"build-output volume is mounted into this worker."
        )

    stat = ROUTING_LAMBDA_ZIP.stat()
    size_mb = stat.st_size / 1_048_576
    mtime_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime))
    task_logger.info(
        "Deploying routing-lambda.zip (%.1f MB, built %s)",
        size_mb,
        mtime_iso,
    )

    session = get_boto_session(cfg)

    uri = upload_file_to_s3(
        local_path=ROUTING_LAMBDA_ZIP,
        bucket=cfg.routing_graph_bucket,
        key=ROUTING_LAMBDA_S3_KEY,
        logger=task_logger,
        s3_client=session.client("s3"),
    )
    task_logger.info("Lambda zip uploaded to %s", uri)

    lam = session.client("lambda")
    response = lam.update_function_code(
        FunctionName=cfg.routing_lambda_arn,
        S3Bucket=cfg.routing_graph_bucket,
        S3Key=ROUTING_LAMBDA_S3_KEY,
    )
    task_logger.info(
        "Lambda updated: version=%s state=%s",
        response.get("Version"),
        response.get("LastUpdateStatus"),
    )

    return {
        "uri": uri,
        "lambda_arn": cfg.routing_lambda_arn,
        "zip_built_at": mtime_iso,
        "last_update_status": response.get("LastUpdateStatus"),
    }


def _build_cities_array(
    env: str, city_ids: list[str], zone_name: str, logger: Logger
) -> list[dict]:
    """Build the cities array for the landing page config.

    For each city in city_ids, reads its per-city config from the bike-map
    app dir to pick up city_name, map_center, and map_bbox. The URL is
    constructed from zone_name. Raises if any per-city config is missing
    or malformed — running the landing deploy with a city in the SSM
    list but no config on disk is a deployment-ordering bug we want to
    fail loudly.

    map_bbox is expected as an object {"south", "west", "north", "east"}
    mirroring the loci.geometry.BBox dataclass, so the field meanings
    are unambiguous wherever this shape shows up.
    """
    cities = []
    for city_id in city_ids:
        config_path = Path(BIKE_MAP_APP_DIR) / "config" / env / f"{city_id}.json"
        if not config_path.exists():
            raise FileNotFoundError(
                f"Per-city config not found for landing page: {config_path}. "
                f"Add the config or remove '{city_id}' from var.cities."
            )
        with config_path.open() as f:
            city_config = json.load(f)
        try:
            cities.append(
                {
                    "id": city_id,
                    "name": city_config["city_name"],
                    "center": city_config["map_center"],
                    "bbox": city_config["map_bbox"],
                    "url": f"https://{city_id}.{zone_name}",
                }
            )
        except KeyError as e:
            raise KeyError(
                f"Per-city config {config_path} is missing required field {e}. "
                f"Landing page needs city_name, map_center, and map_bbox."
            ) from e
    logger.info("Built cities array with %d entries", len(cities))
    return cities


def _sync_bike_map_landing_config(cfg: "LandingEnvConfig", logger: Logger) -> dict:
    """Upload the env-specific landing config.json to S3 with cities injected.

    Reads config/<env>.json from the landing app dir, fills in the cities
    array using the SSM-sourced city list + per-city configs, and uploads
    the result as config.json at the root of the landing S3 bucket.
    """
    config_path = Path(BIKE_MAP_LANDING_APP_DIR) / "config" / f"{cfg.name}.json"
    if not config_path.exists():
        raise FileNotFoundError(f"Landing config file not found: {config_path}")

    with config_path.open() as f:
        config = json.load(f)

    cities = _build_cities_array(cfg.name, cfg.cities, cfg.zone_name, logger)
    config["cities"] = cities

    s3 = get_boto_session(cfg).client("s3")
    logger.info(
        "Uploading landing config → s3://%s/config.json (from %s, %d cities)",
        cfg.app_file_bucket,
        config_path,
        len(cities),
    )
    s3.put_object(
        Bucket=cfg.app_file_bucket,
        Key="config.json",
        Body=json.dumps(config, indent=2).encode("utf-8"),
        ContentType="application/json",
    )
    return {
        "bucket": cfg.app_file_bucket,
        "environment": cfg.name,
        "cities_count": len(cities),
        "source": str(config_path),
    }


@task
def deploy_bike_map_landing(env: str, task_logger: Logger) -> dict:
    """Sync the bike-map landing page to S3 and invalidate the CloudFront cache.

    Reads the list of deployed cities from SSM (provisioned by tofu) and
    builds the landing config from the per-city configs on disk.
    """
    cfg = get_env_apex(env)

    # _sync_to_s3 already skips the config/ subdir via SKIP_DIRS, so the
    # raw config/<env>.json doesn't get uploaded — only the post-processed
    # version written by _sync_bike_map_landing_config below.
    file_count = _sync_to_s3([BIKE_MAP_LANDING_APP_DIR], cfg, task_logger)
    task_logger.info("Uploaded %d landing files to s3://%s", file_count, cfg.app_file_bucket)

    conf_log = _sync_bike_map_landing_config(cfg, task_logger)
    task_logger.info(
        "Uploaded landing config for env=%s with %d cities",
        conf_log["environment"],
        conf_log["cities_count"],
    )

    invalidation_id = _invalidate_cloudfront(cfg, task_logger)

    return {
        "bucket": cfg.app_file_bucket,
        "files_uploaded": file_count,
        "cities_count": conf_log["cities_count"],
        "invalidation_id": invalidation_id,
    }


@task
def push_synthetic_fixture(
    env: str,
    city: str,
    fixture: tuple[tuple[float, float], tuple[float, float]] | None,
    task_logger: Logger,
) -> dict | None:
    """Push the synthetic monitor fixture to SSM so the synthetic Lambda
    can use it as a known-good request payload at check time.

    Cities without a fixture configured get no SSM parameter, and the
    synthetic monitor skips their routing API check.
    """
    if fixture is None:
        task_logger.info("No synthetic fixture configured for %s; skipping", city)
        return None

    cfg = get_env(env, city)
    origin, destination = fixture
    payload = {
        "origin": [origin[0], origin[1]],
        "destination": [destination[0], destination[1]],
    }
    ssm_path = f"/loci-infra/{env}/{city}/synthetic/fixture"
    ssm = get_boto_session(cfg).client("ssm")
    ssm.put_parameter(
        Name=ssm_path,
        Value=json.dumps(payload),
        Type="String",
        Overwrite=True,
    )
    task_logger.info("Pushed synthetic fixture to %s", ssm_path)
    return {"ssm_path": ssm_path, "fixture": payload}
