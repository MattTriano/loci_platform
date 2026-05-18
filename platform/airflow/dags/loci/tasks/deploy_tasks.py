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
from loci.environments import EnvConfig, get_env

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
