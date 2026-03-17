"""
Airflow tasks for deploying the bike map to S3 + CloudFront.

Syncs all files from the local export directory to an S3 bucket and
creates a CloudFront cache invalidation.

Configuration via environment variables:
    BIKE_MAP_S3_BUCKET          — S3 bucket name
    BIKE_MAP_CLOUDFRONT_DIST_ID — CloudFront distribution ID
    BIKE_MAP_ENVIRONMENT        — Environment name (dev, staging, prod)

Usage in a DAG:

    from loci.tasks.deploy_tasks import deploy_bike_map

    @dag(...)
    def my_dag():
        ...
        export_bike_map_geojson(conn_id="gis_dwh_db") >> deploy_bike_map()
"""

import os
import time
from logging import Logger
from pathlib import Path

import boto3
from airflow.sdk import task

BIKE_MAP_APP_DIR = "/opt/airflow/app-files/bike-map"
BIKE_MAP_EXPORT_DIR = "/opt/airflow/exports/bike-map"

# Subdirectories to skip during the general S3 sync.
# These are handled by dedicated upload steps (e.g. _sync_bike_map_config).
SKIP_DIRS = {"config"}

CONTENT_TYPES = {
    ".html": "text/html",
    ".geojson": "application/geo+json",
    ".json": "application/json",
}


def _get_content_type(path: Path) -> str:
    """Return the Content-Type for a file based on its extension."""
    return CONTENT_TYPES.get(path.suffix, "application/octet-stream")


def _sync_to_s3(local_dirs: list[str], bucket: str, logger: Logger) -> int:
    """Upload files from multiple local directories to the S3 bucket.

    Each directory is synced relative to itself, preserving structure.
    Only files with extensions in CONTENT_TYPES are uploaded.
    Subdirectories listed in SKIP_DIRS are excluded.

    For example, given:
        /opt/airflow/app-files/bike-map/index.html         → index.html
        /opt/airflow/exports/bike-map/data/crashes.geojson  → data/crashes.geojson

    Returns the number of files uploaded.
    """
    s3 = boto3.client("s3")
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

            logger.info("Uploading %s → s3://%s/%s (%s)", file_path, bucket, key, content_type)
            s3.upload_file(
                str(file_path),
                bucket,
                key,
                ExtraArgs={"ContentType": content_type},
            )
            count += 1

    return count


def _invalidate_cloudfront(distribution_id: str, logger: Logger) -> str:
    """Create a CloudFront invalidation for all paths.

    Returns the invalidation ID.
    """
    cf = boto3.client("cloudfront")
    resp = cf.create_invalidation(
        DistributionId=distribution_id,
        InvalidationBatch={
            "Paths": {"Quantity": 1, "Items": ["/*"]},
            "CallerReference": str(int(time.time())),
        },
    )
    invalidation_id = resp["Invalidation"]["Id"]
    logger.info("Created CloudFront invalidation %s", invalidation_id)
    return invalidation_id


def _sync_bike_map_config(logger: Logger) -> dict:
    """Upload the environment-specific config.json to S3.

    Reads config/{environment}.json from the app directory and uploads
    it as config.json in the S3 bucket root, where index.html expects it.
    """
    bucket = os.environ["BIKE_MAP_S3_BUCKET"]
    env = os.environ.get("BIKE_MAP_ENVIRONMENT", "dev")

    config_path = Path(BIKE_MAP_APP_DIR) / "config" / f"{env}.json"
    if not config_path.exists():
        raise FileNotFoundError(
            f"Config file not found: {config_path}. "
            f"Expected one of: dev.json, staging.json, prod.json"
        )

    s3 = boto3.client("s3")
    logger.info("Uploading %s → s3://%s/config.json", config_path, bucket)
    s3.upload_file(
        str(config_path),
        bucket,
        "config.json",
        ExtraArgs={"ContentType": "application/json"},
    )

    return {"bucket": bucket, "environment": env, "source": str(config_path)}


@task
def deploy_bike_map(task_logger: Logger) -> dict:
    """Sync bike map files to S3 and invalidate the CloudFront cache."""
    bucket = os.environ["BIKE_MAP_S3_BUCKET"]
    distribution_id = os.environ["BIKE_MAP_CLOUDFRONT_DIST_ID"]

    file_count = _sync_to_s3(
        [BIKE_MAP_APP_DIR, BIKE_MAP_EXPORT_DIR],
        bucket,
        task_logger,
    )
    task_logger.info("Uploaded %d files to s3://%s", file_count, bucket)

    conf_log = _sync_bike_map_config(task_logger)
    task_logger.info(
        "Uploaded %s env config to s3://%s",
        conf_log.get("environment", "missing_env"),
        conf_log.get("bucket", "missing_bucket"),
    )

    invalidation_id = _invalidate_cloudfront(distribution_id, task_logger)

    return {
        "bucket": bucket,
        "files_uploaded": file_count,
        "invalidation_id": invalidation_id,
    }
