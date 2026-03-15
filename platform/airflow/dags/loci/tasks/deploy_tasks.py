"""
Airflow tasks for deploying the bike map to S3 + CloudFront.

Syncs all files from the local export directory to an S3 bucket and
creates a CloudFront cache invalidation.

Configuration via environment variables:
    BIKE_MAP_S3_BUCKET          — S3 bucket name
    BIKE_MAP_CLOUDFRONT_DIST_ID — CloudFront distribution ID

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


CONTENT_TYPES = {
    ".html": "text/html",
    ".geojson": "application/geo+json",
}


def _get_content_type(path: Path) -> str:
    """Return the Content-Type for a file based on its extension."""
    return CONTENT_TYPES.get(path.suffix, "application/octet-stream")


# def _sync_to_s3(local_dir: str, bucket: str, logger: Logger) -> int:
#     """Upload all files from local_dir to the S3 bucket.

#     Uploads files to matching S3 keys, preserving directory structure
#     relative to local_dir. For example:
#         /opt/airflow/exports/bike-map/index.html      → index.html
#         /opt/airflow/exports/bike-map/data/crashes.geojson → data/crashes.geojson

#     Returns the number of files uploaded.
#     """
#     s3 = boto3.client("s3")
#     local_path = Path(local_dir)
#     count = 0

#     for file_path in sorted(local_path.rglob("*")):
#         if not file_path.is_file():
#             continue
#         if file_path.suffix not in CONTENT_TYPES:
#             continue

#         key = str(file_path.relative_to(local_path))
#         content_type = _get_content_type(file_path)

#         logger.info("Uploading %s → s3://%s/%s (%s)", file_path, bucket, key, content_type)
#         s3.upload_file(
#             str(file_path),
#             bucket,
#             key,
#             ExtraArgs={"ContentType": content_type},
#         )
#         count += 1

#     return count


def _sync_to_s3(local_dirs: list[str], bucket: str, logger: Logger) -> int:
    """Upload files from multiple local directories to the S3 bucket.

    Each directory is synced relative to itself, preserving structure.
    Only files with extensions in CONTENT_TYPES are uploaded.

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

            key = str(file_path.relative_to(local_path))
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


# @task
# def deploy_bike_map(task_logger: Logger) -> dict:
#     """Sync bike map files to S3 and invalidate the CloudFront cache."""
#     bucket = os.environ["BIKE_MAP_S3_BUCKET"]
#     distribution_id = os.environ["BIKE_MAP_CLOUDFRONT_DIST_ID"]

#     file_count = _sync_to_s3(BIKE_MAP_DEPLOY_DIR, bucket, task_logger)
#     task_logger.info("Uploaded %d files to s3://%s", file_count, bucket)

#     invalidation_id = _invalidate_cloudfront(distribution_id, task_logger)

#     return {
#         "bucket": bucket,
#         "files_uploaded": file_count,
#         "invalidation_id": invalidation_id,
#     }


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

    invalidation_id = _invalidate_cloudfront(distribution_id, task_logger)

    return {
        "bucket": bucket,
        "files_uploaded": file_count,
        "invalidation_id": invalidation_id,
    }
