"""
Airflow tasks for deploying the bike map to S3 + CloudFront.

Syncs all files from the local export directory to an S3 bucket and
creates a CloudFront cache invalidation.

Configuration via environment variables:
    BIKE_MAP_APP_FILE_BUCKET    — S3 bucket name
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
import subprocess
import tempfile
import time
import zipfile
from logging import Logger
from pathlib import Path

import boto3
from airflow.sdk import task
from loci.db.af_utils import get_postgres_engine
from loci.deploy import upload_file_to_s3
from loci.exports.graph_export import RoutingGraphExporter

BIKE_MAP_APP_DIR = "/opt/airflow/app-files/bike-map"
BIKE_MAP_EXPORT_DIR = "/opt/airflow/exports/bike-map"
_LAMBDA_DIR = Path("/opt/airflow/app-infra/bike-map/lambda")
_LOCI_DIR = Path("/opt/airflow/dags/loci")

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
    bucket = os.environ["BIKE_MAP_APP_FILE_BUCKET"]
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
    bucket = os.environ["BIKE_MAP_APP_FILE_BUCKET"]
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


def build_lambda_zip(output_path: Path) -> Path:
    """Build the Lambda deployment zip at the given path.

    Installs dependencies from requirements.txt into a python/ subdirectory
    alongside the handler, then zips everything up.

    The caller is responsible for managing the lifetime of output_path's
    parent directory (e.g. by using a tempfile.TemporaryDirectory).

    Parameters
    ----------
    output_path : Path
        Destination path for the zip file (e.g. Path(tmpdir) / "routing_api.zip").

    Returns
    -------
    Path to the written zip file.
    """
    requirements = _LAMBDA_DIR.joinpath("requirements.txt")
    handler_src = _LAMBDA_DIR.joinpath("handler.py")
    routing_src = _LOCI_DIR.joinpath("routing.py")

    deps_dir = output_path.parent.joinpath("deps")
    deps_dir.mkdir(exist_ok=True)

    subprocess.run(
        [
            "pip",
            "install",
            "--quiet",
            "--target",
            str(deps_dir),
            "-r",
            str(requirements),
        ],
        check=True,
    )

    with zipfile.ZipFile(output_path, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(handler_src, "handler.py")
        zf.write(routing_src, "routing.py")
        for file in sorted(deps_dir.rglob("*")):
            if file.is_file():
                zf.write(file, str(file.relative_to(deps_dir)))

    return output_path


@task
def export_routing_graph(conn_id: str, task_logger: Logger) -> dict:
    """Build the safety-weighted routing graph and upload it to S3."""
    bucket = os.environ["BIKE_MAP_ROUTING_GRAPH_BUCKET"]
    key = os.environ.get("BIKE_MAP_ROUTING_GRAPH_KEY", "graph/routing_graph.pkl.gz")
    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    exporter = RoutingGraphExporter(engine)

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "routing_graph.pkl.gz"
        exporter.export(output_path)
        uri = upload_file_to_s3(
            local_path=output_path,
            bucket=bucket,
            key=key,
            logger=task_logger,
        )

    task_logger.info("Routing graph uploaded to %s", uri)
    return {"uri": uri, "bucket": bucket, "key": key}


@task
def deploy_lambda(task_logger: Logger) -> dict:
    """Build the Lambda deployment package and update the function code."""
    bucket = os.environ["BIKE_MAP_ROUTING_GRAPH_BUCKET"]
    lambda_arn = os.environ["BIKE_MAP_ROUTING_LAMBDA_ARN"]
    zip_key = "lambda/routing_api.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "routing_api.zip"
        task_logger.info("Building Lambda zip at %s", zip_path)
        build_lambda_zip(zip_path)

        size_mb = zip_path.stat().st_size / 1_048_576
        task_logger.info("Package size: %.1f MB", size_mb)

        uri = upload_file_to_s3(
            local_path=zip_path,
            bucket=bucket,
            key=zip_key,
            logger=task_logger,
        )

    task_logger.info("Lambda package uploaded to %s", uri)

    lam = boto3.client("lambda", region_name=os.environ.get("BIKE_MAP_AWS_REGION", "us-east-1"))
    response = lam.update_function_code(
        FunctionName=lambda_arn,
        S3Bucket=bucket,
        S3Key=zip_key,
    )
    task_logger.info(
        "Lambda updated: version=%s state=%s",
        response.get("Version"),
        response.get("LastUpdateStatus"),
    )

    return {
        "uri": uri,
        "lambda_arn": lambda_arn,
        "last_update_status": response.get("LastUpdateStatus"),
    }
