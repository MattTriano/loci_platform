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

import json
import subprocess
import tempfile
import time
import zipfile
from logging import Logger
from pathlib import Path

from airflow.sdk import task
from loci.aws import get_boto_session
from loci.db.af_utils import get_postgres_engine
from loci.deploy import upload_file_to_s3
from loci.environments import EnvConfig, get_env
from loci.exports.graph_export import RoutingGraphExporter

BIKE_MAP_APP_DIR = "/opt/airflow/app-files/bike-map"
BIKE_MAP_EXPORT_DIR_BASE = "/opt/airflow/exports/bike-map"
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


def _sync_to_s3(local_dirs: list[str], cfg: EnvConfig, logger: Logger) -> int:
    """Upload files from multiple local directories to the S3 bucket.

    Each directory is synced relative to itself, preserving structure.
    Only files with extensions in CONTENT_TYPES are uploaded.
    Subdirectories listed in SKIP_DIRS are excluded.

    For example, given:
        /opt/airflow/app-files/bike-map/index.html              → index.html
        /opt/airflow/exports/bike-map/dev/data/crashes.geojson  → data/crashes.geojson

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
    """Create a CloudFront invalidation for all paths.

    Returns the invalidation ID.
    """
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
    routing API URL, routing API key, and route-logger endpoint from cfg
    (all of which were sourced from SSM), and uploads the result as
    config.json in the S3 bucket root.

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
def export_routing_graph(env: str, city: str, conn_id: str, task_logger: Logger) -> dict:
    """Build the stress-weighted routing graph and upload it to S3."""
    cfg = get_env(env, city)
    session = get_boto_session(cfg)
    engine = get_postgres_engine(conn_id=conn_id, logger=task_logger)
    exporter = RoutingGraphExporter(engine, marts_schema=cfg.marts_schema)

    with tempfile.TemporaryDirectory() as tmpdir:
        output_path = Path(tmpdir) / "routing_graph.pkl.gz"
        exporter.export(output_path)
        uri = upload_file_to_s3(
            local_path=output_path,
            bucket=cfg.routing_graph_bucket,
            key=cfg.routing_graph_key,
            logger=task_logger,
            s3_client=session.client("s3"),
        )

    task_logger.info("Routing graph uploaded to %s", uri)
    return {"uri": uri, "bucket": cfg.routing_graph_bucket, "key": cfg.routing_graph_key}


@task
def deploy_lambda(env: str, city: str, task_logger: Logger) -> dict:
    """Build the Lambda deployment package and update the function code."""
    cfg = get_env(env, city)
    session = get_boto_session(cfg)
    zip_key = "lambda/routing_api.zip"

    with tempfile.TemporaryDirectory() as tmpdir:
        zip_path = Path(tmpdir) / "routing_api.zip"
        task_logger.info("Building Lambda zip at %s", zip_path)
        build_lambda_zip(zip_path)

        size_mb = zip_path.stat().st_size / 1_048_576
        task_logger.info("Package size: %.1f MB", size_mb)

        uri = upload_file_to_s3(
            local_path=zip_path,
            bucket=cfg.routing_graph_bucket,
            key=zip_key,
            logger=task_logger,
            s3_client=session.client("s3"),
        )

    task_logger.info("Lambda package uploaded to %s", uri)

    lam = session.client("lambda")
    response = lam.update_function_code(
        FunctionName=cfg.routing_lambda_arn,
        S3Bucket=cfg.routing_graph_bucket,
        S3Key=zip_key,
    )
    task_logger.info(
        "Lambda updated: version=%s state=%s",
        response.get("Version"),
        response.get("LastUpdateStatus"),
    )

    return {
        "uri": uri,
        "lambda_arn": cfg.routing_lambda_arn,
        "last_update_status": response.get("LastUpdateStatus"),
    }
