import logging
import os
from pathlib import Path

import boto3


def upload_file_to_s3(
    local_path: Path,
    bucket: str,
    key: str,
    logger: logging.Logger | None = None,
    s3_client: str | None = None,
) -> str:
    """Upload a local file to S3.

    Args:
        local_path: Path to the local file.
        bucket: S3 bucket name.
        key: S3 object key (e.g. "routing_graph/costs.parquet").
        logger: Optional logger instance. Falls back to module logger.
        s3_client: Optional boto3 S3 client. Created if not provided.

    Returns:
        The s3:// URI of the uploaded file.
    """
    if logger is None:
        logger = logging.getLogger(__name__)

    if s3_client is None:
        s3_client = boto3.client("s3")

    s3_client.upload_file(local_path, bucket, key)

    size_mb = os.path.getsize(local_path) / 1024 / 1024
    uri = f"s3://{bucket}/{key}"
    logger.info("Uploaded %s (%.1f MB)", uri, size_mb)

    return uri
