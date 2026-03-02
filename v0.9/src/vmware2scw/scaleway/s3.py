"""Scaleway S3-compatible Object Storage operations."""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Optional

import boto3
from botocore.config import Config

from vmware2scw.utils.logging import get_logger

logger = get_logger(__name__)


class ScalewayS3:
    """Manages uploads to Scaleway Object Storage (S3-compatible).

    Scaleway Object Storage is fully S3-compatible, using boto3 with
    a custom endpoint URL.

    Confidence: 92 — S3 API is well-established and Scaleway's
    implementation is mature.
    """

    def __init__(self, region: str, access_key: str, secret_key: str):
        self.region = region
        self.endpoint_url = f"https://s3.{region}.scw.cloud"

        self.client = boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
            config=Config(
                retries={"max_attempts": 3, "mode": "adaptive"},
                max_pool_connections=10,
            ),
        )
        self.resource = boto3.resource(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name=region,
        )

        logger.info(f"Initialized Scaleway S3 client (region: {region})")

    def create_bucket_if_not_exists(self, bucket: str) -> None:
        """Create the transit bucket if it doesn't exist."""
        try:
            self.client.head_bucket(Bucket=bucket)
            logger.info(f"Bucket '{bucket}' already exists")
        except self.client.exceptions.ClientError as e:
            error_code = e.response["Error"]["Code"]
            if error_code == "404" or error_code == "NoSuchBucket":
                logger.info(f"Creating bucket '{bucket}'...")
                self.client.create_bucket(Bucket=bucket)
                logger.info(f"Bucket '{bucket}' created")
            else:
                raise

    def upload_image(
        self,
        local_path: str | Path,
        bucket: str,
        key: str,
        progress_callback: Optional[Callable[[int], None]] = None,
    ) -> str:
        """Upload a qcow2 image to S3 using multipart upload.

        Uses boto3's managed upload which automatically handles:
        - Multipart upload for large files
        - Retry logic
        - Concurrency

        Args:
            local_path: Path to local qcow2 file
            bucket: S3 bucket name
            key: Object key (path within bucket)
            progress_callback: Optional callback(bytes_transferred)

        Returns:
            S3 URL of the uploaded image
        """
        local_path = Path(local_path)
        if not local_path.exists():
            raise FileNotFoundError(f"Image file not found: {local_path}")

        file_size = local_path.stat().st_size
        logger.info(
            f"Uploading {local_path.name} ({file_size / (1024**3):.2f} GB) "
            f"to s3://{bucket}/{key}"
        )

        # Configure multipart upload
        transfer_config = boto3.s3.transfer.TransferConfig(
            multipart_threshold=64 * 1024 * 1024,     # 64MB threshold
            multipart_chunksize=64 * 1024 * 1024,     # 64MB chunks
            max_concurrency=4,
            use_threads=True,
        )

        # Progress tracking
        class ProgressTracker:
            def __init__(self, total_size, callback):
                self.total_size = total_size
                self.transferred = 0
                self.callback = callback
                self.last_logged_pct = -5  # Track last logged percentage

            def __call__(self, bytes_amount):
                self.transferred += bytes_amount
                if self.callback:
                    self.callback(self.transferred)
                # Log every 5%
                pct = self.transferred / self.total_size * 100
                if pct - self.last_logged_pct >= 5:
                    logger.info(f"Upload progress: {pct:.0f}% ({self.transferred / (1024**3):.2f} GB)")
                    self.last_logged_pct = pct

        tracker = ProgressTracker(file_size, progress_callback)

        self.client.upload_file(
            str(local_path),
            bucket,
            key,
            Config=transfer_config,
            Callback=tracker,
        )

        url = f"{self.endpoint_url}/{bucket}/{key}"
        logger.info(f"Upload complete: {url}")
        return url

    def check_object_exists(self, bucket: str, key: str) -> bool:
        """Check if an object already exists in S3."""
        try:
            self.client.head_object(Bucket=bucket, Key=key)
            return True
        except self.client.exceptions.ClientError:
            return False

    def get_object_size(self, bucket: str, key: str) -> int:
        """Get the size of an S3 object in bytes."""
        response = self.client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]

    def delete_object(self, bucket: str, key: str) -> None:
        """Delete an object from S3."""
        logger.info(f"Deleting s3://{bucket}/{key}")
        self.client.delete_object(Bucket=bucket, Key=key)

    def generate_presigned_url(self, bucket: str, key: str, expires_in: int = 3600) -> str:
        """Generate a presigned URL for the object.

        Useful for Scaleway's snapshot import API which may need
        a direct URL to the qcow2 file.
        """
        return self.client.generate_presigned_url(
            "get_object",
            Params={"Bucket": bucket, "Key": key},
            ExpiresIn=expires_in,
        )

    def list_objects(self, bucket: str, prefix: str = "") -> list[dict]:
        """List objects in a bucket with optional prefix filter."""
        paginator = self.client.get_paginator("list_objects_v2")
        objects = []
        for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
            for obj in page.get("Contents", []):
                objects.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"],
                })
        return objects
