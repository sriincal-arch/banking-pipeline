"""
S3/MinIO Resource - Object Storage Operations

This resource provides a unified interface for S3-compatible storage (MinIO):
- File listing and existence checks
- File upload/download (bytes and CSV DataFrames)
- File hash calculation (MD5) for idempotency
- Bucket management
"""

import hashlib
import os
from io import BytesIO
from typing import List, Optional

import boto3
import pandas as pd
from botocore.client import Config
from dagster import ConfigurableResource


class S3Resource(ConfigurableResource):
    """
    Resource for managing S3/MinIO operations.
    
    Configured to work with MinIO (S3-compatible object storage) for local development.
    Supports file operations, DataFrame uploads/downloads, and hash calculation.
    """

    endpoint_url: str = os.environ.get("MINIO_ENDPOINT", "http://localhost:9000")
    access_key: str = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
    secret_key: str = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
    region_name: str = "us-east-1"

    def _get_client(self):
        """Get an S3 client configured for MinIO."""
        return boto3.client(
            "s3",
            endpoint_url=self.endpoint_url,
            aws_access_key_id=self.access_key,
            aws_secret_access_key=self.secret_key,
            region_name=self.region_name,
            config=Config(signature_version="s3v4"),
        )

    def list_files(self, bucket: str, prefix: str = "") -> List[dict]:
        """List files in a bucket with optional prefix."""
        client = self._get_client()
        response = client.list_objects_v2(Bucket=bucket, Prefix=prefix)
        files = []
        for obj in response.get("Contents", []):
            files.append({
                "key": obj["Key"],
                "size": obj["Size"],
                "last_modified": obj["LastModified"],
            })
        return files

    def download_file(self, bucket: str, key: str) -> bytes:
        """Download a file from S3/MinIO."""
        client = self._get_client()
        response = client.get_object(Bucket=bucket, Key=key)
        return response["Body"].read()

    def download_csv_as_df(self, bucket: str, key: str) -> pd.DataFrame:
        """Download a CSV file and return as DataFrame."""
        content = self.download_file(bucket, key)
        return pd.read_csv(BytesIO(content))

    def upload_file(self, bucket: str, key: str, data: bytes) -> None:
        """Upload data to S3/MinIO."""
        client = self._get_client()
        client.put_object(Bucket=bucket, Key=key, Body=data)

    def upload_df_as_csv(self, bucket: str, key: str, df: pd.DataFrame) -> None:
        """Upload a DataFrame as CSV to S3/MinIO."""
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index=False)
        csv_buffer.seek(0)
        self.upload_file(bucket, key, csv_buffer.getvalue())

    def get_file_hash(self, bucket: str, key: str) -> str:
        """
        Calculate MD5 hash of a file for idempotency checks.
        
        Used to prevent duplicate processing of the same file content.
        """
        content = self.download_file(bucket, key)
        return hashlib.md5(content).hexdigest()

    def get_file_size(self, bucket: str, key: str) -> int:
        """Get the size of a file in bytes."""
        client = self._get_client()
        response = client.head_object(Bucket=bucket, Key=key)
        return response["ContentLength"]

    def file_exists(self, bucket: str, key: str) -> bool:
        """Check if a file exists in S3/MinIO."""
        client = self._get_client()
        try:
            client.head_object(Bucket=bucket, Key=key)
            return True
        except client.exceptions.ClientError:
            return False

    def ensure_bucket_exists(self, bucket: str) -> None:
        """Create a bucket if it doesn't exist."""
        client = self._get_client()
        try:
            client.head_bucket(Bucket=bucket)
        except client.exceptions.ClientError:
            client.create_bucket(Bucket=bucket)

