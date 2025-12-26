"""Dagster resources for the banking pipeline."""

from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource

__all__ = ["DuckDBResource", "S3Resource"]

