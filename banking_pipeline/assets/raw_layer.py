"""
Raw Layer Assets - Data Ingestion from S3/MinIO

This module handles the raw layer of the medallion architecture:
- Ingests CSV files from MinIO landing bucket
- Appends data to raw tables (append-only, no updates)
- Tracks file metadata and schema versions for idempotency
- Detects schema changes and maintains version history
"""

import hashlib
import json
import uuid
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from dagster import (
    AssetExecutionContext,
    AssetIn,
    MaterializeResult,
    MetadataValue,
    asset,
)

from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource


def _detect_schema(df: pd.DataFrame) -> List[Dict[str, Any]]:
    """
    Detect schema from DataFrame columns.
    
    Analyzes the DataFrame structure to extract column names, types, and nullability.
    Used for schema version tracking to detect changes in input files.
    """
    schema = []
    for col in df.columns:
        dtype = str(df[col].dtype)
        nullable = df[col].isnull().any()
        schema.append({
            "name": col,
            "type": dtype,
            "nullable": bool(nullable),
        })
    return schema


def _check_schema_change(
    duckdb: DuckDBResource,
    table_name: str,
    new_schema: List[Dict[str, Any]],
) -> tuple[bool, int, str | None]:
    """
    Check if schema has changed compared to the last recorded version.
    
    Returns:
        tuple: (has_changed: bool, new_version: int, previous_schema_id: str | None)
    """
    result = duckdb.fetch_all(
        """
        SELECT schema_id, schema_version, column_definitions
        FROM raw.schema_version_tracking
        WHERE table_name = ?
        ORDER BY schema_version DESC
        LIMIT 1
        """,
        (table_name,),
    )

    if not result:
        return True, 1, None

    prev_id, prev_version, prev_schema_json = result[0]
    prev_schema = json.loads(prev_schema_json) if isinstance(prev_schema_json, str) else prev_schema_json

    # Compare schemas
    if prev_schema != new_schema:
        return True, prev_version + 1, prev_id

    return False, prev_version, prev_id


def _record_schema_version(
    duckdb: DuckDBResource,
    table_name: str,
    schema: List[Dict[str, Any]],
    version: int,
    prev_id: str | None,
    change_desc: str,
) -> str:
    """
    Record a new schema version in the schema_version_tracking table.
    
    Creates a new schema version entry with a unique ID and links it to the previous version.
    """
    schema_id = str(uuid.uuid4())
    duckdb.execute(
        """
        INSERT INTO raw.schema_version_tracking 
        (schema_id, table_name, schema_version, column_definitions, previous_version_id, change_description)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (schema_id, table_name, version, json.dumps(schema), prev_id, change_desc),
    )
    return schema_id


def _check_file_processed(duckdb: DuckDBResource, file_hash: str) -> bool:
    """
    Check if a file has already been processed (idempotency check).
    
    Uses MD5 hash to prevent duplicate processing of the same file.
    """
    result = duckdb.fetch_all(
        "SELECT 1 FROM raw.file_ingestion_metadata WHERE file_hash = ? AND processing_status = 'completed'",
        (file_hash,),
    )
    return len(result) > 0


def _record_file_metadata(
    duckdb: DuckDBResource,
    file_path: str,
    file_name: str,
    file_type: str,
    file_hash: str,
    file_size: int,
    row_count: int,
    status: str = "completed",
    error_message: str = None,
) -> str:
    """
    Record file ingestion metadata in file_ingestion_metadata table.
    
    Tracks file processing history including hash, size, row count, and status.
    Uses ON CONFLICT to update existing records if the same file is processed again.
    """
    file_id = str(uuid.uuid4())
    current_ts = datetime.now().isoformat()
    duckdb.execute(
        """
        INSERT INTO raw.file_ingestion_metadata
        (file_id, file_path, file_name, file_type, file_hash, file_size_bytes, row_count, ingestion_timestamp, processing_status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT (file_hash) DO UPDATE SET
            processing_status = EXCLUDED.processing_status,
            error_message = EXCLUDED.error_message,
            ingestion_timestamp = EXCLUDED.ingestion_timestamp
        """,
        (file_id, file_path, file_name, file_type, file_hash, file_size, row_count, current_ts, status, error_message),
    )
    return file_id


@asset(
    group_name="raw",
    description="Initialize database schemas and metadata tables",
)
def initialize_database(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Initialize the DuckDB database with required schemas and metadata tables.
    
    Creates:
    - Schemas: raw, structured, curated, access
    - Metadata tables: file_ingestion_metadata, schema_version_tracking, process_metadata
    """
    context.log.info("Initializing database schemas...")
    duckdb.initialize_schemas()

    context.log.info("Initializing metadata tables...")
    duckdb.initialize_metadata_tables()

    return MaterializeResult(
        metadata={
            "schemas_created": MetadataValue.text("raw, structured, curated, access"),
            "metadata_tables": MetadataValue.text(
                "file_ingestion_metadata, schema_version_tracking, process_metadata"
            ),
        }
    )


@asset(
    group_name="raw",
    deps=["initialize_database"],
    description="Ingest accounts CSV from MinIO into raw layer",
)
def raw_accounts(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    s3: S3Resource,
) -> MaterializeResult:
    """
    Ingest accounts.csv from MinIO landing bucket into raw.raw_accounts table.
    
    Processing steps:
    1. Check if file exists in landing bucket (warns if not found, doesn't fail)
    2. Calculate file hash for idempotency check
    3. Skip if file already processed
    4. Download and parse CSV
    5. Add metadata columns (_source_file, _file_hash, _row_number, _ingested_at)
    6. Detect and track schema changes
    7. Append to raw table (append-only, no updates)
    8. Record file metadata
    
    Returns MaterializeResult with row count, file hash, and schema version.
    """
    bucket = "landing"
    key = "accounts.csv"
    table_name = "raw_accounts"

    context.log.info(f"Processing {bucket}/{key}...")

    # Check if file exists
    files = s3.list_files(bucket, "accounts")
    if not files:
        warning_msg = f"⚠️  WARNING: No accounts files found in '{bucket}' bucket. Please upload accounts.csv to the landing bucket."
        context.log.warning(warning_msg)
        context.log.warning("Pipeline will continue but raw_accounts table will not be populated.")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("⚠️ NO FILES FOUND"),
                "warning": MetadataValue.text(warning_msg),
                "bucket": MetadataValue.text(bucket),
                "expected_file": MetadataValue.text("accounts.csv"),
            }
        )

    # Get the latest accounts file
    latest_file = sorted(files, key=lambda x: x["last_modified"], reverse=True)[0]
    key = latest_file["key"]

    # Calculate file hash for idempotency
    file_hash = s3.get_file_hash(bucket, key)
    file_size = latest_file["size"]

    # Check if already processed
    if _check_file_processed(duckdb, file_hash):
        context.log.info(f"File {key} already processed (hash: {file_hash})")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("Skipped - already processed"),
                "file_hash": MetadataValue.text(file_hash),
            }
        )

    try:
        # Download and parse CSV
        df = s3.download_csv_as_df(bucket, key)
        row_count = len(df)

        # Add metadata columns
        df["_source_file"] = f"{bucket}/{key}"
        df["_file_hash"] = file_hash
        df["_row_number"] = range(1, len(df) + 1)
        df["_ingested_at"] = datetime.now().isoformat()

        # Detect and track schema changes
        schema = _detect_schema(df.drop(columns=["_source_file", "_file_hash", "_row_number", "_ingested_at"]))
        has_changed, version, prev_id = _check_schema_change(duckdb, table_name, schema)

        if has_changed:
            change_desc = "Initial schema" if version == 1 else "Schema change detected"
            _record_schema_version(duckdb, table_name, schema, version, prev_id, change_desc)
            context.log.info(f"Schema version {version} recorded for {table_name}")

        # Append to raw table (create if not exists, then insert)
        with duckdb.get_connection() as conn:
            # Create table if not exists with same structure
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{table_name} AS 
                SELECT * FROM df WHERE 1=0
            """)
            # Append new data (raw layer is append-only)
            conn.execute(f"INSERT INTO raw.{table_name} SELECT * FROM df")

        # Record file metadata
        _record_file_metadata(
            duckdb, f"{bucket}/{key}", key, "accounts", file_hash, file_size, row_count
        )

        context.log.info(f"Successfully ingested {row_count} rows into raw.{table_name}")

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "file_path": MetadataValue.text(f"{bucket}/{key}"),
                "file_hash": MetadataValue.text(file_hash),
                "schema_version": MetadataValue.int(version),
                "columns": MetadataValue.json(schema),
            }
        )

    except Exception as e:
        _record_file_metadata(
            duckdb, f"{bucket}/{key}", key, "accounts", file_hash, file_size, 0, "failed", str(e)
        )
        raise


@asset(
    group_name="raw",
    deps=["initialize_database"],
    description="Ingest customers CSV from MinIO into raw layer",
)
def raw_customers(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    s3: S3Resource,
) -> MaterializeResult:
    """
    Ingest customers.csv from MinIO landing bucket into raw.raw_customers table.
    
    Same processing logic as raw_accounts:
    - Idempotent file processing via hash check
    - Schema version tracking
    - Append-only to raw table
    - Metadata recording
    """
    bucket = "landing"
    key = "customers.csv"
    table_name = "raw_customers"

    context.log.info(f"Processing {bucket}/{key}...")

    # Check if file exists
    files = s3.list_files(bucket, "customers")
    if not files:
        warning_msg = f"⚠️  WARNING: No customers files found in '{bucket}' bucket. Please upload customers.csv to the landing bucket."
        context.log.warning(warning_msg)
        context.log.warning("Pipeline will continue but raw_customers table will not be populated.")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("⚠️ NO FILES FOUND"),
                "warning": MetadataValue.text(warning_msg),
                "bucket": MetadataValue.text(bucket),
                "expected_file": MetadataValue.text("customers.csv"),
            }
        )

    # Get the latest customers file
    latest_file = sorted(files, key=lambda x: x["last_modified"], reverse=True)[0]
    key = latest_file["key"]

    # Calculate file hash for idempotency
    file_hash = s3.get_file_hash(bucket, key)
    file_size = latest_file["size"]

    # Check if already processed
    if _check_file_processed(duckdb, file_hash):
        context.log.info(f"File {key} already processed (hash: {file_hash})")
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("Skipped - already processed"),
                "file_hash": MetadataValue.text(file_hash),
            }
        )

    try:
        # Download and parse CSV
        df = s3.download_csv_as_df(bucket, key)
        row_count = len(df)

        # Add metadata columns
        df["_source_file"] = f"{bucket}/{key}"
        df["_file_hash"] = file_hash
        df["_row_number"] = range(1, len(df) + 1)
        df["_ingested_at"] = datetime.now().isoformat()

        # Detect and track schema changes
        schema = _detect_schema(df.drop(columns=["_source_file", "_file_hash", "_row_number", "_ingested_at"]))
        has_changed, version, prev_id = _check_schema_change(duckdb, table_name, schema)

        if has_changed:
            change_desc = "Initial schema" if version == 1 else "Schema change detected"
            _record_schema_version(duckdb, table_name, schema, version, prev_id, change_desc)
            context.log.info(f"Schema version {version} recorded for {table_name}")

        # Append to raw table (create if not exists, then insert)
        with duckdb.get_connection() as conn:
            # Create table if not exists with same structure
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS raw.{table_name} AS 
                SELECT * FROM df WHERE 1=0
            """)
            # Append new data (raw layer is append-only)
            conn.execute(f"INSERT INTO raw.{table_name} SELECT * FROM df")

        # Record file metadata
        _record_file_metadata(
            duckdb, f"{bucket}/{key}", key, "customers", file_hash, file_size, row_count
        )

        context.log.info(f"Successfully ingested {row_count} rows into raw.{table_name}")

        return MaterializeResult(
            metadata={
                "row_count": MetadataValue.int(row_count),
                "file_path": MetadataValue.text(f"{bucket}/{key}"),
                "file_hash": MetadataValue.text(file_hash),
                "schema_version": MetadataValue.int(version),
                "columns": MetadataValue.json(schema),
            }
        )

    except Exception as e:
        _record_file_metadata(
            duckdb, f"{bucket}/{key}", key, "customers", file_hash, file_size, 0, "failed", str(e)
        )
        raise

