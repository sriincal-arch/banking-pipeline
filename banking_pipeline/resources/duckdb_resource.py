"""
DuckDB Resource - Database Connection Management

This resource provides a unified interface for DuckDB operations:
- Connection management with context managers
- Query execution (execute, fetch_all, fetch_df)
- Schema initialization (raw, structured, curated, access)
- Metadata table creation (file_ingestion_metadata, schema_version_tracking, process_metadata)
"""

import os
import time
from contextlib import contextmanager
from typing import Generator
from functools import wraps

import duckdb
from dagster import ConfigurableResource, InitResourceContext


def retry_on_lock(max_retries=5, delay=1.0):
    """
    Decorator to retry database operations on lock conflicts.

    Uses exponential backoff to handle transient DuckDB lock conflicts when
    multiple processes try to access the database file simultaneously.

    Args:
        max_retries: Maximum number of retry attempts (default: 5)
        delay: Initial delay in seconds, doubled on each retry (default: 1.0)
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except duckdb.IOException as e:
                    error_msg = str(e)
                    if "Could not set lock" in error_msg and attempt < max_retries - 1:
                        wait_time = delay * (2 ** attempt)
                        print(f"DuckDB lock conflict detected (attempt {attempt + 1}/{max_retries}). Retrying in {wait_time}s...")
                        time.sleep(wait_time)
                        continue
                    # If it's the last attempt or not a lock error, raise it
                    raise
            return func(*args, **kwargs)
        return wrapper
    return decorator


class DuckDBResource(ConfigurableResource):
    """
    Resource for managing DuckDB connections and operations.
    
    Provides methods for executing queries, fetching results, and initializing
    the database schema structure for the medallion architecture.
    """

    database_path: str = os.environ.get("DUCKDB_DATABASE", "data/banking.duckdb")

    @contextmanager
    def get_connection(self, read_only: bool = False) -> Generator[duckdb.DuckDBPyConnection, None, None]:
        """
        Get a DuckDB connection context manager.

        Args:
            read_only: If True, opens the database in read-only mode. This allows
                      multiple concurrent read connections without lock conflicts.
                      Use read_only=True for queries (SELECT), False for writes.
        """
        conn = duckdb.connect(self.database_path, read_only=read_only)
        try:
            yield conn
        finally:
            conn.close()

    @retry_on_lock(max_retries=5, delay=1.0)
    def execute(self, query: str, params: tuple = None) -> None:
        """
        Execute a query without returning results (INSERT, UPDATE, DELETE, CREATE, etc.).

        Includes retry logic with exponential backoff to handle lock conflicts.
        """
        with self.get_connection(read_only=False) as conn:
            if params:
                conn.execute(query, params)
            else:
                conn.execute(query)

    def fetch_all(self, query: str, params: tuple = None) -> list:
        """
        Execute a query and return all results.

        Uses read-only connection to allow concurrent reads without lock conflicts.
        """
        with self.get_connection(read_only=True) as conn:
            if params:
                result = conn.execute(query, params).fetchall()
            else:
                result = conn.execute(query).fetchall()
            return result

    def fetch_df(self, query: str, params: tuple = None):
        """
        Execute a query and return results as a DataFrame.

        Uses read-only connection to allow concurrent reads without lock conflicts.
        """
        with self.get_connection(read_only=True) as conn:
            if params:
                result = conn.execute(query, params).fetchdf()
            else:
                result = conn.execute(query).fetchdf()
            return result

    def initialize_schemas(self) -> None:
        """
        Initialize database schemas for the medallion architecture.
        
        Creates four schemas:
        - raw: Append-only raw data from source files
        - structured: Cleansed and standardized data (upserts)
        - curated: Dimensional models (facts/dimensions)
        - access: Final reports and exports
        """
        with self.get_connection() as conn:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw")
            conn.execute("CREATE SCHEMA IF NOT EXISTS structured")
            conn.execute("CREATE SCHEMA IF NOT EXISTS curated")
            conn.execute("CREATE SCHEMA IF NOT EXISTS access")

    def initialize_metadata_tables(self) -> None:
        """
        Create metadata tracking tables for audit and lineage.
        
        Creates:
        - file_ingestion_metadata: Tracks file processing history (hash, size, status)
        - schema_version_tracking: Tracks schema changes over time
        - process_metadata: Tracks curated layer processing runs (records processed, duration)
        """
        with self.get_connection() as conn:
            # File ingestion metadata table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw.file_ingestion_metadata (
                    file_id VARCHAR PRIMARY KEY,
                    file_path VARCHAR NOT NULL,
                    file_name VARCHAR NOT NULL,
                    file_type VARCHAR NOT NULL,
                    file_hash VARCHAR NOT NULL,
                    file_size_bytes BIGINT,
                    row_count INTEGER,
                    ingestion_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    processing_status VARCHAR DEFAULT 'pending',
                    error_message VARCHAR,
                    UNIQUE(file_hash)
                )
            """)

            # Schema version tracking table
            conn.execute("""
                CREATE TABLE IF NOT EXISTS raw.schema_version_tracking (
                    schema_id VARCHAR PRIMARY KEY,
                    table_name VARCHAR NOT NULL,
                    schema_version INTEGER NOT NULL,
                    column_definitions JSON NOT NULL,
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    previous_version_id VARCHAR,
                    change_description VARCHAR
                )
            """)

            # Process metadata table for curated layer
            conn.execute("""
                CREATE TABLE IF NOT EXISTS curated.process_metadata (
                    run_id VARCHAR PRIMARY KEY,
                    run_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    source_tables JSON,
                    target_table VARCHAR,
                    records_processed INTEGER,
                    records_inserted INTEGER,
                    records_updated INTEGER,
                    status VARCHAR,
                    duration_seconds FLOAT,
                    error_message VARCHAR
                )
            """)

