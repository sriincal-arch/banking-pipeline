"""
Shared fixtures for Dagster tests.

Provides mock resources and test database setup for testing assets.
"""

import os
import tempfile
from unittest.mock import MagicMock, Mock
from typing import Generator

import duckdb  # type: ignore[import-untyped]
import pandas as pd  # type: ignore[import-untyped]
import pytest  # type: ignore[import-untyped]

from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource


@pytest.fixture
def temp_db_path() -> Generator[str, None, None]:
    """Create a temporary DuckDB database file for testing."""
    with tempfile.NamedTemporaryFile(suffix=".duckdb", delete=False) as f:
        db_path = f.name
    
    yield db_path
    
    # Cleanup
    if os.path.exists(db_path):
        os.unlink(db_path)


@pytest.fixture
def mock_duckdb(temp_db_path: str) -> DuckDBResource:
    """Create a DuckDB resource pointing to a temporary database."""
    return DuckDBResource(database_path=temp_db_path)


@pytest.fixture
def initialized_duckdb(mock_duckdb: DuckDBResource) -> DuckDBResource:
    """Create and initialize a DuckDB resource with schemas and metadata tables."""
    mock_duckdb.initialize_schemas()
    mock_duckdb.initialize_metadata_tables()
    return mock_duckdb


@pytest.fixture
def mock_s3() -> S3Resource:
    """Create a mock S3 resource for testing."""
    s3_mock = Mock(spec=S3Resource)
    
    # Sample CSV data for accounts
    accounts_df = pd.DataFrame({
        "AccountID": ["A001", "A002", "A003"],
        "CustomerID": ["101", "102", "103"],
        "Balance": [10000.0, 20000.0, 15000.0],
        "AccountType": ["Savings", "Checking", "Savings"],
    })
    
    # Sample CSV data for customers
    customers_df = pd.DataFrame({
        "CustomerID": ["101", "102", "103"],
        "Name": ["Alice Smith", "Bob Jones", "Charlie Brown"],
        "HasLoan": ["Yes", "No", "Yes"],
    })
    
    # Mock list_files to return file listings
    def list_files(bucket: str, prefix: str = ""):
        if prefix == "accounts":
            return [{
                "key": "accounts.csv",
                "size": 1024,
                "last_modified": pd.Timestamp.now(),
            }]
        elif prefix == "customers":
            return [{
                "key": "customers.csv",
                "size": 512,
                "last_modified": pd.Timestamp.now(),
            }]
        return []
    
    def download_csv_as_df(bucket: str, key: str):
        if key == "accounts.csv":
            return accounts_df.copy()
        elif key == "customers.csv":
            return customers_df.copy()
        return pd.DataFrame()
    
    def get_file_hash(bucket: str, key: str):
        # Return a consistent hash for testing
        return "test_hash_12345"
    
    s3_mock.list_files = Mock(side_effect=list_files)
    s3_mock.download_csv_as_df = Mock(side_effect=download_csv_as_df)
    s3_mock.get_file_hash = Mock(side_effect=get_file_hash)
    s3_mock.upload_df_as_csv = Mock()
    s3_mock.upload_file = Mock()
    s3_mock.file_exists = Mock(return_value=True)
    
    return s3_mock


@pytest.fixture
def sample_raw_data(initialized_duckdb: DuckDBResource) -> DuckDBResource:
    """Load sample raw data into the test database."""
    # Insert sample accounts data using SQL
    with initialized_duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_accounts (
                AccountID VARCHAR,
                CustomerID VARCHAR,
                Balance DOUBLE,
                AccountType VARCHAR,
                _source_file VARCHAR,
                _file_hash VARCHAR,
                _row_number INTEGER,
                _ingested_at VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO raw.raw_accounts VALUES
            ('A001', '101', 10000.0, 'Savings', 'landing/accounts.csv', 'hash1', 1, '2024-01-01T00:00:00'),
            ('A002', '102', 20000.0, 'Checking', 'landing/accounts.csv', 'hash1', 2, '2024-01-01T00:00:00'),
            ('A003', '103', 15000.0, 'Savings', 'landing/accounts.csv', 'hash1', 3, '2024-01-01T00:00:00')
        """)
    
    # Insert sample customers data using SQL
    with initialized_duckdb.get_connection() as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS raw.raw_customers (
                CustomerID VARCHAR,
                Name VARCHAR,
                HasLoan VARCHAR,
                _source_file VARCHAR,
                _file_hash VARCHAR,
                _row_number INTEGER,
                _ingested_at VARCHAR
            )
        """)
        conn.execute("""
            INSERT INTO raw.raw_customers VALUES
            ('101', 'Alice Smith', 'Yes', 'landing/customers.csv', 'hash2', 1, '2024-01-01T00:00:00'),
            ('102', 'Bob Jones', 'No', 'landing/customers.csv', 'hash2', 2, '2024-01-01T00:00:00'),
            ('103', 'Charlie Brown', 'Yes', 'landing/customers.csv', 'hash2', 3, '2024-01-01T00:00:00')
        """)
    
    return initialized_duckdb

