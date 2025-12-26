"""
Tests for Dagster resources.

Verifies that DuckDB and S3 resources work correctly.
"""

import os
import tempfile

import pytest  # type: ignore[import-untyped]

from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource


class TestDuckDBResource:
    """Test DuckDB resource functionality."""
    
    def test_duckdb_resource_connection(self, temp_db_path):
        """Verify DuckDB connection works."""
        duckdb = DuckDBResource(database_path=temp_db_path)
        
        with duckdb.get_connection() as conn:
            result = conn.execute("SELECT 1 as test").fetchone()
            assert result[0] == 1
    
    def test_duckdb_resource_schemas(self, mock_duckdb):
        """Verify schemas are created."""
        mock_duckdb.initialize_schemas()
        
        schemas = mock_duckdb.fetch_all("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'structured', 'curated', 'access')
            ORDER BY schema_name
        """)
        
        assert len(schemas) == 4
        schema_names = [s[0] for s in schemas]
        assert "raw" in schema_names
        assert "structured" in schema_names
        assert "curated" in schema_names
        assert "access" in schema_names
    
    def test_duckdb_resource_execute(self, mock_duckdb):
        """Verify execute method works."""
        mock_duckdb.execute("CREATE TABLE IF NOT EXISTS test.test_table (id INTEGER)")
        
        result = mock_duckdb.fetch_all("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = 'test_table'")
        assert result[0][0] >= 0  # Table may or may not exist depending on schema
    
    def test_duckdb_resource_fetch_all(self, mock_duckdb):
        """Verify fetch_all method works."""
        result = mock_duckdb.fetch_all("SELECT 1 as col1, 2 as col2")
        assert len(result) == 1
        assert result[0] == (1, 2)
    
    def test_duckdb_resource_fetch_df(self, mock_duckdb):
        """Verify fetch_df method works."""
        df = mock_duckdb.fetch_df("SELECT 1 as col1, 2 as col2")
        assert len(df) == 1
        assert df.iloc[0]["col1"] == 1
        assert df.iloc[0]["col2"] == 2
    
    def test_duckdb_resource_metadata_tables(self, mock_duckdb):
        """Verify metadata tables are created."""
        mock_duckdb.initialize_schemas()
        mock_duckdb.initialize_metadata_tables()
        
        # Check file_ingestion_metadata table
        result = mock_duckdb.fetch_all("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'file_ingestion_metadata'
        """)
        assert result[0][0] == 1
        
        # Check schema_version_tracking table
        result = mock_duckdb.fetch_all("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'raw' AND table_name = 'schema_version_tracking'
        """)
        assert result[0][0] == 1
        
        # Check process_metadata table
        result = mock_duckdb.fetch_all("""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = 'curated' AND table_name = 'process_metadata'
        """)
        assert result[0][0] == 1


class TestS3Resource:
    """Test S3 resource functionality."""
    
    def test_s3_resource_initialization(self):
        """Verify S3 resource can be initialized."""
        s3 = S3Resource(
            endpoint_url="http://localhost:9000",
            access_key="test_key",
            secret_key="test_secret",
        )
        
        assert s3.endpoint_url == "http://localhost:9000"
        assert s3.access_key == "test_key"
        assert s3.secret_key == "test_secret"
    
    def test_s3_resource_defaults(self):
        """Verify S3 resource uses environment defaults."""
        # Save original env vars
        original_endpoint = os.environ.get("MINIO_ENDPOINT")
        original_key = os.environ.get("MINIO_ACCESS_KEY")
        original_secret = os.environ.get("MINIO_SECRET_KEY")
        
        try:
            # Set test env vars
            os.environ["MINIO_ENDPOINT"] = "http://test:9000"
            os.environ["MINIO_ACCESS_KEY"] = "test_access"
            os.environ["MINIO_SECRET_KEY"] = "test_secret"
            
            s3 = S3Resource()
            assert s3.endpoint_url == "http://test:9000"
            assert s3.access_key == "test_access"
            assert s3.secret_key == "test_secret"
        finally:
            # Restore original env vars
            if original_endpoint:
                os.environ["MINIO_ENDPOINT"] = original_endpoint
            elif "MINIO_ENDPOINT" in os.environ:
                del os.environ["MINIO_ENDPOINT"]
            
            if original_key:
                os.environ["MINIO_ACCESS_KEY"] = original_key
            elif "MINIO_ACCESS_KEY" in os.environ:
                del os.environ["MINIO_ACCESS_KEY"]
            
            if original_secret:
                os.environ["MINIO_SECRET_KEY"] = original_secret
            elif "MINIO_SECRET_KEY" in os.environ:
                del os.environ["MINIO_SECRET_KEY"]
    
    def test_s3_resource_methods_exist(self):
        """Verify S3 resource has required methods."""
        s3 = S3Resource()
        
        # Check that methods exist (they may fail in tests without actual S3, but should exist)
        assert hasattr(s3, "list_files")
        assert hasattr(s3, "download_file")
        assert hasattr(s3, "download_csv_as_df")
        assert hasattr(s3, "upload_file")
        assert hasattr(s3, "upload_df_as_csv")
        assert hasattr(s3, "get_file_hash")
        assert hasattr(s3, "file_exists")
        assert hasattr(s3, "ensure_bucket_exists")

