"""
Tests for asset materialization.

Verifies that assets materialize successfully with mocked resources.
"""

from unittest.mock import Mock, patch

import pytest  # type: ignore[import-untyped]

from banking_pipeline.assets import (
    export_account_summary,
    initialize_database,
    raw_accounts,
    raw_customers,
    run_dbt_access,
    run_dbt_curated,
    run_dbt_structured,
)
from dagster import materialize


class TestAssetMaterialization:
    """Test that assets materialize successfully."""
    
    def test_initialize_database_materialization(self, mock_duckdb):
        """Verify initialize_database asset materializes."""
        result = materialize(
            [initialize_database],
            resources={"duckdb": mock_duckdb},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "initialize_database"
        
        # Verify schemas were created
        schemas = mock_duckdb.fetch_all("""
            SELECT schema_name 
            FROM information_schema.schemata 
            WHERE schema_name IN ('raw', 'structured', 'curated', 'access')
            ORDER BY schema_name
        """)
        assert len(schemas) == 4
    
    def test_raw_accounts_materialization(self, initialized_duckdb, mock_s3):
        """Verify raw_accounts asset materializes."""
        result = materialize(
            [raw_accounts],
            resources={"duckdb": initialized_duckdb, "s3": mock_s3},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "raw_accounts"
        
        # Verify metadata contains expected fields
        metadata = result.asset_materializations[0].metadata
        assert "row_count" in metadata or "status" in metadata
    
    def test_raw_customers_materialization(self, initialized_duckdb, mock_s3):
        """Verify raw_customers asset materializes."""
        result = materialize(
            [raw_customers],
            resources={"duckdb": initialized_duckdb, "s3": mock_s3},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "raw_customers"
    
    @patch("banking_pipeline.assets.structured_layer.subprocess.run")
    def test_run_dbt_structured_materialization(self, mock_subprocess, initialized_duckdb, sample_raw_data):
        """Verify run_dbt_structured asset materializes."""
        # Mock successful dbt run
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="Successfully ran 2 models",
            stderr="",
        )
        
        result = materialize(
            [run_dbt_structured],
            resources={"duckdb": sample_raw_data},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "run_dbt_structured"
    
    @patch("banking_pipeline.assets.curated_layer.subprocess.run")
    def test_run_dbt_curated_materialization(self, mock_subprocess, initialized_duckdb):
        """Verify run_dbt_curated asset materializes."""
        # Mock successful dbt run
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="Successfully ran 3 models",
            stderr="",
        )
        
        # Create structured tables first (prerequisite)
        with initialized_duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS structured.str_accounts AS
                SELECT 
                    'A001' as account_id,
                    'savings' as account_type,
                    10000.0 as original_balance,
                    '101' as customer_id,
                    CURRENT_TIMESTAMP as created_date,
                    CURRENT_TIMESTAMP as modified_date,
                    '{}'::JSON as metadata_json
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS structured.str_customers AS
                SELECT 
                    '101' as customer_id,
                    'Alice Smith' as name,
                    true as has_loan,
                    CURRENT_TIMESTAMP as created_date,
                    CURRENT_TIMESTAMP as modified_date,
                    '{}'::JSON as metadata_json
            """)
        
        result = materialize(
            [run_dbt_curated],
            resources={"duckdb": initialized_duckdb},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "run_dbt_curated"
    
    @patch("banking_pipeline.assets.access_layer.subprocess.run")
    def test_run_dbt_access_materialization(self, mock_subprocess, initialized_duckdb):
        """Verify run_dbt_access asset materializes."""
        # Mock successful dbt run
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="Successfully ran 1 model",
            stderr="",
        )
        
        # Create curated tables first (prerequisite)
        with initialized_duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS curated.fact_customer_balances AS
                SELECT 
                    '101' as customer_id,
                    'A001' as account_id,
                    10000.0 as original_balance,
                    0.015 as interest_rate,
                    150.0 as annual_interest,
                    10150.0 as new_balance
            """)
        
        result = materialize(
            [run_dbt_access],
            resources={"duckdb": initialized_duckdb},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "run_dbt_access"
    
    @patch("banking_pipeline.assets.access_layer.subprocess.run")
    def test_export_account_summary_materialization(self, mock_subprocess, initialized_duckdb, mock_s3):
        """Verify export_account_summary asset materializes."""
        # Mock successful dbt run
        mock_subprocess.return_value = Mock(
            returncode=0,
            stdout="Successfully ran 1 model",
            stderr="",
        )
        
        # Create account_summary table
        with initialized_duckdb.get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS access.account_summary AS
                SELECT 
                    '101' as customer_id,
                    'A001' as account_id,
                    10000.0 as original_balance,
                    0.015 as interest_rate,
                    150.0 as annual_interest,
                    10150.0 as new_balance
            """)
        
        result = materialize(
            [export_account_summary],
            resources={"duckdb": initialized_duckdb, "s3": mock_s3},
        )
        
        assert result.success
        assert len(result.asset_materializations) == 1
        assert result.asset_materializations[0].asset_key.to_user_string() == "export_account_summary"
        
        # Verify S3 upload was called
        mock_s3.upload_df_as_csv.assert_called_once()

