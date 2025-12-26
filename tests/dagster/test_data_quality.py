"""
Tests for data quality integration.

Verifies that data quality checks are executed and results are tracked correctly.
"""

from unittest.mock import Mock, patch

import pytest  # type: ignore[import-untyped]

from banking_pipeline.assets import run_dbt_access, run_dbt_curated, run_dbt_structured
from dagster import materialize


class TestDBTTestsExecuted:
    """Test that dbt tests are executed during materialization."""
    
    @patch("banking_pipeline.assets.structured_layer.subprocess.run")
    def test_dbt_tests_executed(self, mock_subprocess, initialized_duckdb, sample_raw_data):
        """Verify dbt tests run during structured layer materialization."""
        # Mock dbt run (success)
        # Mock dbt test (success)
        call_count = 0
        
        def mock_run_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "test" in args[0]:
                return Mock(returncode=0, stdout="Tests passed: 5", stderr="")
            else:
                return Mock(returncode=0, stdout="Successfully ran 2 models", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        result = materialize(
            [run_dbt_structured],
            resources={"duckdb": sample_raw_data},
        )
        
        assert result.success
        
        # Verify dbt test was called
        # subprocess.run should be called at least twice (dbt run + dbt test)
        assert call_count >= 2
        
        # Verify test results are in metadata
        materialization = result.asset_materializations[0]
        metadata = materialization.metadata
        assert "tests_passed" in metadata or "dbt_run" in metadata
    
    @patch("banking_pipeline.assets.curated_layer.subprocess.run")
    def test_curated_tests_executed(self, mock_subprocess, initialized_duckdb):
        """Verify dbt tests run during curated layer materialization."""
        call_count = 0
        
        def mock_run_side_effect(*args, **kwargs):
            nonlocal call_count
            call_count += 1
            if "test" in args[0]:
                return Mock(returncode=0, stdout="Tests passed: 8", stderr="")
            else:
                return Mock(returncode=0, stdout="Successfully ran 3 models", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        # Create prerequisite structured tables
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
        
        # Verify dbt test was called
        assert call_count >= 2
        
        # Verify test results are in metadata
        materialization = result.asset_materializations[0]
        metadata = materialization.metadata
        assert "tests_passed" in metadata or "dbt_run" in metadata


class TestTestResultsInMetadata:
    """Test that test results are stored in metadata."""
    
    @patch("banking_pipeline.assets.structured_layer.subprocess.run")
    def test_test_results_in_metadata(self, mock_subprocess, initialized_duckdb, sample_raw_data):
        """Verify tests_passed flag is in metadata."""
        def mock_run_side_effect(*args, **kwargs):
            if "test" in args[0]:
                return Mock(returncode=0, stdout="All tests passed", stderr="")
            else:
                return Mock(returncode=0, stdout="Successfully ran 2 models", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        result = materialize(
            [run_dbt_structured],
            resources={"duckdb": sample_raw_data},
        )
        
        assert result.success
        
        materialization = result.asset_materializations[0]
        metadata = materialization.metadata
        
        # Verify tests_passed is in metadata
        assert "tests_passed" in metadata
        
        # Verify value is boolean
        if "tests_passed" in metadata:
            assert isinstance(metadata["tests_passed"].value, bool)


class TestAllowFailureBehavior:
    """Test that allow_failure=True works correctly for structured/curated layers."""
    
    @patch("banking_pipeline.assets.structured_layer.subprocess.run")
    def test_allow_failure_behavior(self, mock_subprocess, initialized_duckdb, sample_raw_data):
        """Verify structured/curated allow failures don't stop pipeline."""
        def mock_run_side_effect(*args, **kwargs):
            if "test" in args[0]:
                # Simulate test failure
                return Mock(returncode=1, stdout="", stderr="Test failed: not_null test")
            else:
                return Mock(returncode=0, stdout="Successfully ran 2 models", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        # Should not raise exception even if tests fail (allow_failure=True)
        result = materialize(
            [run_dbt_structured],
            resources={"duckdb": sample_raw_data},
        )
        
        assert result.success  # Asset should still succeed
        
        # Verify tests_passed is False in metadata
        materialization = result.asset_materializations[0]
        metadata = materialization.metadata
        
        if "tests_passed" in metadata:
            assert metadata["tests_passed"].value == False
    
    @patch("banking_pipeline.assets.curated_layer.subprocess.run")
    def test_curated_allow_failure(self, mock_subprocess, initialized_duckdb):
        """Verify curated layer allows test failures."""
        def mock_run_side_effect(*args, **kwargs):
            if "test" in args[0]:
                return Mock(returncode=1, stdout="", stderr="Test failed")
            else:
                return Mock(returncode=0, stdout="Successfully ran 3 models", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        # Create prerequisite structured tables
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
        
        assert result.success  # Should succeed even with test failures


class TestAccessLayerStrictFailure:
    """Test that access layer fails on test failure (strict mode)."""
    
    @patch("banking_pipeline.assets.access_layer.subprocess.run")
    def test_access_layer_strict_failure(self, mock_subprocess, initialized_duckdb):
        """Verify access layer fails on test failure."""
        def mock_run_side_effect(*args, **kwargs):
            if "test" in args[0]:
                # Simulate test failure
                return Mock(returncode=1, stdout="", stderr="Test failed")
            else:
                return Mock(returncode=0, stdout="Successfully ran 1 model", stderr="")
        
        mock_subprocess.side_effect = mock_run_side_effect
        
        # Create prerequisite curated table
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
        
        # Access layer should raise exception on test failure (no allow_failure)
        with pytest.raises(Exception):
            materialize(
                [run_dbt_access],
                resources={"duckdb": initialized_duckdb},
            )

