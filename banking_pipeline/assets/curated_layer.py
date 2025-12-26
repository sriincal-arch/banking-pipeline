"""
Curated Layer Assets - Dimensional Modeling and Business Logic

This module orchestrates dbt transformations for the curated layer:
- Builds dimensional models (dim_customer, dim_account, fact_customer_balances)
- Calculates interest rates based on balance tiers and loan status
- Applies incremental processing based on modified_date from structured layer
- Records process metadata for audit trail
- Skips dbt run if no new data detected (graceful handling)
"""

import json
import os
import subprocess
import uuid
from datetime import datetime
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    AssetIn,
    MaterializeResult,
    MetadataValue,
    asset,
)

from banking_pipeline.resources.duckdb_resource import DuckDBResource


DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


def _run_dbt_command(context: AssetExecutionContext, command: list[str], allow_failure: bool = False) -> dict:
    """
    Run a dbt command and return results.
    
    Sets up environment variables and executes dbt commands via subprocess.
    """
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
    
    if "DUCKDB_DATABASE" not in env:
        env["DUCKDB_DATABASE"] = str(DBT_PROJECT_DIR.parent / "data" / "banking.duckdb")

    context.log.info(f"Running dbt command: {' '.join(command)}")

    result = subprocess.run(
        command,
        cwd=str(DBT_PROJECT_DIR),
        env=env,
        capture_output=True,
        text=True,
    )

    if result.returncode != 0 and not allow_failure:
        context.log.error(f"dbt command failed: {result.stderr}")
        context.log.error(f"dbt stdout: {result.stdout}")
        raise Exception(f"dbt command failed: {result.stderr}\n{result.stdout}")

    context.log.info(f"dbt output: {result.stdout}")

    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


def _check_for_new_structured_data(duckdb: DuckDBResource, context: AssetExecutionContext) -> tuple[bool, int]:
    """
    Check if there's new data in structured tables that hasn't been processed yet.
    
    Compares structured savings accounts with curated fact table to detect unprocessed records.
    Used to skip dbt runs when no new data is available.
    
    Returns:
        tuple: (has_new_data: bool, new_records_count: int)
    """
    new_records = 0
    
    try:
        # Check for structured accounts not yet in fact table
        result = duckdb.fetch_all("""
            SELECT COUNT(*) FROM structured.str_accounts s
            WHERE s.account_type = 'savings'
            AND NOT EXISTS (
                SELECT 1 FROM curated.fact_customer_balances f 
                WHERE f.account_id = s.account_id
            )
        """)
        new_records = result[0][0] if result else 0
    except Exception as e:
        # If curated table doesn't exist, all structured data is new
        context.log.info(f"Curated tables may not exist yet: {e}")
        try:
            result = duckdb.fetch_all("SELECT COUNT(*) FROM structured.str_accounts WHERE account_type = 'savings'")
            new_records = result[0][0] if result else 0
        except:
            new_records = 0
    
    return new_records > 0, new_records


def _tables_exist(duckdb: DuckDBResource) -> bool:
    """
    Check if curated tables (dim_customer, dim_account, fact_customer_balances) exist.
    
    Used to determine if this is the first run (needs --full-refresh) or incremental.
    """
    try:
        result = duckdb.fetch_all("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'curated' 
            AND table_name IN ('dim_customer', 'dim_account', 'fact_customer_balances')
        """)
        return result[0][0] >= 3
    except Exception:
        return False


def _get_current_counts(duckdb: DuckDBResource) -> tuple[int, int, int]:
    """
    Get current row counts from curated tables.
    
    Returns:
        tuple: (dim_customer_count, dim_account_count, fact_count)
    """
    dim_customer_count = 0
    dim_account_count = 0
    fact_count = 0
    
    try:
        result = duckdb.fetch_all("SELECT COUNT(*) FROM curated.dim_customer")
        dim_customer_count = result[0][0] if result else 0
    except:
        pass
    try:
        result = duckdb.fetch_all("SELECT COUNT(*) FROM curated.dim_account")
        dim_account_count = result[0][0] if result else 0
    except:
        pass
    try:
        result = duckdb.fetch_all("SELECT COUNT(*) FROM curated.fact_customer_balances")
        fact_count = result[0][0] if result else 0
    except:
        pass
    
    return dim_customer_count, dim_account_count, fact_count


@asset(
    group_name="curated",
    deps=["run_dbt_structured"],
    description="Run dbt dimensional models for curated layer",
)
def run_dbt_curated(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Run dbt to build curated models incrementally.
    
    Processing logic:
    1. Check if curated tables exist (determines full-refresh vs incremental)
    2. Check for new structured data that hasn't been processed
    3. If no new data and tables exist: skip dbt run, return warning (graceful handling)
    4. If new data or first run: execute dbt run with appropriate flags
    5. Record process metadata for audit trail
    6. Run dbt tests for data quality validation
    
    The curated layer builds dimensional models:
    - dim_customer: Customer dimension table
    - dim_account: Account dimension table  
    - fact_customer_balances: Fact table with interest calculations
    """
    context.log.info("Checking for new data to process...")
    start_time = datetime.now()

    tables_exist = _tables_exist(duckdb)
    
    # Check if there's new data to process
    has_new_data, new_records = _check_for_new_structured_data(duckdb, context)
    
    if tables_exist and not has_new_data:
        # No new data - just warn and return success
        context.log.warning("⚠️ No new data to process - all structured records already exist in curated layer")
        context.log.warning("Skipping dbt run. This is expected when duplicate files are ingested.")
        
        dim_customer_count, dim_account_count, fact_count = _get_current_counts(duckdb)
        
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("⚠️ SKIPPED - No new data"),
                "dim_customer_rows": MetadataValue.int(dim_customer_count),
                "dim_account_rows": MetadataValue.int(dim_account_count),
                "fact_customer_balances_rows": MetadataValue.int(fact_count),
                "new_records_found": MetadataValue.int(0),
                "dbt_run": MetadataValue.bool(False),
            }
        )

    # There's new data to process
    context.log.info(f"Found {new_records} new records to process")
    context.log.info("Running dbt curated models...")

    # First run needs full-refresh, subsequent runs are incremental
    if not tables_exist:
        context.log.info("First run - using --full-refresh")
        dbt_command = ["dbt", "run", "--select", "curated.*", "--full-refresh"]
    else:
        context.log.info("Incremental run")
        dbt_command = ["dbt", "run", "--select", "curated.*"]

    # Run curated models
    result = _run_dbt_command(context, dbt_command)

    duration = (datetime.now() - start_time).total_seconds()
    dim_customer_count, dim_account_count, fact_count = _get_current_counts(duckdb)

    # Run dbt tests
    context.log.info("Running dbt tests for curated models...")
    test_result = _run_dbt_command(context, ["dbt", "test", "--select", "curated.*"], allow_failure=True)

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text("✓ Processed new data"),
            "dim_customer_rows": MetadataValue.int(dim_customer_count),
            "dim_account_rows": MetadataValue.int(dim_account_count),
            "fact_customer_balances_rows": MetadataValue.int(fact_count),
            "new_records_found": MetadataValue.int(new_records),
            "dbt_run": MetadataValue.bool(True),
            "duration_seconds": MetadataValue.float(duration),
            "tests_passed": MetadataValue.bool(test_result["returncode"] == 0),
        }
    )
