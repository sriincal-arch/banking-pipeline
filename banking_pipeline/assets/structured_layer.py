"""
Structured Layer Assets - Data Cleansing and Standardization

This module orchestrates dbt transformations for the structured layer:
- Cleanses and standardizes raw data (trim, normalize casing, type conversion)
- Deduplicates records by primary key, keeping latest by ingestion timestamp
- Applies upsert logic (merge strategy) for incremental processing
- Skips dbt run if no new data detected (graceful handling of duplicate files)
"""

import os
import subprocess
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
    
    Sets up environment variables (DBT_PROFILES_DIR, DUCKDB_DATABASE) and executes
    dbt commands via subprocess. Logs output and handles errors.
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


def _check_for_new_raw_data(duckdb: DuckDBResource, context: AssetExecutionContext) -> tuple[bool, int, int]:
    """
    Check if there's new data in raw tables that hasn't been processed yet.
    
    Compares raw tables with structured tables to detect unprocessed records.
    Used to skip dbt runs when no new data is available (idempotency).
    
    Returns:
        tuple: (has_new_data: bool, new_accounts_count: int, new_customers_count: int)
    """
    new_accounts = 0
    new_customers = 0
    
    try:
        # Check for new accounts data
        result = duckdb.fetch_all("""
            SELECT COUNT(*) FROM raw.raw_accounts r
            WHERE NOT EXISTS (
                SELECT 1 FROM structured.str_accounts s 
                WHERE s.account_id = trim(cast(r.AccountID as varchar))
            )
        """)
        new_accounts = result[0][0] if result else 0
    except Exception as e:
        # If structured table doesn't exist, all raw data is new
        context.log.info(f"str_accounts table may not exist yet: {e}")
        try:
            result = duckdb.fetch_all("SELECT COUNT(*) FROM raw.raw_accounts")
            new_accounts = result[0][0] if result else 0
        except:
            new_accounts = 0
    
    try:
        # Check for new customers data
        result = duckdb.fetch_all("""
            SELECT COUNT(*) FROM raw.raw_customers r
            WHERE NOT EXISTS (
                SELECT 1 FROM structured.str_customers s 
                WHERE s.customer_id = trim(cast(r.CustomerID as varchar))
            )
        """)
        new_customers = result[0][0] if result else 0
    except Exception as e:
        # If structured table doesn't exist, all raw data is new
        context.log.info(f"str_customers table may not exist yet: {e}")
        try:
            result = duckdb.fetch_all("SELECT COUNT(*) FROM raw.raw_customers")
            new_customers = result[0][0] if result else 0
        except:
            new_customers = 0
    
    has_new_data = (new_accounts > 0) or (new_customers > 0)
    return has_new_data, new_accounts, new_customers


def _tables_exist(duckdb: DuckDBResource) -> bool:
    """
    Check if structured tables (str_accounts, str_customers) already exist.
    
    Used to determine if this is the first run (needs --full-refresh) or incremental.
    """
    try:
        result = duckdb.fetch_all("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'structured' 
            AND table_name IN ('str_accounts', 'str_customers')
        """)
        return result[0][0] == 2
    except Exception:
        return False


@asset(
    group_name="structured",
    deps=["raw_accounts", "raw_customers"],
    description="Run dbt structured models to transform raw data into structured layer",
)
def run_dbt_structured(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Run dbt to build structured models (str_accounts, str_customers) incrementally.
    
    Processing logic:
    1. Check if structured tables exist (determines full-refresh vs incremental)
    2. Check for new raw data that hasn't been processed
    3. If no new data and tables exist: skip dbt run, return warning (graceful handling)
    4. If new data or first run: execute dbt run with appropriate flags
    5. Run dbt tests for data quality validation
    
    The structured layer uses merge strategy (upsert) to deduplicate by primary key,
    keeping the latest record based on ingestion timestamp.
    """
    context.log.info("Checking for new data to process...")

    tables_exist = _tables_exist(duckdb)
    
    # Check if there's new data to process
    has_new_data, new_accounts, new_customers = _check_for_new_raw_data(duckdb, context)
    
    if tables_exist and not has_new_data:
        # No new data - gracefully skip dbt run instead of failing
        # This allows the pipeline to continue even when duplicate files are ingested
        context.log.warning("⚠️ No new data to process - all raw records already exist in structured layer")
        context.log.warning("Skipping dbt run. This is expected when duplicate files are ingested.")
        
        # Get current counts
        accounts_count = 0
        customers_count = 0
        try:
            accounts_result = duckdb.fetch_all("SELECT COUNT(*) FROM structured.str_accounts")
            accounts_count = accounts_result[0][0] if accounts_result else 0
            customers_result = duckdb.fetch_all("SELECT COUNT(*) FROM structured.str_customers")
            customers_count = customers_result[0][0] if customers_result else 0
        except:
            pass
        
        return MaterializeResult(
            metadata={
                "status": MetadataValue.text("⚠️ SKIPPED - No new data"),
                "str_accounts_rows": MetadataValue.int(accounts_count),
                "str_customers_rows": MetadataValue.int(customers_count),
                "new_accounts_found": MetadataValue.int(0),
                "new_customers_found": MetadataValue.int(0),
                "dbt_run": MetadataValue.bool(False),
            }
        )

    # There's new data to process
    context.log.info(f"Found new data: {new_accounts} new accounts, {new_customers} new customers")
    context.log.info("Running dbt structured models...")

    # Install dbt packages if needed
    _run_dbt_command(context, ["dbt", "deps"])

    # First run needs full-refresh to create tables, subsequent runs are incremental
    # dbt incremental models use merge strategy to upsert records
    if not tables_exist:
        context.log.info("First run - using --full-refresh")
        dbt_command = ["dbt", "run", "--select", "structured.*", "--full-refresh"]
    else:
        context.log.info("Incremental run - dbt will merge new records")
        dbt_command = ["dbt", "run", "--select", "structured.*"]

    # Run structured models
    result = _run_dbt_command(context, dbt_command)

    # Get row counts
    accounts_count = 0
    customers_count = 0
    try:
        accounts_result = duckdb.fetch_all("SELECT COUNT(*) FROM structured.str_accounts")
        accounts_count = accounts_result[0][0] if accounts_result else 0
    except:
        pass
    try:
        customers_result = duckdb.fetch_all("SELECT COUNT(*) FROM structured.str_customers")
        customers_count = customers_result[0][0] if customers_result else 0
    except:
        pass

    # Run dbt tests
    context.log.info("Running dbt tests for structured models...")
    test_result = _run_dbt_command(context, ["dbt", "test", "--select", "structured.*"], allow_failure=True)

    return MaterializeResult(
        metadata={
            "status": MetadataValue.text("✓ Processed new data"),
            "str_accounts_rows": MetadataValue.int(accounts_count),
            "str_customers_rows": MetadataValue.int(customers_count),
            "new_accounts_found": MetadataValue.int(new_accounts),
            "new_customers_found": MetadataValue.int(new_customers),
            "dbt_run": MetadataValue.bool(True),
            "tests_passed": MetadataValue.bool(test_result["returncode"] == 0),
        }
    )
