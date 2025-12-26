"""
Access Layer Assets - Final Report Generation and Export

This module handles the access layer of the medallion architecture:
- Builds final summary table (account_summary) via dbt (always full refresh)
- Exports data to CSV in MinIO access bucket
- Also writes CSV to local filesystem for easy access
- Calculates summary statistics (totals, averages)
"""

import os
import subprocess
from pathlib import Path

from dagster import (
    AssetExecutionContext,
    MaterializeResult,
    MetadataValue,
    asset,
)

from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource


DBT_PROJECT_DIR = Path(__file__).parent.parent.parent / "dbt"


def _run_dbt_command(context: AssetExecutionContext, command: list[str]) -> dict:
    """
    Run a dbt command and return results.
    
    Sets up environment variables and executes dbt commands via subprocess.
    """
    env = os.environ.copy()
    env["DBT_PROFILES_DIR"] = str(DBT_PROJECT_DIR)
    
    # Ensure DUCKDB_DATABASE is set for dbt-duckdb
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

    if result.returncode != 0:
        context.log.error(f"dbt command failed: {result.stderr}")
        context.log.error(f"dbt stdout: {result.stdout}")
        raise Exception(f"dbt command failed: {result.stderr}\n{result.stdout}")

    context.log.info(f"dbt output: {result.stdout}")

    return {
        "returncode": result.returncode,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }


@asset(
    group_name="access",
    deps=["run_dbt_curated"],
    description="Run dbt access layer model to generate account summary",
)
def run_dbt_access(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
) -> MaterializeResult:
    """
    Run dbt to build access layer model (account_summary).
    
    The access layer is always a full refresh (table materialization) to ensure
    the final report reflects the current state of all upstream data.
    """
    context.log.info("Running dbt access layer models...")

    # Run access models (table materialization - always full refresh)
    result = _run_dbt_command(
        context,
        ["dbt", "run", "--select", "access.*"],
    )

    # Get row count from access table
    summary_count = 0
    try:
        summary_result = duckdb.fetch_all("SELECT COUNT(*) FROM access.account_summary")
        summary_count = summary_result[0][0] if summary_result else 0
    except Exception:
        context.log.warning("Could not get account_summary count")

    # Run dbt tests for access models
    context.log.info("Running dbt tests for access models...")
    test_result = _run_dbt_command(
        context,
        ["dbt", "test", "--select", "access.*"],
    )

    return MaterializeResult(
        metadata={
            "account_summary_rows": MetadataValue.int(summary_count),
            "tests_passed": MetadataValue.bool(test_result["returncode"] == 0),
        }
    )


@asset(
    group_name="access",
    deps=["run_dbt_access"],  # Dependency on wrapper asset that creates account_summary table
    description="Export account_summary.csv to MinIO access bucket",
)
def export_account_summary(
    context: AssetExecutionContext,
    duckdb: DuckDBResource,
    s3: S3Resource,
) -> MaterializeResult:
    """
    Export the account_summary table to CSV in MinIO and local filesystem.
    
    Outputs:
    - MinIO: s3://access/account_summary.csv
    - Local: /opt/dagster/data/output/account_summary.csv
    
    Also calculates summary statistics (totals, averages) for metadata.
    """
    context.log.info("Exporting account_summary to CSV...")

    # Fetch the account summary data
    df = duckdb.fetch_df("""
        SELECT 
            customer_id,
            account_id,
            original_balance,
            interest_rate,
            annual_interest,
            new_balance
        FROM access.account_summary
        ORDER BY customer_id, account_id
    """)

    row_count = len(df)

    if row_count == 0:
        context.log.warning("No data in account_summary to export")
        return MaterializeResult(
            metadata={"status": MetadataValue.text("No data to export")}
        )

    # Upload to MinIO as account_summary.csv (single file, no timestamped backups)
    filename = "account_summary.csv"
    s3.upload_df_as_csv("access", filename, df)

    # Also write to local filesystem for easy access
    local_output_dir = Path("/opt/dagster/data/output")
    local_output_dir.mkdir(parents=True, exist_ok=True)
    local_file_path = local_output_dir / filename
    df.to_csv(local_file_path, index=False)

    context.log.info(f"Successfully exported {row_count} rows to access/{filename}")
    context.log.info(f"Local copy saved to {local_file_path}")

    # Calculate summary statistics
    total_original_balance = float(df["original_balance"].sum())
    total_annual_interest = float(df["annual_interest"].sum())
    total_new_balance = float(df["new_balance"].sum())
    avg_interest_rate = float(df["interest_rate"].mean())

    return MaterializeResult(
        metadata={
            "rows_exported": MetadataValue.int(row_count),
            "s3_path": MetadataValue.text(f"access/{filename}"),
            "local_path": MetadataValue.text(str(local_file_path)),
            "total_original_balance": MetadataValue.float(total_original_balance),
            "total_annual_interest": MetadataValue.float(total_annual_interest),
            "total_new_balance": MetadataValue.float(total_new_balance),
            "avg_interest_rate": MetadataValue.float(avg_interest_rate),
        }
    )

