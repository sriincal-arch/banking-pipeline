"""
Dagster Definitions - Pipeline Configuration

This module defines the complete Dagster pipeline:
- Assets: All data pipeline assets (raw, structured, curated, access)
- Jobs: Layer-specific and full pipeline jobs
- Schedules: Automated pipeline runs (e.g., daily at 6 AM)
- Sensors: File-based triggers for new data
- Resources: DuckDB and S3/MinIO connections
"""

import os
from pathlib import Path

from dagster import (
    Definitions,
    define_asset_job,
    AssetSelection,
    ScheduleDefinition,
)
from dagster_dbt import DbtCliResource

from banking_pipeline.assets import (
    initialize_database,
    raw_accounts,
    raw_customers,
    run_dbt_structured,
    run_dbt_curated,
    run_dbt_access,
    export_account_summary,
)
from banking_pipeline.resources.duckdb_resource import DuckDBResource
from banking_pipeline.resources.s3_resource import S3Resource
from banking_pipeline.sensors.file_sensor import new_file_sensor

# Get dbt project directory
DBT_PROJECT_DIR = Path(__file__).parent.parent / "dbt"

# Note: The @dbt_assets approach has been replaced with wrapper assets (run_dbt_structured, run_dbt_curated, run_dbt_access)
# that provide better control over incremental processing, error handling, and skip-if-no-new-data logic.
# The wrapper assets call dbt via subprocess with proper environment setup and dependency management.


# Define the full pipeline job - runs all assets in dependency order
banking_pipeline_job = define_asset_job(
    name="banking_pipeline_job",
    selection=AssetSelection.all(),
    description="Full banking data pipeline from raw to access layer",
)

# Optional layer-specific jobs for testing/debugging individual layers
# These are not required but can be useful during development
# raw_layer_job = define_asset_job(
#     name="raw_layer_job",
#     selection=AssetSelection.groups("raw"),
#     description="Ingest raw data from MinIO",
# )
#
# access_layer_job = define_asset_job(
#     name="access_layer_job",
#     selection=AssetSelection.groups("access"),
#     description="Generate and export access layer reports",
# )

# Define schedules - automated pipeline execution
daily_pipeline_schedule = ScheduleDefinition(
    job=banking_pipeline_job,
    cron_schedule="0 6 * * *",  # Run at 6 AM daily (cron format: minute hour day month weekday)
    description="Daily full pipeline run",
)


# Configure resources - shared connections used by all assets
# Environment variables can override defaults (useful for Docker/local differences)
resources = {
    "duckdb": DuckDBResource(
        database_path=os.environ.get("DUCKDB_DATABASE", "data/banking.duckdb"),
    ),
    "s3": S3Resource(
        endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"),
        access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
    ),
    "dbt": DbtCliResource(
        project_dir=str(DBT_PROJECT_DIR),
        global_config_flags=["--no-use-colors"],
    ),
}


# Export Definitions - Dagster's entry point for the pipeline
# This is loaded by workspace.yaml and exposed to Dagster UI
defs = Definitions(
    assets=[
        # Raw layer assets
        initialize_database,
        raw_accounts,
        raw_customers,
        # dbt orchestration wrapper assets
        # Dependencies: raw -> structured -> curated -> access
        run_dbt_structured,
        run_dbt_curated,
        run_dbt_access,
        # Export asset
        export_account_summary,
    ],
    jobs=[
        banking_pipeline_job,
        # Layer-specific jobs commented out - use banking_pipeline_job or create ad-hoc jobs in UI
        # raw_layer_job,
        # access_layer_job,
    ],
    schedules=[
        daily_pipeline_schedule,
    ],
    sensors=[
        new_file_sensor,  # Triggers pipeline when new files arrive in landing bucket
    ],
    resources=resources,
)

