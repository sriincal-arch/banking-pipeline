"""File sensor for detecting new files in MinIO landing bucket."""

from dagster import (
    RunRequest,
    SensorEvaluationContext,
    sensor,
    SkipReason,
)

from banking_pipeline.resources.s3_resource import S3Resource


@sensor(
    job_name="banking_pipeline_job",
    minimum_interval_seconds=60,
    description="Detects new CSV files in the MinIO landing bucket",
)
def new_file_sensor(context: SensorEvaluationContext, s3: S3Resource):
    """
    Sensor that monitors the MinIO landing bucket for new files.
    
    Triggers the banking_pipeline_job when new accounts.csv or customers.csv
    files are detected.
    """
    bucket = "landing"
    
    try:
        files = s3.list_files(bucket)
    except Exception as e:
        context.log.warning(f"Could not list files in {bucket}: {e}")
        yield SkipReason(f"Could not access MinIO: {e}")
        return
    
    if not files:
        yield SkipReason("No files found in landing bucket")
        return
    
    # Check for accounts and customers files
    accounts_file = None
    customers_file = None
    
    for f in files:
        key = f["key"].lower()
        if "accounts" in key and key.endswith(".csv"):
            if accounts_file is None or f["last_modified"] > accounts_file["last_modified"]:
                accounts_file = f
        elif "customers" in key and key.endswith(".csv"):
            if customers_file is None or f["last_modified"] > customers_file["last_modified"]:
                customers_file = f
    
    if not accounts_file or not customers_file:
        missing = []
        if not accounts_file:
            missing.append("accounts.csv")
        if not customers_file:
            missing.append("customers.csv")
        yield SkipReason(f"Missing required files: {', '.join(missing)}")
        return
    
    # Create a cursor key based on file modification times
    cursor_key = f"{accounts_file['last_modified']}_{customers_file['last_modified']}"
    
    # Check if we've already processed these files
    last_cursor = context.cursor
    if last_cursor == cursor_key:
        yield SkipReason("No new files since last run")
        return
    
    # New files detected, trigger the pipeline
    context.update_cursor(cursor_key)
    
    yield RunRequest(
        run_key=cursor_key,
        run_config={},
        tags={
            "accounts_file": accounts_file["key"],
            "customers_file": customers_file["key"],
        },
    )

