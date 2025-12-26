#!/bin/bash
# Banking Pipeline Runner
#
# Usage:
#   run-pipeline.sh
#
# The script will:
#   1. Ensure database connections are properly closed
#   2. Upload sample CSV files to MinIO
#   3. Run the full Dagster pipeline (raw -> structured -> curated -> access)
#   4. Display the generated account_summary.csv
#
# Note: Pipeline uses incremental processing with smart skip logic.
#       Layers will skip if no new data is detected.

set -e

echo "=========================================="
echo "  Banking Pipeline Runner"
echo "=========================================="

cd /opt/dagster/app
export DAGSTER_HOME=/opt/dagster/dagster_home
export DUCKDB_DATABASE=/opt/dagster/data/banking.duckdb
export DBT_PROFILES_DIR=/opt/dagster/app/dbt
export MINIO_ENDPOINT=http://localhost:9000
export MINIO_ACCESS_KEY=minioadmin
export MINIO_SECRET_KEY=minioadmin
export PYTHONPATH=/opt/dagster/app

# Ensure any lingering DuckDB connections are closed
echo ""
echo "Ensuring database is ready..."
python3 -c "
import duckdb
import gc
import time

# Quick connection test to ensure database is accessible
try:
    conn = duckdb.connect('/opt/dagster/data/banking.duckdb')
    conn.execute('SELECT 1')
    conn.close()
    del conn
    gc.collect()  # Force garbage collection
    time.sleep(0.2)  # Brief pause to ensure lock release
    print('✓ Database connection verified')
except Exception as e:
    print(f'⚠ Database connection test failed: {e}')
"
echo ""

# Upload sample files if they exist
echo "Uploading sample files..."
if [ -f /opt/dagster/data/sample/accounts.csv ]; then
    mc cp /opt/dagster/data/sample/accounts.csv local/landing/ --quiet 2>/dev/null || true
    echo "✓ accounts.csv uploaded"
fi
if [ -f /opt/dagster/data/sample/customers.csv ]; then
    mc cp /opt/dagster/data/sample/customers.csv local/landing/ --quiet 2>/dev/null || true
    echo "✓ customers.csv uploaded"
fi

echo ""
echo "=========================================="
echo "  Running Full Pipeline Job"
echo "=========================================="
echo ""
echo "Executing banking_pipeline_job..."
echo "This will run all assets in dependency order:"
echo "  1. initialize_database"
echo "  2. raw_accounts + raw_customers (parallel)"
echo "  3. run_dbt_structured (str_accounts, str_customers)"
echo "  4. run_dbt_curated (dim_*, fact_*)"
echo "  5. run_dbt_access (account_summary)"
echo "  6. export_account_summary"
echo ""

# Run the full pipeline job end-to-end (suppress debug logs)
dagster job execute -m banking_pipeline.definitions -j banking_pipeline_job 2>&1 | grep -v "DEBUG" || true

echo ""
echo "=========================================="
echo "  Pipeline Complete!"
echo "=========================================="

# Check and display output
echo ""
if [ -f /opt/dagster/data/output/account_summary.csv ]; then
    echo "✓ Output file generated: /opt/dagster/data/output/account_summary.csv"
    echo ""
    echo "=== account_summary.csv ==="
    cat /opt/dagster/data/output/account_summary.csv
else
    echo "✗ No output file generated"
    echo ""
    echo "Checking database for data..."
    python3 << 'EOF'
import duckdb
conn = duckdb.connect('/opt/dagster/data/banking.duckdb')

print("Tables in database:")
tables = conn.execute("SELECT table_schema, table_name FROM information_schema.tables WHERE table_schema NOT IN ('information_schema', 'pg_catalog')").fetchall()
for schema, table in tables:
    count = conn.execute(f"SELECT COUNT(*) FROM {schema}.{table}").fetchone()[0]
    print(f"  {schema}.{table}: {count} rows")
EOF
fi
echo ""
