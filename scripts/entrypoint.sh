#!/bin/bash

echo "=========================================="
echo "  Banking Pipeline - Starting Services"
echo "=========================================="

# Start supervisord (runs MinIO, Dagster webserver, Dagster daemon)
echo "Starting services..."
/usr/bin/supervisord -c /etc/supervisor/supervisord.conf &
SUPERVISOR_PID=$!

# Wait for MinIO to be ready
echo "Waiting for MinIO..."
for i in {1..60}; do
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "✓ MinIO is ready"
        break
    fi
    sleep 1
done

# Configure MinIO buckets
echo "Setting up MinIO buckets..."
mc alias set local http://localhost:9000 minioadmin minioadmin --quiet 2>/dev/null
mc mb local/landing --ignore-existing --quiet 2>/dev/null
mc mb local/raw --ignore-existing --quiet 2>/dev/null
mc mb local/access --ignore-existing --quiet 2>/dev/null

# Generate dbt manifest.json (required for @dbt_assets decorator)
echo "Generating dbt manifest..."
cd /opt/dagster/app/dbt
export DBT_PROFILES_DIR=/opt/dagster/app/dbt
export DUCKDB_DATABASE=/opt/dagster/data/banking.duckdb
dbt parse --quiet 2>/dev/null || dbt compile --quiet 2>/dev/null || echo "⚠️  Could not generate dbt manifest (will be generated on first dbt run)"
cd /opt/dagster/app

# Upload sample files if they exist
if [ -f /opt/dagster/data/sample/accounts.csv ]; then
    mc cp /opt/dagster/data/sample/accounts.csv local/landing/ --quiet 2>/dev/null
    echo "✓ Uploaded accounts.csv to landing bucket"
fi
if [ -f /opt/dagster/data/sample/customers.csv ]; then
    mc cp /opt/dagster/data/sample/customers.csv local/landing/ --quiet 2>/dev/null
    echo "✓ Uploaded customers.csv to landing bucket"
fi

# Wait for Dagster
echo "Waiting for Dagster..."
for i in {1..60}; do
    if curl -s http://localhost:3000/health > /dev/null 2>&1; then
        echo "✓ Dagster is ready"
        break
    fi
    sleep 1
done

echo ""
echo "=========================================="
echo "  All Services Ready!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  • Dagster UI:     http://localhost:3000"
echo "  • MinIO Console:  http://localhost:9001"
echo "  • MinIO API:      http://localhost:9000"
echo ""
echo "To run the pipeline:"
echo "  docker exec -it banking-pipeline run-pipeline"
echo ""
echo "To connect to the container:"
echo "  docker exec -it banking-pipeline bash"
echo ""
echo "Container is running. Press Ctrl+C to stop."

# Keep container running
wait $SUPERVISOR_PID
