#!/bin/bash
# Initialize MinIO with sample data

set -e

MINIO_ENDPOINT=${MINIO_ENDPOINT:-"http://localhost:9000"}
MINIO_ACCESS_KEY=${MINIO_ACCESS_KEY:-"minioadmin"}
MINIO_SECRET_KEY=${MINIO_SECRET_KEY:-"minioadmin"}

echo "Configuring MinIO client..."
mc alias set myminio $MINIO_ENDPOINT $MINIO_ACCESS_KEY $MINIO_SECRET_KEY

echo "Creating buckets..."
mc mb myminio/landing --ignore-existing
mc mb myminio/raw --ignore-existing
mc mb myminio/access --ignore-existing

echo "Setting bucket policies..."
mc anonymous set download myminio/access

# Check if sample data exists
if [ -f "data/sample/accounts.csv" ] && [ -f "data/sample/customers.csv" ]; then
    echo "Uploading sample data to landing bucket..."
    mc cp data/sample/accounts.csv myminio/landing/
    mc cp data/sample/customers.csv myminio/landing/
    echo "Sample data uploaded successfully!"
else
    echo "Sample data not found. Generate it first with:"
    echo "  python scripts/generate_sample_data.py"
fi

echo "MinIO initialization complete!"
echo ""
echo "Buckets:"
mc ls myminio/

echo ""
echo "Landing bucket contents:"
mc ls myminio/landing/


