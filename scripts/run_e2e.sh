#!/bin/bash
set -e

echo "=========================================="
echo "  Banking Pipeline - End-to-End Run"
echo "=========================================="
echo ""

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker daemon is not running!"
    echo "Please start Docker Desktop and try again."
    exit 1
fi

echo "Step 1: Building Docker container..."
docker compose build

echo ""
echo "Step 2: Starting services..."
docker compose up -d

echo ""
echo "Waiting for services to be ready..."
sleep 10

echo ""
echo "Step 3: Running the pipeline..."
docker exec -it banking-pipeline run-pipeline

echo ""
echo "=========================================="
echo "  Pipeline Execution Complete!"
echo "=========================================="
echo ""
echo "Access Points:"
echo "  • Dagster UI:     http://localhost:3000"
echo "  • MinIO Console:  http://localhost:9001"
echo ""
echo "To view logs:"
echo "  docker compose logs -f"
echo ""
echo "To stop services:"
echo "  docker compose down"
echo ""

