#!/bin/sh
set -eu

echo "=============================================="
echo "DataSync Ingestion - Running Solution"
echo "=============================================="

echo "Starting services..."
docker compose up -d --build

echo ""
echo "Waiting for services to initialize..."
sleep 5

echo ""
echo "Monitoring ingestion progress..."
echo "(Press Ctrl+C to stop monitoring)"
echo "=============================================="

while true; do
  if docker logs assignment-ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
    echo ""
    echo "=============================================="
    echo "INGESTION COMPLETE!"
    echo "=============================================="
    exit 0
  fi

  if ! docker ps --format '{{.Names}}' | grep -q '^assignment-ingestion$'; then
    echo ""
    echo "Ingestion container is not running. Check logs with: docker logs assignment-ingestion"
    exit 1
  fi

  echo "[$(date '+%H:%M:%S')] ingestion in progress"
  sleep 5
done
