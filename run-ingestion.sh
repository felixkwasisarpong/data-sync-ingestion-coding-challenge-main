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
  COUNT=$(docker compose exec -T postgres psql -U postgres -d ingestion -t -A -c "SELECT COUNT(*) FROM ingested_events;" 2>/dev/null || echo "0")

  if docker compose logs ingestion 2>&1 | grep -q "ingestion complete" 2>/dev/null; then
    echo ""
    echo "=============================================="
    echo "INGESTION COMPLETE!"
    echo "Total events: $COUNT"
    echo "=============================================="
    exit 0
  fi

  CONTAINER_ID=$(docker compose ps -a -q ingestion 2>/dev/null || true)

  if [ -z "$CONTAINER_ID" ]; then
    echo ""
    echo "Ingestion container was not found."
    exit 1
  fi

  STATUS=$(docker inspect -f '{{.State.Status}}' "$CONTAINER_ID")
  EXIT_CODE=$(docker inspect -f '{{.State.ExitCode}}' "$CONTAINER_ID")

  if [ "$STATUS" = "exited" ] || [ "$STATUS" = "dead" ]; then
    echo ""
    echo "Ingestion container stopped unexpectedly (status=$STATUS, exitCode=$EXIT_CODE)."
    echo "Last logs:"
    docker compose logs --tail 100 ingestion || true
    exit 1
  fi

  echo "[$(date '+%H:%M:%S')] Events ingested: $COUNT"
  sleep 5
done
