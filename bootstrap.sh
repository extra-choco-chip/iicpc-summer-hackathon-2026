#!/bin/sh
# bootstrap.sh — run this ONCE before docker compose up if you have Go installed locally
# If you don't have Go, skip this — the Dockerfiles handle it automatically via go mod tidy

set -e

SERVICES="api-gateway submission-service bot-orchestrator bot-worker telemetry-service scoring-service ws-gateway"

echo "Generating go.sum files for all services..."

for svc in $SERVICES; do
  dir="services/$svc"
  echo "  ▸ $svc"
  (cd "$dir" && go mod tidy)
done

echo ""
echo "Done. Now run: docker compose up --build"
