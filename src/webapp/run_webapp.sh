#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-webapp.yml"
CONTAINER_NAME="${WEBAPP_CONTAINER_NAME:-ssb_webapp}"

cd "$PROJECT_DIR"

# Ensure bind mount exists
mkdir -p "$SCRIPT_DIR/data"

# Stop compose stack for this file (best-effort)
if docker compose -f "$COMPOSE_FILE" ps -q >/dev/null 2>&1; then
  docker compose -f "$COMPOSE_FILE" down --remove-orphans || true
fi

# Remove any leftover container that keeps the name reserved (even if it was created outside this compose project)
if docker ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
  echo "Removing stale container with reserved name: $CONTAINER_NAME"
  docker rm -f "$CONTAINER_NAME" >/dev/null
fi

echo "Building and starting..."
docker compose -f "$COMPOSE_FILE" up --build -d

echo "Web frontend should be available at: http://localhost:7979/login"
echo "Tailing logs (Ctrl+C to exit)..."
docker logs -f "$CONTAINER_NAME"
