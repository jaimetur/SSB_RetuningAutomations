#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-webapp.yml"
CONTAINER_NAME="ssb_webapp"

cd "$PROJECT_DIR"

# Ensure bind mount exists
mkdir -p "$SCRIPT_DIR/data"

# Stop existing container if running
if docker ps -a --format '{{.Names}}' | grep -qx "$CONTAINER_NAME"; then
  echo "Stopping existing container: $CONTAINER_NAME"
  docker compose -f "$COMPOSE_FILE" down
fi

echo "Building and starting..."
docker compose -f "$COMPOSE_FILE" up --build -d

echo "Tailing logs (Ctrl+C to exit)..."
docker logs -f "$CONTAINER_NAME"
