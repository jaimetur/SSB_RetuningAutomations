#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-dev.yml"

# Timezone
TZ="Europe/Madrid"

# Container Name
CONTAINER_NAME="ssb-retuning-automations-dev"

# Port
PORT=7979

# Change if you have other folder's deployment
APP_DIR="$SCRIPT_DIR/../.."
DATA_DIR="$APP_DIR/data"

export TZ
export CONTAINER_NAME
export PORT
export APP_DIR
export DATA_DIR

mkdir -p "$DATA_DIR"

echo "Stopping any running container publishing port $PORT (no removal)..."
ids="$(docker ps -q --filter "publish=$PORT" || true)"
if [[ -n "$ids" ]]; then
  # shellcheck disable=SC2086
  docker stop $ids >/dev/null || true
fi

echo "Removing only the compose container name if it exists: $CONTAINER_NAME"
if docker ps -a --format '{{.Names}}' | grep -Fxq "$CONTAINER_NAME"; then
  docker rm -f "$CONTAINER_NAME" >/dev/null || true
fi

echo "Starting compose (build + remove orphans)..."
docker compose -f "$COMPOSE_FILE" up --build -d --remove-orphans

echo "Logs:"
docker compose -f "$COMPOSE_FILE" logs -f

