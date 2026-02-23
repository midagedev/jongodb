#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[teardown-replset] %s\n' "$*"
}

MONGO_CONTAINER_NAME="${MONGO_CONTAINER_NAME:-jongodb-replset}"

if ! command -v docker >/dev/null 2>&1; then
  log "Docker command is unavailable; skipping teardown."
  exit 0
fi

if docker ps -a --format '{{.Names}}' | grep -Fxq "$MONGO_CONTAINER_NAME"; then
  log "Removing container ${MONGO_CONTAINER_NAME}."
  docker rm -f "$MONGO_CONTAINER_NAME" >/dev/null
else
  log "Container ${MONGO_CONTAINER_NAME} not found; nothing to teardown."
fi
