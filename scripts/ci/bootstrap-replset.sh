#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[bootstrap-replset] %s\n' "$*"
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    log "Required command not found: ${command_name}"
    exit 1
  fi
}

wait_for_condition() {
  local description="$1"
  local timeout_seconds="$2"
  local interval_seconds="$3"
  shift 3

  local deadline
  deadline=$(( $(date +%s) + timeout_seconds ))

  while true; do
    if "$@"; then
      return 0
    fi

    if [ "$(date +%s)" -ge "$deadline" ]; then
      log "Timed out waiting for ${description} after ${timeout_seconds}s."
      return 1
    fi

    sleep "$interval_seconds"
  done
}

mongo_ping_ready() {
  docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval 'const ping = db.adminCommand({ ping: 1 }); if (ping.ok === 1) { quit(0); } quit(1);' >/dev/null 2>&1
}

mongo_primary_ready() {
  docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval '
    try {
      const hello = db.adminCommand({ hello: 1 });
      if (hello.isWritablePrimary === true || hello.ismaster === true) {
        quit(0);
      }
    } catch (error) {
      // Retry until timeout.
    }
    quit(1);
  ' >/dev/null 2>&1
}

initialize_replica_set() {
  docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval "
    const replSetName = '${MONGO_REPLSET_NAME}';
    const host = '${MONGO_HOST}:${MONGO_PORT}';
    let initialized = true;

    try {
      rs.status();
    } catch (error) {
      initialized = false;
      if (error.codeName !== 'NotYetInitialized' && error.code !== 94) {
        throw error;
      }
    }

    if (!initialized) {
      const result = rs.initiate({
        _id: replSetName,
        members: [{ _id: 0, host: host }]
      });
      if (result.ok !== 1 && result.codeName !== 'AlreadyInitialized') {
        throw new Error('rs.initiate failed: ' + JSON.stringify(result));
      }
    }
  " >/dev/null
}

MONGO_IMAGE="${MONGO_IMAGE:-mongo:7.0}"
MONGO_CONTAINER_NAME="${MONGO_CONTAINER_NAME:-jongodb-replset}"
MONGO_REPLSET_NAME="${MONGO_REPLSET_NAME:-rs0}"
MONGO_HOST="${MONGO_HOST:-127.0.0.1}"
MONGO_PORT="${MONGO_PORT:-27017}"
REPLSET_WAIT_TIMEOUT_SECONDS="${REPLSET_WAIT_TIMEOUT_SECONDS:-120}"
REPLSET_WAIT_INTERVAL_SECONDS="${REPLSET_WAIT_INTERVAL_SECONDS:-2}"

require_command docker

if ! docker info >/dev/null 2>&1; then
  log "Docker daemon is unavailable."
  exit 1
fi

if docker ps -a --format '{{.Names}}' | grep -Fxq "$MONGO_CONTAINER_NAME"; then
  log "Removing existing container ${MONGO_CONTAINER_NAME}."
  docker rm -f "$MONGO_CONTAINER_NAME" >/dev/null
fi

log "Starting MongoDB container ${MONGO_CONTAINER_NAME} (${MONGO_IMAGE})."
docker run --detach \
  --name "$MONGO_CONTAINER_NAME" \
  --publish "${MONGO_PORT}:${MONGO_PORT}" \
  "$MONGO_IMAGE" \
  --replSet "$MONGO_REPLSET_NAME" \
  --bind_ip_all \
  --port "$MONGO_PORT" >/dev/null

if ! wait_for_condition "mongod ping readiness" "$REPLSET_WAIT_TIMEOUT_SECONDS" "$REPLSET_WAIT_INTERVAL_SECONDS" mongo_ping_ready; then
  docker logs "$MONGO_CONTAINER_NAME" || true
  exit 1
fi

log "Initializing replica set ${MONGO_REPLSET_NAME}."
initialize_replica_set

if ! wait_for_condition "replica set primary election" "$REPLSET_WAIT_TIMEOUT_SECONDS" "$REPLSET_WAIT_INTERVAL_SECONDS" mongo_primary_ready; then
  log "Replica set did not elect a writable primary in time."
  docker logs "$MONGO_CONTAINER_NAME" || true
  docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval 'JSON.stringify(rs.status(), null, 2)' || true
  exit 1
fi

MONGO_URI="mongodb://${MONGO_HOST}:${MONGO_PORT}/?replicaSet=${MONGO_REPLSET_NAME}&retryWrites=false&directConnection=true"
log "Replica set primary is ready."
log "Exporting JONGODB_REAL_MONGOD_URI=${MONGO_URI}"

export JONGODB_REAL_MONGOD_URI="$MONGO_URI"

if [ -n "${GITHUB_ENV:-}" ]; then
  printf 'JONGODB_REAL_MONGOD_URI=%s\n' "$MONGO_URI" >>"$GITHUB_ENV"
fi

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf 'uri=%s\n' "$MONGO_URI" >>"$GITHUB_OUTPUT"
fi
