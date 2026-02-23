#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[collect-replset-diagnostics] %s\n' "$*"
}

capture_command() {
  local name="$1"
  shift

  local output_file="${REPLSET_DIAGNOSTICS_DIR}/${name}.txt"
  {
    printf '# timestamp: %s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf '# command:'
    for arg in "$@"; do
      printf ' %q' "$arg"
    done
    printf '\n\n'
  } >"$output_file"

  if "$@" >>"$output_file" 2>&1; then
    return 0
  fi

  local exit_code=$?
  printf '\n# command failed with exit code %s\n' "$exit_code" >>"$output_file"
  return 0
}

MONGO_CONTAINER_NAME="${MONGO_CONTAINER_NAME:-jongodb-replset}"
MONGO_PORT="${MONGO_PORT:-27017}"
REPLSET_DIAGNOSTICS_DIR="${1:-${REPLSET_DIAGNOSTICS_DIR:-build/reports/replset-diagnostics}}"

mkdir -p "$REPLSET_DIAGNOSTICS_DIR"

{
  printf 'timestamp_utc=%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
  printf 'container_name=%s\n' "$MONGO_CONTAINER_NAME"
  printf 'mongo_uri=%s\n' "${JONGODB_REAL_MONGOD_URI:-unset}"
} >"${REPLSET_DIAGNOSTICS_DIR}/context.txt"

if ! command -v docker >/dev/null 2>&1; then
  log "Docker command is unavailable; writing marker file and exiting."
  printf 'docker command not found\n' >"${REPLSET_DIAGNOSTICS_DIR}/docker-unavailable.txt"
  exit 0
fi

capture_command "docker-version" docker version
capture_command "docker-ps" docker ps -a
capture_command "docker-inspect" docker inspect "$MONGO_CONTAINER_NAME"
capture_command "docker-logs" docker logs "$MONGO_CONTAINER_NAME"
capture_command "mongosh-hello" docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval 'JSON.stringify(db.adminCommand({ hello: 1 }), null, 2)'
capture_command "mongosh-rs-status" docker exec "$MONGO_CONTAINER_NAME" mongosh --quiet --port "$MONGO_PORT" --eval 'JSON.stringify(rs.status(), null, 2)'
