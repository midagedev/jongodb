#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[run-utf-shard] %s\n' "$*"
}

require_command() {
  local command_name="$1"
  if ! command -v "$command_name" >/dev/null 2>&1; then
    log "Required command not found: ${command_name}"
    exit 1
  fi
}

usage() {
  cat <<'EOF'
Usage: run-utf-shard.sh \
  --spec-repo-root <path> \
  --shard-index <int> \
  --shard-count <int> \
  --output-dir <path> \
  --seed <value> \
  --mongo-uri <uri> \
  [--suite-root <relative-path>]... \
  [--replay-limit <int>] \
  [--runon-lanes <enabled|disabled>] \
  [--gradle-cmd <command>]
EOF
}

SPEC_REPO_ROOT=""
SHARD_INDEX=""
SHARD_COUNT=""
OUTPUT_DIR=""
SEED=""
MONGO_URI=""
REPLAY_LIMIT="20"
RUNON_LANES_MODE="enabled"
GRADLE_CMD="gradle"
SUITE_ROOTS=()

while (($# > 0)); do
  case "$1" in
    --spec-repo-root=*)
      SPEC_REPO_ROOT="${1#*=}"
      shift
      ;;
    --spec-repo-root)
      SPEC_REPO_ROOT="${2:-}"
      shift 2
      ;;
    --shard-index=*)
      SHARD_INDEX="${1#*=}"
      shift
      ;;
    --shard-index)
      SHARD_INDEX="${2:-}"
      shift 2
      ;;
    --shard-count=*)
      SHARD_COUNT="${1#*=}"
      shift
      ;;
    --shard-count)
      SHARD_COUNT="${2:-}"
      shift 2
      ;;
    --output-dir=*)
      OUTPUT_DIR="${1#*=}"
      shift
      ;;
    --output-dir)
      OUTPUT_DIR="${2:-}"
      shift 2
      ;;
    --seed=*)
      SEED="${1#*=}"
      shift
      ;;
    --seed)
      SEED="${2:-}"
      shift 2
      ;;
    --mongo-uri=*)
      MONGO_URI="${1#*=}"
      shift
      ;;
    --mongo-uri)
      MONGO_URI="${2:-}"
      shift 2
      ;;
    --suite-root=*)
      SUITE_ROOTS+=("${1#*=}")
      shift
      ;;
    --suite-root)
      SUITE_ROOTS+=("${2:-}")
      shift 2
      ;;
    --replay-limit=*)
      REPLAY_LIMIT="${1#*=}"
      shift
      ;;
    --replay-limit)
      REPLAY_LIMIT="${2:-}"
      shift 2
      ;;
    --runon-lanes=*)
      RUNON_LANES_MODE="${1#*=}"
      shift
      ;;
    --runon-lanes)
      RUNON_LANES_MODE="${2:-}"
      shift 2
      ;;
    --gradle-cmd=*)
      GRADLE_CMD="${1#*=}"
      shift
      ;;
    --gradle-cmd)
      GRADLE_CMD="${2:-}"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      log "Unknown argument: $1"
      usage
      exit 1
      ;;
  esac
done

if [ -z "$SPEC_REPO_ROOT" ] || [ -z "$SHARD_INDEX" ] || [ -z "$SHARD_COUNT" ] \
  || [ -z "$OUTPUT_DIR" ] || [ -z "$SEED" ] || [ -z "$MONGO_URI" ]; then
  usage
  exit 1
fi

if ! [[ "$SHARD_INDEX" =~ ^[0-9]+$ ]] || ! [[ "$SHARD_COUNT" =~ ^[0-9]+$ ]]; then
  log "shard-index and shard-count must be non-negative integers."
  exit 1
fi

if [ "$SHARD_COUNT" -le 0 ] || [ "$SHARD_INDEX" -ge "$SHARD_COUNT" ]; then
  log "Invalid shard configuration: index=${SHARD_INDEX}, count=${SHARD_COUNT}"
  exit 1
fi

case "${RUNON_LANES_MODE}" in
  enabled|disabled)
    ;;
  *)
    log "runon-lanes must be one of: enabled | disabled"
    exit 1
    ;;
esac

if [ "${#SUITE_ROOTS[@]}" -eq 0 ]; then
  SUITE_ROOTS=(
    "source/crud/tests/unified"
    "source/transactions/tests/unified"
    "source/sessions/tests"
  )
fi

require_command find
require_command sort
require_command mkdir
require_command cp
require_command mktemp
require_command "$GRADLE_CMD"

if [ ! -d "$SPEC_REPO_ROOT" ]; then
  log "spec-repo-root does not exist: ${SPEC_REPO_ROOT}"
  exit 1
fi

mkdir -p "$OUTPUT_DIR"

SPEC_SELECTION_FILE="$OUTPUT_DIR/spec-selection.txt"
: >"$SPEC_SELECTION_FILE"

TEMP_SHARD_ROOT="$(mktemp -d)"
cleanup() {
  rm -rf "$TEMP_SHARD_ROOT"
}
trap cleanup EXIT

spec_files=()
for suite_root in "${SUITE_ROOTS[@]}"; do
  suite_path="${SPEC_REPO_ROOT}/${suite_root}"
  if [ -d "$suite_path" ]; then
    while IFS= read -r file; do
      spec_files+=("$file")
    done < <(find "$suite_path" -type f \( -name "*.json" -o -name "*.yml" -o -name "*.yaml" \))
  fi
done

if [ "${#spec_files[@]}" -eq 0 ]; then
  log "No spec files found under configured suite roots."
  exit 1
fi

IFS=$'\n' read -r -d '' -a sorted_files < <(printf '%s\n' "${spec_files[@]}" | LC_ALL=C sort -u && printf '\0')

selected_count=0
for index in "${!sorted_files[@]}"; do
  file_path="${sorted_files[$index]}"
  if [ $((index % SHARD_COUNT)) -ne "$SHARD_INDEX" ]; then
    continue
  fi

  relative_path="${file_path#${SPEC_REPO_ROOT}/}"
  destination_path="${TEMP_SHARD_ROOT}/${relative_path}"
  mkdir -p "$(dirname "$destination_path")"
  cp "$file_path" "$destination_path"
  printf '%s\n' "$relative_path" >>"$SPEC_SELECTION_FILE"
  selected_count=$((selected_count + 1))
done

if [ "$selected_count" -le 0 ]; then
  log "Shard ${SHARD_INDEX}/${SHARD_COUNT} selected zero spec files."
  exit 1
fi

SHARD_SEED="${SEED}-shard-${SHARD_INDEX}-of-${SHARD_COUNT}"
log "Running shard ${SHARD_INDEX}/${SHARD_COUNT} with ${selected_count} files and seed=${SHARD_SEED} (runon-lanes=${RUNON_LANES_MODE})."

JONGODB_UTF_RUNON_LANES="${RUNON_LANES_MODE}" "$GRADLE_CMD" --no-daemon \
  -PutfSpecRoot="${TEMP_SHARD_ROOT}/source" \
  -PutfOutputDir="$OUTPUT_DIR" \
  -PutfSeed="$SHARD_SEED" \
  -PutfReplayLimit="$REPLAY_LIMIT" \
  -PutfMongoUri="$MONGO_URI" \
  utfCorpusEvidence

JSON_REPORT="${OUTPUT_DIR}/utf-differential-report.json"
if [ ! -f "$JSON_REPORT" ]; then
  log "Expected report not found: ${JSON_REPORT}"
  exit 1
fi

printf '%s\n' "$selected_count" >"${OUTPUT_DIR}/selected-count.txt"
log "Shard run completed. Output: ${OUTPUT_DIR}"
