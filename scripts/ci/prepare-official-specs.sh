#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[prepare-official-specs] %s\n' "$*"
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
Usage: prepare-official-specs.sh --repo <url> --ref <commit-or-tag-or-branch> --dest <directory>
EOF
}

REPO_URL=""
SPEC_REF=""
DEST_DIR=""

while (($# > 0)); do
  case "$1" in
    --repo=*)
      REPO_URL="${1#*=}"
      shift
      ;;
    --repo)
      REPO_URL="${2:-}"
      shift 2
      ;;
    --ref=*)
      SPEC_REF="${1#*=}"
      shift
      ;;
    --ref)
      SPEC_REF="${2:-}"
      shift 2
      ;;
    --dest=*)
      DEST_DIR="${1#*=}"
      shift
      ;;
    --dest)
      DEST_DIR="${2:-}"
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

if [ -z "$REPO_URL" ] || [ -z "$SPEC_REF" ] || [ -z "$DEST_DIR" ]; then
  usage
  exit 1
fi

require_command git
require_command rm

if [ -d "$DEST_DIR/.git" ]; then
  log "Refreshing existing clone at ${DEST_DIR}."
  git -C "$DEST_DIR" remote set-url origin "$REPO_URL"
  git -C "$DEST_DIR" fetch --prune origin
else
  if [ -e "$DEST_DIR" ]; then
    log "Removing non-git destination path: ${DEST_DIR}"
    rm -rf "$DEST_DIR"
  fi
  log "Cloning ${REPO_URL} into ${DEST_DIR}."
  git clone --no-checkout "$REPO_URL" "$DEST_DIR"
fi

if git -C "$DEST_DIR" fetch --depth 1 origin "$SPEC_REF"; then
  git -C "$DEST_DIR" checkout --detach FETCH_HEAD
else
  log "Depth-limited fetch failed for ref ${SPEC_REF}, retrying full fetch."
  git -C "$DEST_DIR" fetch origin "$SPEC_REF"
  git -C "$DEST_DIR" checkout --detach FETCH_HEAD
fi

RESOLVED_SHA="$(git -C "$DEST_DIR" rev-parse HEAD)"
log "Resolved official specs ref to ${RESOLVED_SHA}"

if [ -n "${GITHUB_OUTPUT:-}" ]; then
  printf 'resolved_sha=%s\n' "$RESOLVED_SHA" >>"$GITHUB_OUTPUT"
fi
