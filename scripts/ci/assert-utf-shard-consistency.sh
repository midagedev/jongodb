#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[assert-utf-shard-consistency] %s\n' "$*"
}

usage() {
  cat <<'EOF'
Usage: assert-utf-shard-consistency.sh --baseline-json <path> --rerun-json <path>
EOF
}

BASELINE_JSON=""
RERUN_JSON=""

while (($# > 0)); do
  case "$1" in
    --baseline-json=*)
      BASELINE_JSON="${1#*=}"
      shift
      ;;
    --baseline-json)
      BASELINE_JSON="${2:-}"
      shift 2
      ;;
    --rerun-json=*)
      RERUN_JSON="${1#*=}"
      shift
      ;;
    --rerun-json)
      RERUN_JSON="${2:-}"
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

if [ -z "$BASELINE_JSON" ] || [ -z "$RERUN_JSON" ]; then
  usage
  exit 1
fi

if [ ! -f "$BASELINE_JSON" ] || [ ! -f "$RERUN_JSON" ]; then
  log "JSON report file missing."
  exit 1
fi

python3 - "$BASELINE_JSON" "$RERUN_JSON" <<'PY'
import json
import sys
from pathlib import Path

baseline_path = Path(sys.argv[1])
rerun_path = Path(sys.argv[2])

baseline = json.loads(baseline_path.read_text(encoding="utf-8"))
rerun = json.loads(rerun_path.read_text(encoding="utf-8"))


def summary(doc):
    import_summary = doc.get("importSummary", {})
    differential = doc.get("differentialSummary", {})
    replays = doc.get("failureReplays", [])
    replay_pairs = [(r.get("scenarioId"), r.get("status"), r.get("message")) for r in replays]
    return {
        "imported": import_summary.get("imported"),
        "skipped": import_summary.get("skipped"),
        "unsupported": import_summary.get("unsupported"),
        "total": differential.get("total"),
        "match": differential.get("match"),
        "mismatch": differential.get("mismatch"),
        "error": differential.get("error"),
        "failureReplays": replay_pairs,
    }


b_summary = summary(baseline)
r_summary = summary(rerun)
if b_summary != r_summary:
    print("UTF shard consistency check failed.", file=sys.stderr)
    print("baseline=", json.dumps(b_summary, ensure_ascii=False), file=sys.stderr)
    print("rerun=", json.dumps(r_summary, ensure_ascii=False), file=sys.stderr)
    sys.exit(1)

print("UTF shard consistency check passed.")
PY

log "Shard consistency assertions passed."
