#!/usr/bin/env bash
set -euo pipefail

log() {
  printf '[summarize-runon-lanes] %s\n' "$*"
}

usage() {
  cat <<'USAGE'
Usage: summarize-runon-lanes.sh --report <utf-differential-report.json> [options]

Options:
  --label <name>         Label shown in summary (default: report filename)
  --output <path>        Write markdown summary to path
  --output-json <path>   Write machine-readable JSON summary to path
USAGE
}

REPORT_PATH=""
OUTPUT_PATH=""
OUTPUT_JSON_PATH=""
LABEL=""

while (($# > 0)); do
  case "$1" in
    --report=*)
      REPORT_PATH="${1#*=}"
      shift
      ;;
    --report)
      REPORT_PATH="${2:-}"
      shift 2
      ;;
    --output=*)
      OUTPUT_PATH="${1#*=}"
      shift
      ;;
    --output)
      OUTPUT_PATH="${2:-}"
      shift 2
      ;;
    --output-json=*)
      OUTPUT_JSON_PATH="${1#*=}"
      shift
      ;;
    --output-json)
      OUTPUT_JSON_PATH="${2:-}"
      shift 2
      ;;
    --label=*)
      LABEL="${1#*=}"
      shift
      ;;
    --label)
      LABEL="${2:-}"
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

if [ -z "$REPORT_PATH" ]; then
  usage
  exit 1
fi

if [ ! -f "$REPORT_PATH" ]; then
  log "report file not found: $REPORT_PATH"
  exit 1
fi

python3 - "$REPORT_PATH" "$OUTPUT_PATH" "$OUTPUT_JSON_PATH" "$LABEL" <<'PY'
import json
import sys
from pathlib import Path

report_path = Path(sys.argv[1])
output_path = Path(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2] else None
output_json_path = Path(sys.argv[3]) if len(sys.argv) > 3 and sys.argv[3] else None
label = str(sys.argv[4]).strip() if len(sys.argv) > 4 else ""
report = json.loads(report_path.read_text(encoding="utf-8"))

skipped_cases = report.get("skippedCases", [])

def is_runon_not_satisfied(case: dict) -> bool:
    reason = str(case.get("reason", ""))
    return reason.startswith("runOnRequirements not satisfied")

def is_mongos_pin_auto(source_path: str) -> bool:
    return source_path in {
        "transactions/tests/unified/mongos-pin-auto.json",
        "transactions/tests/unified/mongos-pin-auto.yml",
    }

def is_hint_legacy_lane(source_path: str) -> bool:
    if not source_path.startswith("crud/tests/unified/"):
        return False
    filename = source_path[len("crud/tests/unified/"):]
    if not (filename.endswith(".json") or filename.endswith(".yml") or filename.endswith(".yaml")):
        return False
    if "-hint-" not in filename:
        return False
    return ("unacknowledged" in filename) or ("clientError" in filename) or ("serverError" in filename)

def is_client_bulk_write_lane(source_path: str) -> bool:
    return source_path.startswith("crud/tests/unified/client-bulkWrite") or source_path.startswith(
        "transactions/tests/unified/client-bulkWrite"
    )

runon_cases = [case for case in skipped_cases if is_runon_not_satisfied(case)]

if not label:
    label = report_path.name

mongos_count = sum(1 for case in runon_cases if is_mongos_pin_auto(str(case.get("sourcePath", ""))))
hint_count = sum(1 for case in runon_cases if is_hint_legacy_lane(str(case.get("sourcePath", ""))))
client_bulk_count = sum(1 for case in runon_cases if is_client_bulk_write_lane(str(case.get("sourcePath", ""))))
remaining_count = len(runon_cases) - mongos_count - hint_count - client_bulk_count

summary = report.get("importSummary", {})
diff = report.get("differentialSummary", {})

lines = [
    "# runOn Lane Summary",
    "",
    f"- label: `{label}`",
    f"- report: `{report_path}`",
    f"- generatedAt: `{report.get('generatedAt', '')}`",
    f"- seed: `{report.get('seed', '')}`",
    "",
    "## Import/Diff Summary",
    f"- imported: `{summary.get('imported', 0)}`",
    f"- skipped: `{summary.get('skipped', 0)}`",
    f"- unsupported: `{summary.get('unsupported', 0)}`",
    f"- match: `{diff.get('match', 0)}`",
    f"- mismatch: `{diff.get('mismatch', 0)}`",
    f"- error: `{diff.get('error', 0)}`",
    "",
    "## runOn Not Satisfied Breakdown",
    f"- total: `{len(runon_cases)}`",
    f"- mongos-pin-auto lane: `{mongos_count}`",
    f"- hint legacy lane: `{hint_count}`",
    f"- client-bulkWrite lane: `{client_bulk_count}`",
    f"- remaining: `{remaining_count}`",
]

markdown = "\n".join(lines) + "\n"
print(markdown)

if output_path:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(markdown, encoding="utf-8")

if output_json_path:
    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    summary_json = {
        "label": label,
        "report": str(report_path),
        "generatedAt": report.get("generatedAt"),
        "seed": report.get("seed"),
        "importSummary": {
            "imported": int(summary.get("imported", 0)),
            "skipped": int(summary.get("skipped", 0)),
            "unsupported": int(summary.get("unsupported", 0)),
        },
        "differentialSummary": {
            "total": int(diff.get("total", 0)),
            "match": int(diff.get("match", 0)),
            "mismatch": int(diff.get("mismatch", 0)),
            "error": int(diff.get("error", 0)),
        },
        "runOnNotSatisfied": {
            "total": len(runon_cases),
            "lanes": {
                "mongosPinAuto": mongos_count,
                "hintLegacy": hint_count,
                "clientBulkWrite": client_bulk_count,
            },
            "remaining": remaining_count,
        },
    }
    output_json_path.write_text(json.dumps(summary_json, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
PY
