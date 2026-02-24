#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DEFAULT_MANIFEST_PATH="third_party/mongodb-specs/manifest.json"

print_usage() {
  cat <<'EOF'
Usage:
  verify-mongodb-specs.sh [options]

Options:
  --manifest <path>       Manifest file path.
                          Default: third_party/mongodb-specs/manifest.json
  --checkout-dir <path>   Override checkout directory instead of manifest value.
  --help                  Show help.
EOF
}

die() {
  echo "error: $*" >&2
  exit 1
}

resolve_path() {
  local input="$1"
  if [[ "${input}" = /* ]]; then
    printf '%s\n' "${input}"
  else
    printf '%s/%s\n' "${REPO_ROOT}" "${input}"
  fi
}

normalize_repo_url() {
  local url="$1"
  case "${url}" in
    git@github.com:*)
      url="https://github.com/${url#git@github.com:}"
      ;;
    ssh://git@github.com/*)
      url="https://github.com/${url#ssh://git@github.com/}"
      ;;
    git://github.com/*)
      url="https://github.com/${url#git://github.com/}"
      ;;
  esac
  url="${url%/}"
  url="${url%.git}"
  printf '%s\n' "${url}"
}

manifest_path="${DEFAULT_MANIFEST_PATH}"
checkout_override=""

while [[ $# -gt 0 ]]; do
  case "$1" in
    --manifest)
      [[ $# -ge 2 ]] || die "--manifest requires a value"
      manifest_path="$2"
      shift 2
      ;;
    --checkout-dir)
      [[ $# -ge 2 ]] || die "--checkout-dir requires a value"
      checkout_override="$2"
      shift 2
      ;;
    --help|-h)
      print_usage
      exit 0
      ;;
    *)
      die "unknown option: $1"
      ;;
  esac
done

[[ -x "$(command -v python3 || true)" ]] || die "python3 is required for manifest parsing"

manifest_abs="$(resolve_path "${manifest_path}")"
[[ -f "${manifest_abs}" ]] || die "manifest not found: ${manifest_abs}"

if ! parsed_lines="$(python3 - "${manifest_abs}" <<'PY'
import json
import re
import sys

manifest_path = sys.argv[1]

with open(manifest_path, "r", encoding="utf-8") as fh:
    data = json.load(fh)

required_keys = ("sourceRepoUrl", "pinnedCommit", "generatedAtUtc", "includedSuiteRoots")
for key in required_keys:
    if key not in data:
        raise SystemExit(f"missing required key: {key}")

source_repo = data["sourceRepoUrl"]
pinned_commit = data["pinnedCommit"]
generated_at = data["generatedAtUtc"]
checkout_dir = data.get("checkoutDir", "third_party/mongodb-specs/.checkout/specifications")
included_roots = data["includedSuiteRoots"]

if not isinstance(source_repo, str) or not source_repo:
    raise SystemExit("invalid sourceRepoUrl")
if not isinstance(pinned_commit, str) or not re.fullmatch(r"[0-9a-f]{40}", pinned_commit):
    raise SystemExit("invalid pinnedCommit; expected lowercase 40-char sha")
if not isinstance(generated_at, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}Z", generated_at):
    raise SystemExit("invalid generatedAtUtc; expected YYYY-MM-DDTHH:MM:SSZ")
if not isinstance(checkout_dir, str) or not checkout_dir.strip():
    raise SystemExit("invalid checkoutDir")
if (
    not isinstance(included_roots, list)
    or len(included_roots) == 0
    or any((not isinstance(v, str)) or not v.strip() for v in included_roots)
):
    raise SystemExit("invalid includedSuiteRoots")

print(f"repo={source_repo}")
print(f"commit={pinned_commit}")
print(f"generated={generated_at}")
print(f"checkout={checkout_dir}")
for root in included_roots:
    normalized = root.strip().strip("/")
    print(f"root={normalized}")
PY
)"; then
  die "manifest validation failed for ${manifest_abs}"
fi

source_repo=""
pinned_commit=""
generated_at=""
checkout_manifest=""
included_roots=()

while IFS= read -r line; do
  case "${line}" in
    repo=*)
      source_repo="${line#repo=}"
      ;;
    commit=*)
      pinned_commit="${line#commit=}"
      ;;
    generated=*)
      generated_at="${line#generated=}"
      ;;
    checkout=*)
      checkout_manifest="${line#checkout=}"
      ;;
    root=*)
      included_roots+=("${line#root=}")
      ;;
  esac
done <<EOF
${parsed_lines}
EOF

[[ -n "${source_repo}" ]] || die "manifest missing sourceRepoUrl"
[[ -n "${pinned_commit}" ]] || die "manifest missing pinnedCommit"
[[ -n "${generated_at}" ]] || die "manifest missing generatedAtUtc"
[[ "${#included_roots[@]}" -gt 0 ]] || die "manifest missing includedSuiteRoots"

checkout_path="${checkout_manifest}"
if [[ -n "${checkout_override}" ]]; then
  checkout_path="${checkout_override}"
fi

checkout_abs="$(resolve_path "${checkout_path}")"
[[ -d "${checkout_abs}/.git" ]] || die "checkout git dir not found: ${checkout_abs}"

actual_commit="$(git -C "${checkout_abs}" rev-parse --verify HEAD)"
if [[ "${actual_commit}" != "${pinned_commit}" ]]; then
  die "checkout commit mismatch: expected ${pinned_commit}, got ${actual_commit}"
fi

origin_url="$(git -C "${checkout_abs}" config --get remote.origin.url || true)"
[[ -n "${origin_url}" ]] || die "remote.origin.url missing in checkout: ${checkout_abs}"

normalized_manifest_repo="$(normalize_repo_url "${source_repo}")"
normalized_origin_repo="$(normalize_repo_url "${origin_url}")"
if [[ "${normalized_manifest_repo}" != "${normalized_origin_repo}" ]]; then
  die "origin URL mismatch: expected ${source_repo}, got ${origin_url}"
fi

for suite_root in "${included_roots[@]}"; do
  [[ -e "${checkout_abs}/${suite_root}" ]] || die "included suite root missing: ${suite_root}"
done

echo "manifest and checkout verified"
echo "manifest: ${manifest_path}"
echo "generatedAtUtc: ${generated_at}"
echo "repo: ${source_repo}"
echo "commit: ${pinned_commit}"
