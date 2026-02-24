#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

DEFAULT_REPO_URL="https://github.com/mongodb/specifications.git"
DEFAULT_MANIFEST_PATH="third_party/mongodb-specs/manifest.json"
DEFAULT_CHECKOUT_DIR="third_party/mongodb-specs/.checkout/specifications"

DEFAULT_INCLUDED_SUITES=(
  "source/transactions/tests/unified"
  "source/sessions/tests"
  "source/crud/tests/unified"
  "source/unified-test-format"
)

print_usage() {
  cat <<'EOF'
Usage:
  sync-mongodb-specs.sh --commit <sha> [options]

Options:
  --commit <sha>            Required MongoDB specifications commit to pin.
  --repo-url <url>          Source repository URL.
                            Default: https://github.com/mongodb/specifications.git
  --manifest <path>         Manifest file path.
                            Default: third_party/mongodb-specs/manifest.json
  --checkout-dir <path>     Local checkout directory for pinned commit.
                            Default: third_party/mongodb-specs/.checkout/specifications
  --suite-root <path>       Included suite root under the spec repo (repeatable).
                            Default roots:
                              - source/transactions/tests/unified
                              - source/sessions/tests
                              - source/crud/tests/unified
                              - source/unified-test-format
  --help                    Show help.
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

normalize_suite_root() {
  local root="$1"
  root="${root#/}"
  root="${root%/}"
  printf '%s\n' "${root}"
}

json_escape() {
  printf '%s' "$1" | sed -e 's/\\/\\\\/g' -e 's/"/\\"/g'
}

repo_url="${DEFAULT_REPO_URL}"
manifest_path="${DEFAULT_MANIFEST_PATH}"
checkout_dir="${DEFAULT_CHECKOUT_DIR}"
pinned_commit=""
included_suites=()

while [[ $# -gt 0 ]]; do
  case "$1" in
    --commit)
      [[ $# -ge 2 ]] || die "--commit requires a value"
      pinned_commit="$2"
      shift 2
      ;;
    --repo-url)
      [[ $# -ge 2 ]] || die "--repo-url requires a value"
      repo_url="$2"
      shift 2
      ;;
    --manifest)
      [[ $# -ge 2 ]] || die "--manifest requires a value"
      manifest_path="$2"
      shift 2
      ;;
    --checkout-dir)
      [[ $# -ge 2 ]] || die "--checkout-dir requires a value"
      checkout_dir="$2"
      shift 2
      ;;
    --suite-root)
      [[ $# -ge 2 ]] || die "--suite-root requires a value"
      normalized_suite="$(normalize_suite_root "$2")"
      [[ -n "${normalized_suite}" ]] || die "--suite-root cannot be empty"
      included_suites+=("${normalized_suite}")
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

[[ -n "${pinned_commit}" ]] || die "--commit is required"

if [[ "${#included_suites[@]}" -eq 0 ]]; then
  included_suites=("${DEFAULT_INCLUDED_SUITES[@]}")
fi

manifest_abs="$(resolve_path "${manifest_path}")"
checkout_abs="$(resolve_path "${checkout_dir}")"

mkdir -p "$(dirname "${manifest_abs}")"
mkdir -p "$(dirname "${checkout_abs}")"

if [[ -d "${checkout_abs}/.git" ]]; then
  current_origin="$(git -C "${checkout_abs}" config --get remote.origin.url || true)"
  if [[ -z "${current_origin}" ]]; then
    git -C "${checkout_abs}" remote add origin "${repo_url}"
  elif [[ "${current_origin}" != "${repo_url}" ]]; then
    git -C "${checkout_abs}" remote set-url origin "${repo_url}"
  fi
else
  rm -rf "${checkout_abs}"
  if ! git clone --no-checkout --filter=blob:none "${repo_url}" "${checkout_abs}" >/dev/null 2>&1; then
    rm -rf "${checkout_abs}"
    git clone --no-checkout "${repo_url}" "${checkout_abs}" >/dev/null
  fi
fi

if ! git -C "${checkout_abs}" fetch --force --depth 1 origin "${pinned_commit}" >/dev/null 2>&1; then
  git -C "${checkout_abs}" fetch --force --prune origin >/dev/null
fi

resolved_commit="$(git -C "${checkout_abs}" rev-parse --verify "${pinned_commit}^{commit}")"
git -C "${checkout_abs}" -c advice.detachedHead=false checkout --detach --force "${resolved_commit}" >/dev/null

actual_commit="$(git -C "${checkout_abs}" rev-parse --verify HEAD)"
if [[ "${actual_commit}" != "${resolved_commit}" ]]; then
  die "checkout mismatch: expected ${resolved_commit}, got ${actual_commit}"
fi

generated_at_utc="$(date -u +%Y-%m-%dT%H:%M:%SZ)"

tmp_manifest="${manifest_abs}.tmp"
{
  echo "{"
  echo "  \"schemaVersion\": 1,"
  echo "  \"sourceRepoUrl\": \"$(json_escape "${repo_url}")\","
  echo "  \"pinnedCommit\": \"${actual_commit}\","
  echo "  \"generatedAtUtc\": \"${generated_at_utc}\","
  echo "  \"checkoutDir\": \"$(json_escape "${checkout_dir}")\","
  echo "  \"includedSuiteRoots\": ["
  for i in "${!included_suites[@]}"; do
    comma=","
    if [[ "${i}" -eq $((${#included_suites[@]} - 1)) ]]; then
      comma=""
    fi
    echo "    \"$(json_escape "${included_suites[$i]}")\"${comma}"
  done
  echo "  ]"
  echo "}"
} > "${tmp_manifest}"
mv "${tmp_manifest}" "${manifest_abs}"

echo "synced mongodb/specifications"
echo "repo: ${repo_url}"
echo "commit: ${actual_commit}"
echo "checkout: ${checkout_dir}"
echo "manifest: ${manifest_path}"
