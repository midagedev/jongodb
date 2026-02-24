#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

SYNC_SCRIPT="${REPO_ROOT}/scripts/spec-sync/sync-mongodb-specs.sh"
VERIFY_SCRIPT="${REPO_ROOT}/scripts/spec-sync/verify-mongodb-specs.sh"

tmp_dir="$(mktemp -d "${TMPDIR:-/tmp}/jongodb-spec-sync.XXXXXX")"
trap 'rm -rf "${tmp_dir}"' EXIT

remote_repo="${tmp_dir}/specifications-remote.git"
seed_repo="${tmp_dir}/specifications-seed"
checkout_dir="${tmp_dir}/specifications-checkout"
manifest_path="${tmp_dir}/manifest.json"

mkdir -p "${seed_repo}"
git init "${seed_repo}" >/dev/null

mkdir -p "${seed_repo}/source/transactions/tests/unified"
mkdir -p "${seed_repo}/source/sessions/tests"
mkdir -p "${seed_repo}/source/crud/tests/unified"
mkdir -p "${seed_repo}/source/unified-test-format"

cat > "${seed_repo}/source/transactions/tests/unified/txn.yml" <<'EOF'
description: transactions fixture
EOF
cat > "${seed_repo}/source/sessions/tests/sessions.yml" <<'EOF'
description: sessions fixture
EOF
cat > "${seed_repo}/source/crud/tests/unified/crud.yml" <<'EOF'
description: crud fixture
EOF
cat > "${seed_repo}/source/unified-test-format/schema-latest.json" <<'EOF'
{"type":"object"}
EOF

git -C "${seed_repo}" add .
git -C "${seed_repo}" \
  -c user.name="spec-sync-test" \
  -c user.email="spec-sync-test@example.com" \
  commit -m "seed commit" >/dev/null
commit_one="$(git -C "${seed_repo}" rev-parse --verify HEAD)"

echo "description: transactions fixture v2" > "${seed_repo}/source/transactions/tests/unified/txn.yml"
git -C "${seed_repo}" add source/transactions/tests/unified/txn.yml
git -C "${seed_repo}" \
  -c user.name="spec-sync-test" \
  -c user.email="spec-sync-test@example.com" \
  commit -m "update commit" >/dev/null
commit_two="$(git -C "${seed_repo}" rev-parse --verify HEAD)"

git init --bare "${remote_repo}" >/dev/null
git -C "${seed_repo}" remote add origin "${remote_repo}"
git -C "${seed_repo}" push --force origin HEAD:main >/dev/null

"${SYNC_SCRIPT}" \
  --repo-url "${remote_repo}" \
  --commit "${commit_one}" \
  --checkout-dir "${checkout_dir}" \
  --manifest "${manifest_path}" >/dev/null

"${VERIFY_SCRIPT}" --manifest "${manifest_path}" >/dev/null

manifest_commit="$(sed -n 's/.*"pinnedCommit": "\([0-9a-f]\{40\}\)".*/\1/p' "${manifest_path}")"
if [[ "${manifest_commit}" != "${commit_one}" ]]; then
  echo "expected manifest pinnedCommit=${commit_one}, got ${manifest_commit}" >&2
  exit 1
fi

git -C "${checkout_dir}" fetch --force origin "${commit_two}" >/dev/null
git -C "${checkout_dir}" -c advice.detachedHead=false checkout --detach --force "${commit_two}" >/dev/null

if "${VERIFY_SCRIPT}" --manifest "${manifest_path}" >/dev/null 2>&1; then
  echo "verify should fail when checkout commit diverges from manifest" >&2
  exit 1
fi

echo "spec-sync integration test passed"
