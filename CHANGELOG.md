# Changelog

All notable changes to this project are documented in this file.

## [Unreleased]

### Added
- Added declarative fixture manifest schema (`fixture-manifest.v1`) with standardized `dev`/`smoke`/`full` profile model.
- Added fixture manifest loader/validator with path-aware aggregated validation errors.
- Added deterministic fixture extraction planner + profile fingerprint generation.
- Added fixture manifest CLI utility (`FixtureManifestTool`) and Gradle task (`fixtureManifestPlan`) for profile plan validation/output.
- Added sample fixture manifest templates under `testkit/fixture/manifests`.
- Added fixture manifest onboarding documentation (`docs/FIXTURE_MANIFEST.md`).
- Added fixture usage/operations playbook (`docs/FIXTURE_PLAYBOOK.md`) for developer/reviewer/operator workflows.
- Added Node adapter replica-set profile contract tests for URI/env propagation and `hello` handshake invariants.
- Added Node adapter topology-profile/URI option sync validation with mismatch regression tests.
- Added Jest global setup/teardown stabilization for idempotent state reuse and stale state auto-recovery.
- Added Vitest workspace helper for project-level DB/env isolation policy.
- Added Nest Jest adapter hardened defaults (`MONGODB_URI`+`DATABASE_URL`, worker DB isolation) with override coverage.
- Added Express integration smoke scenario and recipe coverage in node-compat suite.
- Added Koa integration smoke scenario and recipe coverage in node-compat suite.
- Added Mongoose transaction/session smoke harness scenario with session visibility/commit coverage.
- Added TypeORM MongoDB repository CRUD smoke harness scenario in node-compat suite.
- Added Prisma MongoDB provider CRUD smoke harness with generated client workflow in node-compat suite.
- Added node adapter compatibility CI matrix across Ubuntu/macOS and Node 20/22.
- Added npm release pipeline tarball attestation flow (`actions/attest-build-provenance`) for binary/core Node packages.
- Added Node adapter canary pre-release workflow with canary dist-tag publish automation and attested tarball flow.
- Added Node adapter semver/compatibility policy documentation (`docs/NODE_SEMVER_POLICY.md`).
- Added dedicated migration guide from `mongodb-memory-server` to `@jongodb/memory-server`.
- Added Node adapter troubleshooting playbook mapped by error signatures.

## [0.1.3] - 2026-02-24

### Added
- Added real-mongod differential execution adapter path for UTF `bulkWrite` scenarios.
- Added transaction regression coverage for:
  - repeated `commitTransaction` replay behavior
  - `abortTransaction` after `commitTransaction`
  - commit-time write conflict handling across concurrent transactions
- Added explicit engine exception type for commit-time write conflict detection.

### Fixed
- Resolved remaining official UTF differential mismatches for imported `crud`, `transactions`, and `sessions` suites.
- Fixed `bulkWrite` update validation to require atomic-modifier style updates.
- Fixed `replaceOne` validation parity in real-mongod differential backend.
- Fixed null `_id` upsert result propagation so update/replace upsert counters align with reference behavior.

### Changed
- Added default `_id` unique index behavior to the in-memory collection store.
- Strengthened transaction terminal-state handling (`COMMITTED`/`ABORTED`) for retry and post-terminal command semantics.
- Updated compatibility scorecard snapshot to current `failureCount=0` R3 ledger evidence.
- Default local project version moved to `0.1.3-SNAPSHOT`.

## [0.1.2] - 2026-02-24

### Added
- Added single-hook Spring test bootstrap annotation: `@JongodbMongoTest`.
- Added command handlers for `countDocuments`, `replaceOne`, `findOneAndUpdate`, `findOneAndReplace`.
- Added ordered `bulkWrite` core subset support.
- Expanded `findOneAndUpdate` / `findOneAndReplace` projection subset behavior.
- Added PR CI workflow `.github/workflows/ci-test-suite.yml` to enforce `clean test` on PRs and `main`.

### Fixed
- Included `aggregate` in transactional command routing so transaction-scoped aggregate operations use the transaction snapshot.
- Added regression coverage for transactional aggregate visibility and post-commit publish behavior.

### Changed
- Maven release workflow now runs `clean test` before staging/deploy.
- Default local project version moved to `0.1.2-SNAPSHOT`.
- Updated README and release docs to distinguish `v0.1.1` certification baseline from current `main`.

## [0.1.1] - 2026-02-24

### Fixed
- Replaced full-store transaction snapshot publish with namespace-aware merge on commit.
- Preserved out-of-transaction writes interleaved during an open transaction, including same-namespace writes with different `_id` values.

### Behavior
- Added deterministic same-document conflict policy: when transactional and non-transactional writes target the same `_id`, commit applies the transactional version.

### Testing
- Added mixed transactional/non-transactional regression scenarios for commit and abort paths.
- Expanded Spring compatibility matrix scenarios for multi-template transaction paths and interleaving write cases.
- Verified full test suite and Spring matrix evidence generation on release branch.

## [0.1.0] - 2026-02-24

Initial public release.

### Added
- In-memory MongoDB-compatible command engine for Spring integration tests.
- In-process command ingress with deterministic unsupported-feature responses.
- Differential/evidence tooling for compatibility and release readiness.

### Publishing
- Maven coordinate: `io.github.midagedev:jongodb:0.1.0`
- Maven path: `https://repo1.maven.org/maven2/io/github/midagedev/jongodb/0.1.0/`
- Initial Maven Central publish workflow run: `22334177236`
