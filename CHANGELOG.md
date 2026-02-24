# Changelog

All notable changes to this project are documented in this file.

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
