# Compatibility Scorecard

Status date: 2026-02-24

This document is the final R3 certification snapshot used for release decisions.
It is an integration-test compatibility report, not a production MongoDB parity claim.
Scope: release baseline for `v0.1.1` (`f4a8bbb`), not the latest `main` head.

## Evidence Sources

| Evidence | Source run / artifact | Result |
| --- | --- | --- |
| Official UTF sharded differential (baseline) | GitHub Actions `Official Suite Sharded` run `22332998372` | PASS |
| Official UTF sharded differential (rerun/flake gate) | GitHub Actions `Official Suite Sharded` run `22332998372` | PASS |
| R3 failure ledger | GitHub Actions `R3 Failure Ledger` run `22332937657` | PASS |
| External Spring canary certification | GitHub Actions `R3 External Canary Certification` run `22332937633` | PASS |
| Support manifest scorecard | `build/reports/r2-compatibility/r2-compatibility-scorecard.json` | PASS |

## Final Gate Summary

| Gate | Status | Metrics |
| --- | --- | --- |
| UTF sharded baseline | PASS | imported=146, match=146, mismatch=0, error=0 |
| UTF sharded rerun | PASS | imported=146, match=146, mismatch=0, error=0 |
| UTF flake consistency | PASS | shard-0/1/2 baseline == rerun |
| R3 failure ledger | PASS | suiteCount=3, failureCount=0 |
| External canary certification | PASS | projectCount=3, canaryPass=3, rollbackSuccess=3, maxRecoverySeconds=38 |
| Spring compatibility matrix | PASS | totalCells=20, pass=20, fail=0, passRate=1.0 |
| Compatibility scorecard gates | PASS | pass=2, fail=0, missing=0 |

## Post-0.1.1 Main Deltas (Not Included in This Snapshot)

The following `main` changes landed after `v0.1.1` and require the next release-cycle certification snapshot:

- `#86`: single-hook Spring test annotation (`@JongodbMongoTest`)
- `#87`: `countDocuments`, `replaceOne`, `findOneAndUpdate`, `findOneAndReplace` command support
- `#88`: ordered `bulkWrite` core subset
- `#91`: projection subset expansion for `findOneAndUpdate` / `findOneAndReplace`

## Official UTF Coverage Snapshot

Totals from the baseline shard artifacts in run `22332998372`:

| Metric | Value |
| --- | --- |
| imported | 146 |
| skipped | 567 |
| unsupported | 868 |
| total differential cases | 146 |
| match | 146 |
| mismatch | 0 |
| error | 0 |

Top unsupported reasons in baseline artifacts:

| Reason | Count |
| --- | --- |
| `unsupported UTF operation: startTransaction` | 234 |
| `unsupported UTF operation: failPoint` | 134 |
| `unsupported UTF operation: clientBulkWrite` | 86 |
| `unsupported UTF operation: bulkWrite` | 84 |
| `unsupported UTF operation: findOneAndUpdate` | 48 |
| `unsupported UTF operation: createEntities` | 40 |
| `unsupported UTF operation: findOneAndReplace` | 36 |
| `unsupported UTF aggregate stage in pipeline` | 28 |
| `unsupported UTF operation: countDocuments` | 22 |
| `unsupported UTF operation: replaceOne` | 22 |

## Artifact Digest Snapshot

SHA-256 digests captured during certification assembly:

| Artifact | SHA-256 |
| --- | --- |
| `utf-shard-summary.md` | `93a8fefa915b48e6799f9f3f4004c53bf4d678a7f473b41e75ca5e3ee974fd08` |
| `shard-0 baseline utf-differential-report.json` | `483cd8cfbd8acd3ea590849bfe063b8abf31b4bbe37800e6c6ba5742b99b92af` |
| `shard-1 baseline utf-differential-report.json` | `cf226ab24bdddce9c6748bf1393cbaf776c704358788137f9875f78dbb6856eb` |
| `shard-2 baseline utf-differential-report.json` | `6dabf85f22be0e60bf275ab0fdc896a7fbc35413ce455c678ada5b0e32fe68e3` |
| `r3-failure-ledger.json` | `0dfe884ed25be8be46c2d7937f8386075ec00e90e4d5c5e83ccdbbe2b2079d08` |
| `r2-canary-certification.json` (R3 canary output artifact name) | `8ffd103c55f0cb3ef95c7fc52bfc347454984fcc057419c50e607acf4f565103` |
| `r2-compatibility-scorecard.json` | `3ba3469366a19453786e631841a633645082d6cd80fd1a22a50ed7ba4dead6a0` |
| `r2-support-manifest.json` | `84fd659f98a332134209b02d1302321aceaf56125fd691e0a7d38e564f4f913f` |

## Reproduction

Run the same gates locally:

```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon springCompatibilityMatrixEvidence

./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  r2CompatibilityEvidence \
  -Pr2CompatibilityUtfReport=/tmp/official-utf-summary.json \
  -Pr2CompatibilitySpringMatrixJson=build/reports/spring-matrix/spring-compatibility-matrix.json \
  -Pr2CompatibilityFailOnGate=true

./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  r3CanaryCertificationEvidence \
  -Pr3CanaryInputJson=testkit/canary/r3/projects.sample.json \
  -Pr3CanaryFailOnGate=true
```
