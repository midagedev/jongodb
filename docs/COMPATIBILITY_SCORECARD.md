# Compatibility Scorecard

Status date: 2026-02-24

This scorecard tracks integration-test compatibility against MongoDB official specs.
It is not a production MongoDB parity claim.

## Scope

- Baseline target: `v0.1.2` and current `main`.
- Primary objective: increase imported differential coverage while keeping mismatch/error at zero.

## Evidence Sources

| Evidence | Source run / artifact | Result |
| --- | --- | --- |
| Official UTF sharded differential (historical baseline) | GitHub Actions `Official Suite Sharded` run `22339640229` | PASS |
| R3 failure ledger (current) | `build/reports/r3-failure-ledger-local/r3-failure-ledger.json` | PASS (`failureCount=0`) |
| UTF differential (current, suite-level) | `build/reports/unified-spec-*-local/utf-differential-report.json` | PASS (`mismatch=0`, `error=0`) |

## Baseline vs Current

Historical frozen baseline (`22339640229`):

| Metric | Value |
| --- | --- |
| imported | 200 |
| skipped | 567 |
| unsupported | 814 |
| total differential cases | 200 |
| match | 146 |
| mismatch | 54 |
| error | 0 |

Current local certification snapshot (`r3-failure-ledger-local`, generated 2026-02-24T07:35:28Z):

| Metric | Value |
| --- | --- |
| imported | 480 |
| skipped | 575 |
| unsupported | 526 |
| total differential cases | 480 |
| match | 480 |
| mismatch | 0 |
| error | 0 |

## Current R3 Ledger Snapshot

| Suite | Imported | Unsupported | Mismatch | Error |
| --- | --- | --- | --- | --- |
| `crud-unified` | 298 | 252 | 0 | 0 |
| `transactions-unified` | 162 | 228 | 0 | 0 |
| `sessions` | 20 | 46 | 0 | 0 |

Current ledger gate status:

- `failureCount=0`
- `byTrack={}`
- `byStatus={}`

## Top Unsupported Reasons (Current)

| Reason | Count |
| --- | --- |
| `unsupported-by-policy UTF operation: failPoint` | 136 |
| `unsupported UTF operation: clientBulkWrite` | 92 |
| `unsupported UTF operation: targetedFailPoint` | 58 |
| `unsupported UTF update option: arrayFilters` | 32 |
| `unsupported UTF aggregate stage in pipeline` | 28 |
| `unsupported UTF operation: findOneAndDelete` | 26 |
| `unsupported UTF operation: distinct` | 26 |
| `unsupported UTF operation: runCommand` | 14 |
| `unsupported UTF operation: count` | 12 |
| `unsupported UTF update pipeline` | 10 |

Notes:

- UTF importer now supports subset adapters for `runCommand` and `clientBulkWrite` (`#229`, `#231`).
- The counts above are from the latest frozen snapshot; rerunning the ledger will shift those categories toward narrower unsupported reasons (for example unsupported command names/options).

## Policy Exclusions

- `failPoint` remains a policy exclusion for deterministic in-process execution.
- Scorecard accounting distinguishes this as `unsupported-by-policy UTF operation: failPoint`.

## Gap-to-Issue Mapping

- `#100`: transaction operation adapter coverage (`startTransaction` and lifecycle wiring) - completed.
- `#101`: unified CRUD adapter coverage (`bulkWrite`, `findOneAndUpdate`, `findOneAndReplace`, `countDocuments`, `replaceOne`) - completed.
- `#229`: UTF `runCommand` subset adapter (`ping`, `buildInfo`, `listIndexes`, `count`) - completed.
- `#231`: UTF `clientBulkWrite` subset adapter (ordered single-namespace rewrite to `bulkWrite`) - completed.
- `#104`: aggregate-stage unsupported reduction - remaining.

## Reproduction

Run the same ledger locally:

```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  -Pr3SpecRepoRoot="<path-to-mongodb-specifications>" \
  -Pr3FailureLedgerMongoUri="<replica-set-uri>" \
  -Pr3FailureLedgerFailOnFailures=true \
  r3FailureLedger
```
