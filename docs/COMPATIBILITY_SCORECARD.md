# Compatibility Scorecard

Status date: 2026-02-24

This scorecard tracks integration-test compatibility against MongoDB official specs.
It is not a production MongoDB parity claim.

## Scope

- Baseline target: `v0.1.2` and current `main` evidence.
- Primary objective: increase imported differential coverage while keeping behavior deterministic.

## Evidence Sources

| Evidence | Source run / artifact | Result |
| --- | --- | --- |
| Official UTF sharded differential | GitHub Actions `Official Suite Sharded` run `22339640229` | PASS |
| R3 failure ledger | GitHub Actions `R3 Failure Ledger` run `22339640205` | FAIL (gate reports remaining mismatches) |
| Real mongod baseline | GitHub Actions `Real Mongod Baseline` run `22339640211` | PASS |

## Frozen Baseline (Run `22339640229`)

Aggregated baseline metrics from shard artifacts:

| Metric | Value |
| --- | --- |
| imported | 200 |
| skipped | 567 |
| unsupported | 814 |
| total differential cases | 200 |
| match | 146 |
| mismatch | 54 |
| error | 0 |

## R3 Ledger Snapshot (Run `22339640205`)

| Suite | Imported | Unsupported | Mismatch | Error |
| --- | --- | --- | --- | --- |
| `crud-unified` | 188 | 362 | 54 | 0 |
| `transactions-unified` | 0 | 396 | 0 | 0 |
| `sessions` | 12 | 56 | 0 | 0 |

Current ledger gate status:

- `failureCount=54` (all `MISMATCH`, no `ERROR`)
- primary mismatch bucket: `bulkWrite` protocol scenarios

## Top Unsupported Reasons (Baseline)

| Reason | Count |
| --- | --- |
| `unsupported UTF operation: startTransaction` | 234 |
| `unsupported UTF operation: failPoint` | 134 |
| `unsupported UTF operation: clientBulkWrite` | 86 |
| `unsupported UTF operation: findOneAndUpdate` | 48 |
| `unsupported UTF operation: createEntities` | 40 |
| `unsupported UTF operation: findOneAndReplace` | 36 |
| `unsupported UTF aggregate stage in pipeline` | 28 |
| `unsupported UTF update option: arrayFilters` | 26 |
| `unsupported UTF operation: countDocuments` | 24 |
| `unsupported UTF operation: replaceOne` | 22 |

## Policy Exclusions

- `failPoint` is currently treated as a policy exclusion for deterministic in-process execution.
- Scorecard accounting distinguishes this as `unsupported-by-policy UTF operation: failPoint` in importer output.

## Gap-to-Issue Mapping

- `#100`: transaction operation adapter coverage (`startTransaction` and lifecycle wiring)
- `#101`: unified CRUD adapter coverage (`findOneAndUpdate`, `findOneAndReplace`, `countDocuments`, `replaceOne`)
- `#102`: `createEntities` subset for official suites
- `#103`: explicit `failPoint` policy for deterministic in-memory backend
- `#104`: aggregate-stage unsupported reduction

## Reproduction

Run the same ledger locally:

```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  -Pr3SpecRepoRoot="<path-to-mongodb-specifications>" \
  -Pr3FailureLedgerMongoUri="<replica-set-uri>" \
  -Pr3FailureLedgerFailOnFailures=true \
  r3FailureLedger
```
