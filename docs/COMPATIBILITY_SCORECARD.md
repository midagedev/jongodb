# Compatibility Scorecard

Status date: 2026-02-28

This scorecard tracks integration-test compatibility against MongoDB official specs.
It is not a production MongoDB parity claim.

## Scope

- Historical baseline: Official Suite Sharded run `22339640229`.
- Current target: `origin/main` commit `d2ba61b`.
- Primary objective: increase imported differential coverage while keeping mismatch/error at zero.

## Evidence Sources

| Evidence | Source run / artifact | Result |
| --- | --- | --- |
| Official UTF sharded differential (current) | GitHub Actions `Official Suite Sharded` run `22516323868`, artifact `utf-shard-summary/utf-shard-summary.md` | PASS (`total=508`, `match=508`, `mismatch=0`, `error=0`) |
| R3 failure ledger (current) | GitHub Actions `R3 Failure Ledger` run `22516324202`, artifact `r3-failure-ledger/r3-failure-ledger.json` | PASS (`failureCount=0`) |
| Complex-query certification (canonical pack) | GitHub Actions `Complex Query Certification` run `22516137734`, artifact `complex-query-certification/complex-query-certification.json` | PASS (`packVersion=complex-query-pack-v3`, `mismatchCount=0`, `unsupportedByPolicyCount=0`) |
| External canary certification (latest success) | GitHub Actions `R3 External Canary Certification` run `22378993613` | PASS (3-project canary set) |
| Release-readiness streak | `r3-release-readiness-streak.json` from run `22516324202` | Not yet satisfied (`minStreak=3`, counters `0/1`) |

## Baseline vs Current

| Metric | Baseline (`22339640229`) | Current (`22516324202`) | Delta |
| --- | --- | --- | --- |
| imported | 200 | 508 | +308 |
| skipped | 567 | 851 | +284 |
| unsupported | 814 | 222 | -592 |
| total differential cases | 200 | 508 | +308 |
| match | 146 | 508 | +362 |
| mismatch | 54 | 0 | -54 |
| error | 0 | 0 | 0 |

## Current R3 Ledger Snapshot

| Suite | Imported | Skipped | Unsupported | Mismatch | Error |
| --- | --- | --- | --- | --- | --- |
| `crud-unified` | 328 | 700 | 44 | 0 | 0 |
| `transactions-unified` | 164 | 127 | 140 | 0 | 0 |
| `sessions` | 16 | 24 | 38 | 0 | 0 |

Current ledger gate status:

- `failureCount=0`
- `byTrack={}`
- `byStatus={}`

## Complex Query Snapshot

From run `22516137734`:

| Metric | Value |
| --- | --- |
| packVersion | `complex-query-pack-v3` |
| totalPatterns | 24 |
| supportedPatterns | 17 |
| supportedPass | 17 |
| supportedPassRate | 1.0 |
| mismatchCount | 0 |
| errorCount | 0 |
| unsupportedByPolicyCount | 0 |
| unsupportedDeltaCount | 0 |

Recent subset closures reflected in this snapshot:
- `#396`: `$expr.$add` certification subset.
- `#397`: minimal `$graphLookup` certification subset.

## Release-Readiness Streak Snapshot

From run `22516324202` artifact `r3-release-readiness-streak.json`:

- `threshold.minStreak=3`
- `officialZeroMismatchStreak=0`
- `r3LedgerZeroFailureStreak=1`
- `readiness.satisfied=false`

Note:
- Streak history parsing currently reports `artifact-read-error: HTTP Error 401` for some schedule-history artifact reads, which keeps `officialZeroMismatchStreak` conservative.

## Policy Exclusions

- `failPoint` is a policy exclusion in strict profile for deterministic in-process execution.
- Compat profile allows only failpoint-disable subset and keeps other modes explicit unsupported.
- Scorecard accounting distinguishes strict-profile policy exclusions as `unsupported-by-policy UTF operation: failPoint`.

## Gap-to-Issue Mapping

Completed compatibility-expansion issues:

- `#395`: listLocalSessions regression pack and importer normalization - completed.
- `#396`: `$expr.$add` certification subset - completed.
- `#397`: `$graphLookup` minimal subset for certification - completed.
- `#398`: release-readiness streak tracking and summary artifacts - completed.
- `#100`, `#101`, `#229`, `#231`, `#232`, `#233`, `#234`, `#235`, `#236`, `#238`, `#239`, `#240`, `#241`, `#242`, `#243`, `#245`, `#265`, `#266`, `#267`, `#269` - completed.

Remaining track:

- `#104`: aggregate-stage unsupported reduction (remaining focus: `$merge` and advanced non-alias stages).

## Reproduction

Run the same ledger locally:

```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  -Pr3SpecRepoRoot="<path-to-mongodb-specifications>" \
  -Pr3FailureLedgerMongoUri="<replica-set-uri>" \
  -Pr3FailureLedgerFailOnFailures=true \
  r3FailureLedger
```

Run complex-query certification locally:

```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon \
  -PcomplexQueryMongoUri="<replica-set-uri>" \
  -PcomplexQueryFailOnGate=true \
  complexQueryCertificationEvidence
```
