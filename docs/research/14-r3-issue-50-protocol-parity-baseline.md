# R3 Issue #50: Protocol/Wire Parity Baseline

Date: 2026-02-24
Status: implemented and certified

## Objective

Close protocol and command-surface gaps identified by the R3 failure ledger.

## Baseline Method

1. Bootstrap a local single-node replica set fixture.
2. Check out pinned `mongodb/specifications` commit from `third_party/mongodb-specs/manifest.json`.
3. Run:

```bash
gradle r3FailureLedger \
  -Pr3SpecRepoRoot="third_party/mongodb-specs/.checkout/specifications" \
  -Pr3FailureLedgerMongoUri="<replica-set-uri>"
```

4. Inspect `build/reports/r3-failure-ledger/r3-failure-ledger.json`.

## Baseline Result (early local snapshot)

- `failureCount`: 104
- `byTrack`:
  - `aggregation`: 44
  - `query_update`: 60
  - `protocol`: 0
  - `txn`: 0

## Current Certification Snapshot

From GitHub Actions run `22332937657` (`R3 Failure Ledger`):

- `failureCount`: 0
- `suiteCount`: 3
- `byTrack`: empty
- `byStatus`: empty

## Conclusion for Issue #50

For the current pinned corpus and runner profile, there are no protocol-tagged failures in the failure ledger.
Protocol track remains covered by existing wire/command tests, and parity work is now concentrated on query/update and aggregation tracks.
