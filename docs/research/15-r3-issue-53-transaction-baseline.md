# R3 Issue #53: Session/Transaction Baseline and Unsupported Surface

Date: 2026-02-24
Status: implemented baseline; advanced coverage intentionally partial

## Objective

Track transaction/session parity status from the R3 failure-ledger runner and publish the currently unsupported
surface explicitly.

## Baseline Method

1. Bootstrap a local replica set fixture.
2. Check out pinned `mongodb/specifications` commit.
3. Run:

```bash
gradle r3FailureLedger \
  -Pr3SpecRepoRoot="third_party/mongodb-specs/.checkout/specifications" \
  -Pr3FailureLedgerMongoUri="<replica-set-uri>"
```

4. Inspect `build/reports/r3-failure-ledger/r3-failure-ledger.json` suite summaries.

## Baseline Snapshot (pinned corpus)

- `transactions-unified` suite summary:
  - `imported`: 0
  - `mismatch`: 0
  - `error`: 0
  - `unsupported`: 396
  - `skipped`: 35

Certification reference:
- GitHub Actions run `22332937657` (`R3 Failure Ledger`)
- artifact: `r3-failure-ledger/r3-failure-ledger.json`

## Conclusion for Issue #53

- Transaction-tagged mismatches are zero for the currently imported transaction/session subset.
- The transaction unified corpus currently lands mostly in unsupported paths and is explicitly tracked via
  `unsupported` counts in suite summaries.
- Follow-up implementation work should focus on reducing `transactions-unified.unsupported` while preserving
  zero mismatch/error for supported cases.
