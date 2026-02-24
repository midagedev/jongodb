# Issue #126: [scorecard][r4][slice-06] Design clientBulkWrite transaction wiring contract

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/126
- Slice key: `slice-06`
- Track: `track:txn`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
