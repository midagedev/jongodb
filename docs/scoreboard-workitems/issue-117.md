# Issue #117: [scorecard][r4][slice-01] Define clientBulkWrite operation contract for unified runner

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/117
- Slice key: `slice-01`
- Track: `track:protocol`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
