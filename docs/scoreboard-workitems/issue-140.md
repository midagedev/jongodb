# Issue #140: [scorecard][r4][slice-20] Define arrayFilters matcher binding model

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/140
- Slice key: `slice-20`
- Track: `track:engine`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
