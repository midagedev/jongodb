# Issue #133: [scorecard][r4][slice-13] Define runCommand subset for listIndexes and collStats

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/133
- Slice key: `slice-13`
- Track: `track:protocol`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
