# Issue #131: [scorecard][r4][slice-11] Define runCommand subset for ping/buildInfo

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/131
- Slice key: `slice-11`
- Track: `track:protocol`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
