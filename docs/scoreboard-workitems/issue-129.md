# Issue #129: [scorecard][r4][slice-09] Define targetedFailPoint policy normalization

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/129
- Slice key: `slice-09`
- Track: `track:protocol`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
