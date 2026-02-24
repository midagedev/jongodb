# Issue #138: [scorecard][r4][slice-18] Define findOneAndDelete option parity subset

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/138
- Slice key: `slice-18`
- Track: `track:engine`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
