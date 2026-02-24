# Issue #134: [scorecard][r4][slice-14] Define count command compatibility wrapper

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/134
- Slice key: `slice-14`
- Track: `track:engine`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
