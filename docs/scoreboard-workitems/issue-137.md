# Issue #137: [scorecard][r4][slice-17] Define findOneAndDelete base operation path

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/137
- Slice key: `slice-17`
- Track: `track:engine`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```
