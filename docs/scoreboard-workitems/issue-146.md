# Issue #146: [scorecard][r4][slice-26] Define aggregation replaceRoot and replaceWith subset

Status: in progress

## Metadata
- Issue: https://github.com/midagedev/jongodb/issues/146
- Slice key: `slice-26`
- Track: `track:engine`

## Objective
Define an isolated execution slice that contributes to reducing unsupported surface in the compatibility scorecard.

## Planned Validation
```bash
./.tooling/gradle-8.10.2/bin/gradle --no-daemon r3FailureLedger
```

## Review Follow-up
- Added explicit rollback note and deterministic validation reminder.
- Rollback: revert the merge commit if this workitem slice is superseded.

Status: reviewed and ready to merge
