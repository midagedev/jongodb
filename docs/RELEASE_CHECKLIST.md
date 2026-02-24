# Release Checklist

Status date: 2026-02-24

## R3 Certification Sign-Off

- [x] Official suite sharded run on `main` completed with zero mismatch/error.
  - run id: `22332998372`
- [x] R3 failure ledger run on `main` completed with `failureCount=0`.
  - run id: `22332937657`
- [x] External canary certification completed for 3 projects with rollback success.
  - run id: `22332937633`
- [x] Support boundary documents are updated.
  - `docs/SUPPORT_MATRIX.md`
  - `docs/COMPATIBILITY_SCORECARD.md`
- [x] README certification snapshot is updated and links current evidence docs.

## Tagging Gate

Before creating a release tag:

1. Re-run the three CI workflows above on the release candidate commit.
2. Confirm that `docs/COMPATIBILITY_SCORECARD.md` metrics still match artifacts.
3. Confirm no open P1/P2 compatibility regressions remain.
4. Tag and publish only after all checks are green.

