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
4. Push the release tag (`git tag -a vX.Y.Z <commit> -m "jongodb X.Y.Z"` then `git push origin vX.Y.Z`).
5. Confirm `.github/workflows/maven-central-release.yml` completed and a GitHub Release exists for the tag.

## Release History

| Version | Date (UTC) | Commit | Maven | GitHub Actions |
| --- | --- | --- | --- | --- |
| `0.1.0` | `2026-02-24` | `df774da` | `io.github.midagedev:jongodb:0.1.0` | run `22334177236` |
