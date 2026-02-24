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

## Current Release-Line Notes

- `main` is ahead of `v0.1.2`.
- Next tag candidate should regenerate certification artifacts against the release-candidate commit, not reuse historical tag evidence.

## Tagging Gate

Before creating a release tag:

1. Confirm `CI Test Suite` is green on the release candidate commit.
2. Re-run the three certification workflows above on the release candidate commit.
3. Confirm that `docs/COMPATIBILITY_SCORECARD.md` metrics still match artifacts.
4. Confirm no open P1/P2 compatibility regressions remain.
5. Push the release tag (`git tag -a vX.Y.Z <commit> -m "jongodb X.Y.Z"` then `git push origin vX.Y.Z`).
6. Confirm `.github/workflows/maven-central-release.yml` completed and a GitHub Release exists for the tag.

## Node Adapter Release Gate (Draft)

Use this when publishing `@jongodb/memory-server`:

1. Confirm `Node Adapter Release` workflow verify job is green.
2. Confirm package version is set explicitly (tag `node-vX.Y.Z` or manual input).
3. For manual workflow runs, never set `publish=true` with empty `version` (workflow blocks this).
4. Run `npm publish --dry-run` and review package contents.
5. Publish only when `NPM_TOKEN` is configured and verify npm registry visibility.
6. Update README usage examples with the released package version.

## Release History

| Version | Date (UTC) | Commit | Maven | GitHub Actions |
| --- | --- | --- | --- | --- |
| `0.1.2` | `2026-02-24` | `2ace4a4` | `io.github.midagedev:jongodb:0.1.2` | run `22338690663` |
| `0.1.1` | `2026-02-24` | `f4a8bbb` | `io.github.midagedev:jongodb:0.1.1` | run `22335420545` |
| `0.1.0` | `2026-02-24` | `df774da` | `io.github.midagedev:jongodb:0.1.0` | run `22334177236` |
