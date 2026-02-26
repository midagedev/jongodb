# Release Checklist

Status date: 2026-02-25

## R3 Certification Sign-Off

- [x] Official suite sharded run on `main` completed with zero mismatch/error.
  - run id: `22332998372`
- [x] R3 failure ledger run on `main` completed with `failureCount=0`.
  - run id: `22332937657`
- [x] External canary certification completed for 3 projects with rollback success.
  - run id: `22332937633`
- [x] Complex-query certification gate completed on `main` with no supported-subset regressions.
  - workflow: `.github/workflows/complex-query-certification.yml`
  - run id: `22377660027`
- [x] Support boundary documents are updated.
  - `docs/SUPPORT_MATRIX.md`
  - `docs/COMPATIBILITY_SCORECARD.md`
- [x] README certification snapshot is updated and links current evidence docs.

## Current Release-Line Notes

- Latest Java release tag: `v0.1.5` (run `22379241855`, commit `868bbc7`).
- Latest Node adapter tag: `node-v0.1.4` (run `22379340586`, commit `868bbc7`).
- Next tag candidate should regenerate certification artifacts against the release-candidate commit, not reuse historical tag evidence.

## Tagging Gate

Before creating a release tag:

1. Confirm `CI Test Suite` is green on the release candidate commit.
2. Re-run certification workflows (Official Suite Sharded, R3 Failure Ledger, R3 External Canary, Complex Query Certification) on the release candidate commit.
3. Confirm that `docs/COMPATIBILITY_SCORECARD.md` metrics still match artifacts.
4. Confirm no open P1/P2 compatibility regressions remain.
5. Push the release tag (`git tag -a vX.Y.Z <commit> -m "jongodb X.Y.Z"` then `git push origin vX.Y.Z`).
6. Confirm `.github/workflows/maven-central-release.yml` completed and a GitHub Release exists for the tag.

## Node Adapter Release Gate (Draft)

Use this when publishing `@jongodb/memory-server`:

1. Confirm `Node Adapter Release` workflow verify job is green.
2. Confirm package version is set explicitly (tag `node-vX.Y.Z` or manual input).
3. Confirm version bump rationale aligns with `docs/NODE_SEMVER_POLICY.md` (patch/minor/major classification).
4. Confirm npm scope ownership for `@jongodb` and valid `NPM_TOKEN` in GitHub repository secrets.
5. Confirm GraalVM native-image is available in workflow runners (linux, macOS, windows) for binary jobs.
6. Confirm platform binary packages publish first:
   - `@jongodb/memory-server-bin-linux-x64-gnu`
   - `@jongodb/memory-server-bin-darwin-arm64`
   - `@jongodb/memory-server-bin-win32-x64`
7. Confirm core package publishes after binaries with synced optional dependency versions.
8. For manual workflow runs, never set `publish=true` with empty `version` (workflow blocks this).
9. Confirm workflow packs tarball artifacts first (binary/core) and publishes from those packed files.
10. Confirm GitHub Artifact Attestations are generated for packed tarballs (`actions/attest-build-provenance`).
11. Run `npm publish --dry-run` and review package contents.
12. Publish only when `NPM_TOKEN` is configured and verify npm registry visibility.
13. Update README usage examples with the released package version.

## Node Canary Channel Gate (Draft)

Use this when validating `@jongodb/memory-server` canary automation:

1. Confirm `Node Adapter Canary Release` workflow is configured on `main` push and `workflow_dispatch`.
2. Confirm generated version format includes canary suffix (`<base>-canary.<UTC timestamp>.<short sha>`).
3. Confirm workflow packs the npm tarball, attests it (`actions/attest-build-provenance`), then publishes from that tarball.
4. Confirm publish uses npm dist-tag `canary` (never `latest`).
5. Confirm dry-run path works without publish credentials (`publish=false` in manual run).

## Release History

| Version | Date (UTC) | Commit | Maven | GitHub Actions |
| --- | --- | --- | --- | --- |
| `0.1.5` | `2026-02-25` | `868bbc7` | `io.github.midagedev:jongodb:0.1.5` | run `22379241855` |
| `0.1.4` | `2026-02-24` | `e6f6bbf` | `io.github.midagedev:jongodb:0.1.4` | run `22355417610` |
| `0.1.3` | `2026-02-24` | `61cfa03` | `io.github.midagedev:jongodb:0.1.3` | run `22341381717` |
| `0.1.2` | `2026-02-24` | `2ace4a4` | `io.github.midagedev:jongodb:0.1.2` | run `22338690663` |
| `0.1.1` | `2026-02-24` | `f4a8bbb` | `io.github.midagedev:jongodb:0.1.1` | run `22335420545` |
| `0.1.0` | `2026-02-24` | `df774da` | `io.github.midagedev:jongodb:0.1.0` | run `22334177236` |
