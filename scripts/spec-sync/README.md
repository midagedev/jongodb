# MongoDB Spec Sync Tooling

This folder provides deterministic pinning/sync tooling for the
`mongodb/specifications` repository.

## Commands

Sync a pinned commit and update manifest metadata:

```bash
./scripts/spec-sync/sync-mongodb-specs.sh \
  --commit 99704fa8860777da1d919ef765af1e41e75f5859
```

Verify manifest/checkouts for CI or local checks:

```bash
./scripts/spec-sync/verify-mongodb-specs.sh
```

## Notes

- The default checkout is local-only at
  `third_party/mongodb-specs/.checkout/specifications`.
- Full specs are intentionally not vendored by default.
- The manifest is written to `third_party/mongodb-specs/manifest.json`.
