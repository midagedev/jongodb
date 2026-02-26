# Node Adapter Semver and Compatibility Policy

Applies to:
- `@jongodb/memory-server`
- `@jongodb/memory-server-bin-*` optional binary packages

## Semver Rules

1. Stable releases follow SemVer (`MAJOR.MINOR.PATCH`).
2. `PATCH` releases:
   - bug fixes and internal hardening
   - no intended public API/behavior break
3. `MINOR` releases:
   - backward-compatible API additions
   - new integrations, options, diagnostics, and non-breaking defaults
4. `MAJOR` releases:
   - breaking public API changes
   - behavior changes that require user migration

## Pre-Release Channels

- Canary channel publishes with npm dist-tag `canary`.
- Canary version format:
  - `<base>-canary.<UTC timestamp>.<short sha>`
- Canary builds are for early feedback and may change without deprecation windows.

## Compatibility Contract

- Runtime baseline:
  - Node.js `20+`
- Launch modes:
  - binary launch when bundled platform binary exists
  - Java fallback (`JONGODB_CLASSPATH`) remains supported
- Module contract:
  - ESM and CommonJS both supported for the public runtime API
- Framework integration scope:
  - Jest / Vitest / Nest Jest helpers
  - compatibility smoke coverage for mongodb driver, express, koa, mongoose, prisma, typeorm

## Core vs Binary Package Versioning

- Stable release line:
  - core package and platform binary packages are version-aligned for the same release tag.
- Canary line:
  - canary publish currently targets `@jongodb/memory-server` with `canary` dist-tag.
  - optional binary dependencies may stay pinned to the latest stable release unless a dedicated canary binary publish is introduced.

## Deprecation and Removal Policy

1. Deprecations are announced in `CHANGELOG.md` and release notes before removal.
2. Deprecated public options should remain available for at least one minor release cycle, unless a security or correctness issue requires immediate removal.
3. Breaking removals must include migration notes in docs and release notes.

## Release Safety Gates

Before publishing stable Node adapter releases:
- pass Node adapter verify checks (`node:build`, `node:typecheck`, smoke workflow)
- publish/verify attested tarball artifacts in release workflow
- confirm compatibility and release checklist documents are current

Related docs:
- `docs/RELEASE_CHECKLIST.md`
- `docs/NODE_COMPAT_SMOKE.md`
- `packages/memory-server/README.md`
