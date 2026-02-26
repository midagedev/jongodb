# Node Launcher Doctor (Preflight Diagnostics)

`node:doctor` checks launcher preflight health for `@jongodb/memory-server`.

## Command

```bash
npm run node:doctor -- --outputDir build/reports/node-doctor
```

Outputs:
- `build/reports/node-doctor/doctor.json`
- `build/reports/node-doctor/doctor.md`

## What It Checks

- Node version baseline (`>=20`)
- memory-server build artifact presence
- `JONGODB_BINARY_PATH` validity (if configured)
- `JONGODB_BINARY_CHECKSUM` format/match against explicit binary path (if configured)
- bundled binary package resolution for current platform/arch
- classpath availability:
  - direct `JONGODB_CLASSPATH`
  - Gradle auto-probe (`gradle -q printLauncherClasspath`)
- aggregate runtime preflight readiness (binary or classpath available)

## Exit Behavior

- exits with non-zero code on failing checks by default
- use `--no-fail true` to always exit zero while still producing reports

## Recommended Usage

Run before filing runtime issues and attach report files.

Related:
- `docs/NODE_DEBUG_BUNDLE.md`
- `docs/NODE_TROUBLESHOOTING_PLAYBOOK.md`
