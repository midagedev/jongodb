# Node Debug Bundle Collector

Use the debug bundle CLI to collect reproducibility data before opening Node adapter issues.

## Command

```bash
npm run node:debug:bundle -- --outputDir build/reports/node-debug-bundle
```

Optional flags:
- `--env KEY1,KEY2,...` : override selected environment variable list
- `--file <path>` : include additional files (repeatable)
- `--log-file <path>` : include additional log files (repeatable)
- `--outputDir <path>` : custom output directory

## Output

- `bundle.json` : structured diagnostic bundle
- `SUMMARY.md` : concise issue-ready summary

Default collected data includes:
- host/runtime (`platform`, `arch`, `node`)
- git context (`commit`, `branch`, `status`)
- package versions (`jongodb-workspace`, `@jongodb/memory-server`)
- selected env vars (with secret redaction)
- known diagnostic files (if present):
  - `.jongodb/jest-memory-server.json`
  - `testkit/node-compat/reports/node-compat-smoke.json`
  - `build/reports/node-cold-start/report.json`

## Security

Bundle collector applies secret redaction (`<redacted>`) for credential/token/password-like patterns before writing output.
See policy:
- `docs/NODE_LOG_REDACTION_POLICY.md`

## Issue Flow

Use issue template:
- `.github/ISSUE_TEMPLATE/node-memory-server-debug.yml`

Attach both generated files to the issue.
