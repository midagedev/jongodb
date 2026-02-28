# Node Compatibility Smoke

Status date: 2026-02-24

This suite validates practical Node ecosystem compatibility on top of `@jongodb/memory-server`:

- official `mongodb` Node driver
- `mongoose`

Current scenario set:
- CRUD baseline
- transaction commit
- transaction rollback

Execution:
- local command: `npm --prefix testkit/node-compat test`
- CI workflow: `.github/workflows/node-compat-smoke.yml`
- recurring guard: scheduled `main` run (`cron: 0 4 * * *`) plus `main` push runs
- report artifact: `testkit/node-compat/reports/node-compat-smoke.json`
- lane summary artifact:
  - `testkit/node-compat/reports/node-compat-smoke-summary-ubuntu-latest-node20.md`
  - `testkit/node-compat/reports/node-compat-smoke-summary-ubuntu-latest-node22.md`
  - `testkit/node-compat/reports/node-compat-smoke-summary-macos-latest-node20.md`
  - `testkit/node-compat/reports/node-compat-smoke-summary-macos-latest-node22.md`

Failure categorization in report:
- `unsupported`: explicit unsupported/not-implemented surface
- `transaction`: transaction flow failures
- `runtime`: generic process/runtime failures
