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
- report artifact: `testkit/node-compat/reports/node-compat-smoke.json`

Failure categorization in report:
- `unsupported`: explicit unsupported/not-implemented surface
- `transaction`: transaction flow failures
- `runtime`: generic process/runtime failures
