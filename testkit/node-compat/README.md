# Node Compatibility Smoke Suite

Practical compatibility smoke suite for:

- official Node `mongodb` driver
- `express` route integration with MongoDB driver
- `koa` route integration with MongoDB driver
- `mongoose` (CRUD + transaction/session)
- `typeorm` MongoDB repository CRUD
- `prisma` MongoDB provider CRUD

The suite runs against `jongodb` started through the Node adapter runtime and writes a JSON report.

## Run

1. Build Node adapter package:

```bash
npm run node:build
```

2. Resolve Java classpath:

```bash
export JONGODB_CLASSPATH="$(./.tooling/gradle-8.10.2/bin/gradle --no-daemon -q printLauncherClasspath | tail -n 1)"
```

3. Execute smoke suite:

```bash
npm --prefix testkit/node-compat ci
npm --prefix testkit/node-compat test
```

`test` script generates Prisma client from `testkit/node-compat/prisma/schema.prisma` before running scenarios.

Report output:

- `testkit/node-compat/reports/node-compat-smoke.json`
