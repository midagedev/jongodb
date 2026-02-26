# Node Migration Guide: `mongodb-memory-server` -> `@jongodb/memory-server`

This guide helps existing `mongodb-memory-server` test suites migrate to `@jongodb/memory-server` with minimal friction.

## 1. Install

```bash
npm i -D @jongodb/memory-server
```

For Java fallback mode, make sure `JONGODB_CLASSPATH` is resolvable in CI/local environments.

## 2. Replace Lifecycle Wiring

Before (`mongodb-memory-server`):

```ts
import { MongoMemoryServer } from "mongodb-memory-server";

let mongod: MongoMemoryServer;

beforeAll(async () => {
  mongod = await MongoMemoryServer.create();
  process.env.MONGODB_URI = mongod.getUri();
});

afterAll(async () => {
  await mongod.stop();
});
```

After (`@jongodb/memory-server`):

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  databaseName: "test",
  databaseNameStrategy: "worker",
  envVarNames: ["MONGODB_URI", "DATABASE_URL"],
});

beforeAll(async () => {
  await runtime.setup();
});

afterAll(async () => {
  await runtime.teardown();
});
```

## 3. Option Mapping

| mongodb-memory-server | @jongodb/memory-server |
| --- | --- |
| `MongoMemoryServer.create({ instance: { port } })` | `createJongodbEnvRuntime({ port })` |
| `MongoMemoryServer.create({ instance: { dbName } })` | `createJongodbEnvRuntime({ databaseName })` |
| `mongod.getUri()` | `runtime.uri` (after `setup`) or env keys |
| `mongod.stop()` | `runtime.teardown()` |

## 4. Runner-Specific Migration

Jest (file hooks):

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForJest } from "@jongodb/memory-server/jest";

registerJongodbForJest({ beforeAll, afterAll });
```

Jest (global setup/teardown):

```ts
// jest.global-setup.ts
import { createJestGlobalSetup } from "@jongodb/memory-server/jest";
export default createJestGlobalSetup();
```

```ts
// jest.global-teardown.ts
import { createJestGlobalTeardown } from "@jongodb/memory-server/jest";
export default createJestGlobalTeardown();
```

Vitest:

```ts
import { beforeAll, afterAll } from "vitest";
import { registerJongodbForVitest } from "@jongodb/memory-server/vitest";

registerJongodbForVitest({ beforeAll, afterAll });
```

Nest Jest:

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForNestJest } from "@jongodb/memory-server/nestjs";

registerJongodbForNestJest({ beforeAll, afterAll });
```

## 5. Parallel Test Suites

- Prefer `databaseNameStrategy: "worker"` for worker-level isolation.
- Use `envVarNames` if your stack expects both `MONGODB_URI` and `DATABASE_URL`.
- For Vitest workspaces, use `registerJongodbForVitestWorkspace` with project isolation.

## 6. Launch Mode Selection

- Default (`launchMode: "auto"`): bundled binary first, Java fallback.
- Force binary mode:
  - `launchMode: "binary"`
- Force Java mode:
  - `launchMode: "java"` and provide `classpath` or `JONGODB_CLASSPATH`.

## 7. Migration Validation Checklist

1. Replace all `MongoMemoryServer.create` and `stop` lifecycle code.
2. Confirm test env keys are set by runtime (`MONGODB_URI`, `DATABASE_URL` as needed).
3. Confirm CI can resolve binary or `JONGODB_CLASSPATH`.
4. Run node compatibility smoke suite:
   - `npm run node:build`
   - `JONGODB_CLASSPATH="$(./.tooling/gradle-8.10.2/bin/gradle --no-daemon -q printLauncherClasspath | tail -n 1)" npm --prefix testkit/node-compat test`
5. Verify no stale global setup state files remain in `.jongodb/` for migrated projects.
