# @jongodb/memory-server

Node test runtime adapter for [`jongodb`](https://github.com/midagedev/jongodb).

It starts a local `jongodb` launcher process, provides a MongoDB URI, and manages lifecycle for test runners.

## Why This Package Exists

`mongodb-memory-server` is useful, but it still depends on `mongod` bootstrap behavior.

`@jongodb/memory-server` focuses on a different tradeoff:
- faster integration-test startup loops
- deterministic local process lifecycle control
- simple test runner integration (Jest, Vitest, Nest Jest)

This package is for test runtime usage only. It is not a production MongoDB server.

## Value for Integration Test Suites

Observed benchmark summary on one NestJS integration suite shape:
- historical suite-scoped lifecycle: about `55.9%` faster than `mongodb-memory-server`
- current single-boot lifecycle: about `64.2%` faster than `mongodb-memory-server`

Actual deltas vary by test shape, dataset size, and CI machine characteristics.

## Scope

- integration-test runtime helper
- launcher process start/stop/detach lifecycle
- framework-agnostic runtime API
- convenience hooks for Jest, Vitest, and Nest Jest

## Requirements

- Node.js 20+

## Install

```bash
npm i -D @jongodb/memory-server
```

## Module Format

This package supports both:
- ESM (`import`)
- CommonJS (`require`)

## Quick Start

Recommended pattern: one runtime per test process.

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

Runtime behavior:
- `setup()` starts launcher and writes URI to configured env key(s)
- `teardown()` stops launcher and restores previous env values
- overlapping runtimes that share env keys now restore without clobbering each other

CommonJS example:

```js
const { createJongodbEnvRuntime } = require("@jongodb/memory-server/runtime");
const runtime = createJongodbEnvRuntime({ databaseName: "test" });
```

## Migration from `mongodb-memory-server`

Basic Jest migration:

Before:

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

After:

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({ databaseName: "test" });

beforeAll(async () => {
  await runtime.setup();
});

afterAll(async () => {
  await runtime.teardown();
});
```

Option mapping:
- `MongoMemoryServer.create({ instance: { port } })` -> `createJongodbEnvRuntime({ port })`
- `MongoMemoryServer.create({ instance: { dbName } })` -> `createJongodbEnvRuntime({ databaseName })`
- `mongod.getUri()` -> `runtime.uri` (after `setup`) or configured env var
- `mongod.stop()` -> `runtime.teardown()`

Parallel test tip:
- set `databaseNameStrategy: "worker"` to isolate worker data by database name

## Runner Integrations

Jest (per-file hooks):

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForJest } from "@jongodb/memory-server/jest";

registerJongodbForJest({ beforeAll, afterAll });
```

Jest (global setup/teardown):

`jest.global-setup.ts`

```ts
import { createJestGlobalSetup } from "@jongodb/memory-server/jest";

export default createJestGlobalSetup();
```

`jest.global-teardown.ts`

```ts
import { createJestGlobalTeardown } from "@jongodb/memory-server/jest";

export default createJestGlobalTeardown();
```

Vitest:

```ts
import { beforeAll, afterAll } from "vitest";
import { registerJongodbForVitest } from "@jongodb/memory-server/vitest";

registerJongodbForVitest({ beforeAll, afterAll });
```

NestJS (Jest E2E):

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForNestJest } from "@jongodb/memory-server/nestjs";

registerJongodbForNestJest({ beforeAll, afterAll });
```

Common patterns:
- NestJS + Mongoose: use `process.env.MONGODB_URI` in `forRootAsync`
- Prisma (Mongo): set runtime `envVarNames` to include `DATABASE_URL`
- TypeORM (Mongo): pass runtime URI into `url`

## Runtime Resolution

`launchMode` values:
- `auto` (default): binary first, Java fallback
- `binary`: binary only
- `java`: Java only

Binary resolution order:
1. `options.binaryPath`
2. `JONGODB_BINARY_PATH`
3. bundled platform binary package

Bundled targets:
- `@jongodb/memory-server-bin-darwin-arm64`
- `@jongodb/memory-server-bin-linux-x64-gnu`
- `@jongodb/memory-server-bin-win32-x64`

Java fallback:
- set `classpath` option or `JONGODB_CLASSPATH`

Launcher contract:
- stdout ready line: `JONGODB_URI=mongodb://...`
- optional stderr failure line: `JONGODB_START_FAILURE=...`
- process stays alive until termination signal

## API Surface

Main export (`@jongodb/memory-server`):
- `startJongodbMemoryServer(options?)`

Runtime export (`@jongodb/memory-server/runtime`):
- `createJongodbEnvRuntime(options?)`

Jest export (`@jongodb/memory-server/jest`):
- `registerJongodbForJest(hooks, options?)`
- `createJestGlobalSetup(options?)`
- `createJestGlobalTeardown(options?)`
- `readJestGlobalState(options?)`
- `readJestGlobalUri(options?)`

Nest export (`@jongodb/memory-server/nestjs`):
- `registerJongodbForNestJest(hooks, options?)`

Vitest export (`@jongodb/memory-server/vitest`):
- `registerJongodbForVitest(hooks, options?)`

## Options Reference

Core:
- `launchMode`: `auto` | `binary` | `java` (default: `auto`)
- `binaryPath`: binary executable override path
- `classpath`: Java classpath string or string array
- `javaPath`: Java executable path (default: `java`)
- `launcherClass`: Java launcher class (default: `org.jongodb.server.TcpMongoServerLauncher`)
- `topologyProfile`: `standalone` | `singleNodeReplicaSet` (default: `standalone`)
- `replicaSetName`: replica-set name for `singleNodeReplicaSet` profile (default: `jongodb-rs0`)
- `databaseName`: base DB name (default: `test`)
- `databaseNameSuffix`: suffix appended to `databaseName` (example: `_ci`)
- `databaseNameStrategy`: `static` | `worker` (default: `static`)
- `host`: bind host (default: `127.0.0.1`)
- `port`: bind port (`0` means ephemeral)
- `startupTimeoutMs`: startup timeout (default: `15000`)
- `stopTimeoutMs`: stop timeout before forced kill (default: `5000`)
- `env`: extra child process environment variables
- `logLevel`: `silent` | `info` | `debug` (default: `silent`)

Runtime helper options:
- `envVarName`: single target env key (default: `MONGODB_URI`)
- `envVarNames`: multiple target env keys (example: `["MONGODB_URI", "DATABASE_URL"]`)
- `envTarget`: optional scoped env object (default: `process.env`)

Replica-set profile example:

```ts
const runtime = createJongodbEnvRuntime({
  topologyProfile: "singleNodeReplicaSet",
  replicaSetName: "rs-test",
});
```

Replica-set contract test coverage:
- URI must include `?replicaSet=<name>` when `topologyProfile=singleNodeReplicaSet`
- `hello` response must expose `setName`, `hosts`, `primary`, `isWritablePrimary`, `topologyVersion`

Run Node adapter contract tests:

```bash
npm run --workspace @jongodb/memory-server test
```

Scoped env example:

```ts
const scopedEnv: Record<string, string | undefined> = {};
const runtime = createJongodbEnvRuntime({
  envVarName: "MONGODB_URI",
  envTarget: scopedEnv,
});
```

## Troubleshooting

- `No launcher runtime configured`: set `binaryPath` / `JONGODB_BINARY_PATH` / `classpath` / `JONGODB_CLASSPATH`
- `Binary launch mode requested but no binary was found`: provide `binaryPath` or `JONGODB_BINARY_PATH`
- `Java launch mode requested but Java classpath is not configured`: provide `classpath` or `JONGODB_CLASSPATH`
- `spawn ... ENOENT`: missing runtime executable path
- startup timeout: launcher did not emit `JONGODB_URI=...`

Parallel test tip:
- use `databaseNameStrategy: "worker"` or per-worker suffixes to prevent data collisions
- if multiple runtimes share a Node process, prefer `envTarget` for per-runtime scoped bindings
