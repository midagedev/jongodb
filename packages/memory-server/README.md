# @jongodb/memory-server

Node test runtime adapter for `jongodb`.

It starts a local `jongodb` launcher process, exposes a MongoDB URI, and manages process lifecycle for test runners.

## Scope

- integration-test runtime only
- launcher lifecycle management (start/stop/detach)
- framework-agnostic base helper
- Jest/Vitest/Nest Jest convenience helpers

This package is not a production MongoDB server.

## Why This vs Embedded MongoDB (`mongodb-memory-server`)

`@jongodb/memory-server` can run in native binary mode instead of booting a full `mongod` test binary.

Practical effects:

- no `mongod` binary download/extract step in cold environments
- lower launcher startup overhead in many integration-test loops
- fewer environment issues tied to `mongod` binary provisioning

Rough performance expectation (not a guarantee):

- cold CI/dev machine: often much faster because there is no `mongod` artifact fetch/unpack
- warm cache environment: startup-phase wins are usually smaller

Observed benchmark (NestJS integration suite subset, duplicated for load):

- workload: 9 suites / 102 test cases (`--runInBand`), same machine
- `mongodb-memory-server` real time:
  - run1: `28.17s`
  - run2: `27.19s`
  - average: `27.68s`
- `jongodb` native binary real time:
  - run1: `12.35s`
  - run2: `12.04s`
  - average: `12.20s`

In this benchmark, native binary mode was about `55.9%` faster (`15.48s` less wall clock).

Your actual delta depends on test shape, I/O, and query workload.

## Requirements

- Node.js 20+
- one launcher runtime:
  - native binary, or
  - Java 17+ with `jongodb` classpath

## Install

```bash
npm i -D @jongodb/memory-server
```

## Quick Start (Recommended)

Use `createJongodbEnvRuntime` and keep one runtime per test process.

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI",
  databaseName: "test"
});

beforeAll(async () => {
  await runtime.setup();
});

afterAll(async () => {
  await runtime.teardown();
});
```

Behavior:

- `setup()` starts launcher and writes `process.env[envVarName]`
- `teardown()` stops launcher and restores previous env value

## Launcher Modes

`launchMode` values:

- `auto` (default): binary first, Java fallback
- `binary`: binary only, fail fast if binary not resolvable
- `java`: Java only, fail fast if classpath not configured

In `auto` mode, launch failures are attempted in order and reported with source tags.

## Binary Runtime

Binary resolution order:

1. `options.binaryPath`
2. `JONGODB_BINARY_PATH`
3. bundled platform package (if installed)

Current bundled package targets:

- `@jongodb/memory-server-bin-darwin-arm64`
- `@jongodb/memory-server-bin-linux-x64-gnu`
- `@jongodb/memory-server-bin-win32-x64`

Version policy:

- binary package versions are pinned to the core package version (`optionalDependencies`)
- if a bundled binary is not installed/available for your version, `auto` mode can still run with Java fallback

Binary-only example:

```ts
import { startJongodbMemoryServer } from "@jongodb/memory-server";

const server = await startJongodbMemoryServer({
  launchMode: "binary",
  binaryPath: process.env.JONGODB_BINARY_PATH
});
```

## Java Runtime

Provide classpath by:

1. `startJongodbMemoryServer({ classpath: "..." })`, or
2. `JONGODB_CLASSPATH`

Repository helper command (when using this repo launcher build):

```bash
./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath
```

## Standalone Launcher Contract

Binary and Java launchers follow one contract:

- readiness line on stdout: `JONGODB_URI=mongodb://...`
- optional failure line on stderr: `JONGODB_START_FAILURE=...`
- process remains alive until terminated by signal (`SIGTERM`/`SIGINT`)

The adapter relies on this contract for deterministic startup/teardown handling.

## Runner Helpers

### Jest (per-file hooks)

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForJest } from "@jongodb/memory-server/jest";

registerJongodbForJest({ beforeAll, afterAll }, {
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI"
});
```

### Jest (global setup/teardown)

`jest.global-setup.ts`:

```ts
import { createJestGlobalSetup } from "@jongodb/memory-server/jest";

export default createJestGlobalSetup({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI"
});
```

`jest.global-teardown.ts`:

```ts
import { createJestGlobalTeardown } from "@jongodb/memory-server/jest";

export default createJestGlobalTeardown();
```

### Vitest

```ts
import { beforeAll, afterAll } from "vitest";
import { registerJongodbForVitest } from "@jongodb/memory-server/vitest";

registerJongodbForVitest({ beforeAll, afterAll }, {
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI"
});
```

### NestJS (Jest E2E)

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForNestJest } from "@jongodb/memory-server/nestjs";

registerJongodbForNestJest({ beforeAll, afterAll }, {
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI"
});
```

## Common Integration Patterns

- NestJS + Mongoose: `MongooseModule.forRootAsync({ useFactory: () => ({ uri: process.env.MONGODB_URI }) })`
- Express/Fastify: inject `process.env.MONGODB_URI` into DB bootstrap before app init
- Prisma (Mongo): set `envVarName: "DATABASE_URL"` for runtime helper
- TypeORM (Mongo): pass runtime URI as `url`

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

Core options:

- `launchMode`: `auto` | `binary` | `java` (default: `auto`)
- `binaryPath`: binary executable path
- `classpath`: Java classpath string or string array
- `javaPath`: Java executable path (default: `java`)
- `launcherClass`: Java launcher class (default: `org.jongodb.server.TcpMongoServerLauncher`)
- `databaseName`: default DB in generated URI (default: `test`)
- `host`: bind host (default: `127.0.0.1`)
- `port`: bind port (`0` for ephemeral, default: `0`)
- `startupTimeoutMs`: startup timeout (default: `15000`)
- `stopTimeoutMs`: stop timeout before forced kill (default: `5000`)
- `env`: additional child process env vars
- `logLevel`: `silent` | `info` | `debug` (default: `silent`)
- `envVarName` (runtime/jest/vitest helpers): target env key (default: `MONGODB_URI`)

## Troubleshooting

- `No launcher runtime configured`: set binary path/env or classpath/env
- `Binary launch mode requested but no binary was found`: provide `binaryPath` or `JONGODB_BINARY_PATH`
- `Java launch mode requested but Java classpath is not configured`: provide `classpath` or `JONGODB_CLASSPATH`
- `spawn ... ENOENT`: missing binary/java executable path
- startup timeout: launcher did not emit `JONGODB_URI=...`

Parallel test tip:

- use unique `databaseName` per worker/process to avoid data collisions
