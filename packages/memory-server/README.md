# @jongodb/memory-server

Start and stop a local `jongodb` process for Node integration tests and expose a MongoDB URI.

## Where It Fits

- test runners: Jest, Vitest, Mocha, Node test runner
- server frameworks: NestJS, Express, Fastify
- MongoDB data layers: MongoDB driver, Mongoose, Prisma (Mongo), TypeORM (Mongo)

This package is framework-agnostic. NestJS support is a convenience wrapper, not a hard dependency.

## Requirements

- Node.js 20+
- one launcher runtime:
  - native binary (`binaryPath`, `JONGODB_BINARY_PATH`, or bundled platform package), or
  - Java 17+ with `jongodb` classpath

## Install

```bash
npm i -D @jongodb/memory-server
```

## Launcher Runtime Selection

Default mode is `launchMode: "auto"`:

1. try binary runtime (`binaryPath`, `JONGODB_BINARY_PATH`, bundled platform package)
2. fallback to Java runtime (`classpath`, `JONGODB_CLASSPATH`)

You can force behavior with:
- `launchMode: "binary"`
- `launchMode: "java"`

## Native Binary Mode

Binary mode can be configured with:

1. `binaryPath` option
2. `JONGODB_BINARY_PATH` environment variable
3. installed platform package candidate (optional dependency)

Current platform package naming convention:
- `@jongodb/memory-server-bin-darwin-arm64`
- `@jongodb/memory-server-bin-darwin-x64`
- `@jongodb/memory-server-bin-linux-x64-gnu`
- `@jongodb/memory-server-bin-linux-x64-musl`
- `@jongodb/memory-server-bin-linux-arm64-gnu`
- `@jongodb/memory-server-bin-linux-arm64-musl`
- `@jongodb/memory-server-bin-win32-x64`
- `@jongodb/memory-server-bin-win32-arm64`

Binary mode example:

```ts
import { startJongodbMemoryServer } from "@jongodb/memory-server";

const server = await startJongodbMemoryServer({
  launchMode: "binary",
  binaryPath: process.env.JONGODB_BINARY_PATH
});
```

## Java Classpath Mode

Pass Java classpath in one of two ways:

1. `startJongodbMemoryServer({ classpath: "..." })`
2. `JONGODB_CLASSPATH` environment variable

Repository helper command:

```bash
./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath
```

If startup is slow, resolve classpath once and reuse it for the whole test run:

```bash
export JONGODB_CLASSPATH="$(./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath)"
npm run test:e2e
```

## Recommended Base API (`runtime`)

Use this when you want one lifecycle pattern across frameworks and runners.

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
- `setup()` starts the server and writes `process.env[envVarName]`
- `teardown()` stops the server and restores previous env value

## Runner Convenience APIs

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

### Mocha (or any runner with before/after hooks)

```ts
import { before, after } from "mocha";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI"
});

before(async () => {
  await runtime.setup();
});

after(async () => {
  await runtime.teardown();
});
```

## Framework Integration Patterns

### NestJS (Jest E2E example)

```ts
import { beforeAll, afterAll, describe, it, expect } from "@jest/globals";
import { Test } from "@nestjs/testing";
import { INestApplication } from "@nestjs/common";
import { registerJongodbForNestJest } from "@jongodb/memory-server/nestjs";
import { AppModule } from "../src/app.module";

describe("App (e2e)", () => {
  let app: INestApplication;

  registerJongodbForNestJest({ beforeAll, afterAll }, {
    launchMode: "auto",
    binaryPath: process.env.JONGODB_BINARY_PATH,
    classpath: process.env.JONGODB_CLASSPATH,
    envVarName: "MONGODB_URI",
    databaseName: "nest_e2e"
  });

  beforeAll(async () => {
    const moduleRef = await Test.createTestingModule({
      imports: [AppModule]
    }).compile();
    app = moduleRef.createNestApplication();
    await app.init();
  });

  afterAll(async () => {
    await app.close();
  });

  it("health check", async () => {
    expect(app).toBeDefined();
  });
});
```

Nest config (`forRootAsync`) pattern:

```ts
MongooseModule.forRootAsync({
  useFactory: () => ({
    uri: process.env.MONGODB_URI
  })
})
```

### Express/Fastify pattern

Framework side only needs a Mongo URI from env or config.

```ts
import { beforeAll, afterAll } from "@jest/globals";
import request from "supertest";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";
import { createApp } from "../src/app";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI",
  databaseName: "http_e2e"
});

let app: Awaited<ReturnType<typeof createApp>>;

beforeAll(async () => {
  await runtime.setup();
  app = await createApp({ mongoUri: process.env.MONGODB_URI! });
});

afterAll(async () => {
  await app.close();
  await runtime.teardown();
});

it("GET /health", async () => {
  await request(app.server).get("/health").expect(200);
});
```

## ORM/ODM Integration Patterns

### Mongoose

```ts
import mongoose from "mongoose";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI",
  databaseName: "mongoose_test"
});

await runtime.setup();
await mongoose.connect(process.env.MONGODB_URI!);
```

### Prisma (MongoDB connector)

Set `envVarName: "DATABASE_URL"` so Prisma uses the expected variable.

`schema.prisma`:

```prisma
datasource db {
  provider = "mongodb"
  url      = env("DATABASE_URL")
}
```

`test setup`:

```ts
import { PrismaClient } from "@prisma/client";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "DATABASE_URL",
  databaseName: "prisma_test"
});

await runtime.setup();
const prisma = new PrismaClient();
```

### TypeORM (MongoDB)

```ts
import { DataSource } from "typeorm";
import { createJongodbEnvRuntime } from "@jongodb/memory-server/runtime";

const runtime = createJongodbEnvRuntime({
  launchMode: "auto",
  binaryPath: process.env.JONGODB_BINARY_PATH,
  classpath: process.env.JONGODB_CLASSPATH,
  envVarName: "MONGODB_URI",
  databaseName: "typeorm_test"
});

await runtime.setup();

const dataSource = new DataSource({
  type: "mongodb",
  url: process.env.MONGODB_URI!,
  database: "typeorm_test"
});
await dataSource.initialize();
```

## Options

Common runtime options:
- `launchMode`: `auto` | `binary` | `java` (default: `auto`)
- `binaryPath`: path to standalone launcher binary (optional)
- `classpath`: Java classpath string or array (required unless `JONGODB_CLASSPATH` is set)
- `databaseName`: default database in generated URI (default: `test`)
- `host`: bind host (default: `127.0.0.1`)
- `port`: bind port, `0` for random (default: `0`)
- `startupTimeoutMs`: startup timeout (default: `15000`)
- `stopTimeoutMs`: graceful stop timeout before SIGKILL (default: `5000`)
- `javaPath`: Java executable path (default: `java`)
- `launcherClass`: launcher class (default: `org.jongodb.server.TcpMongoServerLauncher`)
- `envVarName`: env key written by helpers (default: `MONGODB_URI`)

Parallel test tip:
- use unique `databaseName` per worker/process to avoid data collisions

## Troubleshooting

- `No launcher runtime configured`: set binary path/env or Java classpath/env
- `Binary launch mode requested but no binary was found`: set `binaryPath` or `JONGODB_BINARY_PATH`
- `Java launch mode requested but Java classpath is not configured`: set `classpath` or `JONGODB_CLASSPATH`
- `spawn ... ENOENT`: binary path/java executable is missing on `PATH`
- `startup timeout`: launcher could not initialize or failed to emit `JONGODB_URI=...`
- slow startup on CI: resolve classpath once at workflow level and reuse env

## API

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

NestJS export (`@jongodb/memory-server/nestjs`):
- `registerJongodbForNestJest(hooks, options?)`

Vitest export (`@jongodb/memory-server/vitest`):
- `registerJongodbForVitest(hooks, options?)`
