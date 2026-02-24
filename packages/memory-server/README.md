# @jongodb/memory-server

Node.js adapter package for starting a `jongodb` test server and obtaining a MongoDB URI.

Current status:
- runtime process manager is implemented
- Jest/Vitest convenience wrappers are implemented

## Runtime Prerequisite

The launcher requires a Java classpath that contains `jongodb` and runtime dependencies.

- pass `classpath` in `startJongodbMemoryServer({ classpath })`, or
- set `JONGODB_CLASSPATH` environment variable.

Repository-local helper:

```bash
./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath
```

## Vitest helper

```ts
import { beforeAll, afterAll } from "vitest";
import { registerJongodbForVitest } from "@jongodb/memory-server/vitest";

const runtime = registerJongodbForVitest({ beforeAll, afterAll }, {
  classpath: process.env.JONGODB_CLASSPATH
});

// runtime.uri available after beforeAll
```

## Jest helper

Per-file lifecycle:

```ts
import { beforeAll, afterAll } from "@jest/globals";
import { registerJongodbForJest } from "@jongodb/memory-server/jest";

const runtime = registerJongodbForJest({ beforeAll, afterAll }, {
  classpath: process.env.JONGODB_CLASSPATH
});
```

Global setup/teardown:

```ts
import { createJestGlobalSetup, createJestGlobalTeardown } from "@jongodb/memory-server/jest";

export default createJestGlobalSetup({ classpath: process.env.JONGODB_CLASSPATH });
// teardown file: export default createJestGlobalTeardown();
```

## Local Commands

From repository root:

```bash
npm ci
npm run node:build
npm run node:typecheck
npm run node:test
```
