# @jongodb/memory-server

Node.js adapter package for starting a `jongodb` test server and obtaining a MongoDB URI.

Current status:
- runtime process manager is implemented
- Jest/Vitest convenience wrappers are tracked separately (`#109`)

## Runtime Prerequisite

The launcher requires a Java classpath that contains `jongodb` and runtime dependencies.

- pass `classpath` in `startJongodbMemoryServer({ classpath })`, or
- set `JONGODB_CLASSPATH` environment variable.

Repository-local helper:

```bash
./.tooling/gradle-8.10.2/bin/gradle -q printLauncherClasspath
```

## Local Commands

From repository root:

```bash
npm ci
npm run node:build
npm run node:typecheck
npm run node:test
```
