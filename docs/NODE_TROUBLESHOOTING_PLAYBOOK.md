# Node Adapter Troubleshooting Playbook (Error Signature Based)

Use this playbook when `@jongodb/memory-server` startup or test-runtime integration fails.

## Quick Triage Flow

1. Capture the exact error message (first line + stack origin).
2. Match message text in the signature table below.
3. Apply the corresponding fix.
4. Re-run with debug logging (`logLevel: "debug"`) if still unresolved.

## Signature Table

| Error signature (contains) | Likely cause | Primary fix |
| --- | --- | --- |
| `No launcher runtime configured` | neither binary nor Java classpath resolved | set `binaryPath` / `JONGODB_BINARY_PATH`, or set `classpath` / `JONGODB_CLASSPATH` |
| `Binary launch mode requested but no binary was found` | `launchMode: "binary"` but executable is not resolvable | provide explicit `binaryPath` or set `JONGODB_BINARY_PATH` |
| `Binary checksum verification failed` | configured checksum does not match local binary bytes | verify binary provenance and align `binaryChecksum`/`JONGODB_BINARY_CHECKSUM` |
| `EADDRINUSE` / `address already in use` | requested port is occupied by another process/test | use `port: 0` or tune `portRetryAttempts`/`portRetryBackoffMs` for fixed-port mode |
| `Java launch mode requested but Java classpath is not configured` | `launchMode: "java"` without classpath | pass `classpath` option or export `JONGODB_CLASSPATH` |
| `Classpath auto-discovery probe failed` | Gradle classpath probe command/cwd is invalid or unavailable | set explicit `classpath`/`JONGODB_CLASSPATH`, or fix `classpathDiscoveryCommand`/`classpathDiscoveryWorkingDirectory` |
| `Failed to start jongodb with available launch configurations` | all launch candidates failed (binary/java) | inspect aggregated `[binary:...]` / `[java:...]` messages and fix per candidate |
| `Launcher emitted empty JONGODB_URI line` | launcher started but did not emit valid URI contract | verify launcher command args/env; update runtime/binary to a compatible build |
| `Requested topologyProfile=singleNodeReplicaSet but URI is missing replicaSet query` | topology profile and emitted URI are mismatched | ensure launcher emits `?replicaSet=<name>` or switch to `topologyProfile: "standalone"` |
| `Requested topologyProfile=standalone but URI includes replicaSet query` | standalone profile requested, replica-set URI emitted | remove replica-set URI query or request replica-set profile explicitly |
| `Requested replicaSetName does not match URI replicaSet query` | configured replica set name differs from emitted URI | align `replicaSetName` with launcher output |
| `Jest global state file has invalid schema` | stale/corrupted `.jongodb/jest-memory-server.json` | delete state file and rerun setup |
| `Unable to stop detached Jest process pid=` | detached launcher process did not terminate cleanly | verify PID ownership/process health and rerun teardown; if needed kill process manually |
| port remains occupied after abrupt test abort | non-detached launcher was left running after forced interruption | keep `cleanupOnProcessExit=true` (default) and rerun teardown/cleanup |
| `envVarName/envVarNames entries must not be empty` | invalid runtime/jest/vitest env key config | ensure all env var names are non-empty trimmed strings |
| `projectName must not be empty` | invalid Vitest workspace integration input | provide non-empty `projectName` for `registerJongodbForVitestWorkspace` |

## Fast Fix Commands

Resolve Java classpath in repo:

```bash
export JONGODB_CLASSPATH="$(./.tooling/gradle-8.10.2/bin/gradle --no-daemon -q printLauncherClasspath | tail -n 1)"
```

Customize classpath auto-discovery probe:

```bash
export JONGODB_CLASSPATH_DISCOVERY_CMD="./.tooling/gradle-8.10.2/bin/gradle"
export JONGODB_CLASSPATH_DISCOVERY_CWD="$PWD"
```

Run node compatibility smoke after fix:

```bash
npm run node:build
npm --prefix testkit/node-compat test
```

Clear classpath artifact cache:

```bash
rm -rf .jongodb/cache
```

Clear Jest global runtime state:

```bash
rm -f .jongodb/jest-memory-server.json
```

## Configuration Sanity Checklist

- `launchMode` is one of `auto`, `binary`, `java`.
- `classpathDiscovery` is `auto` or `off` (`auto` default).
- `artifactCacheMaxEntries` / `artifactCacheMaxBytes` / `artifactCacheTtlMs` are positive values.
- `cleanupOnProcessExit` is enabled unless you intentionally want detached/orphaned lifecycle behavior.
- `portRetryAttempts` / `portRetryBackoffMs` are non-negative values for fixed-port collision handling.
- `logLevel` is one of `silent`/`error`/`warn`/`info`/`debug` and `logFormat` is `plain` or `json`.
- `host`, `port`, `databaseName` values are valid/non-empty.
- `topologyProfile` and `replicaSetName` match expected URI contract.
- `envVarName` / `envVarNames` are valid and unique for your test runtime.
- CI runner has either bundled binary availability or Java classpath setup.

## Escalation Data to Attach

When opening an issue, include:
- exact error message
- runtime options (without secrets)
- launch mode (`auto` / `binary` / `java`)
- Node version + OS
- whether failure is local-only or CI-only
