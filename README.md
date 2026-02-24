# jongodb

In-memory MongoDB-compatible backend for fast Spring integration tests.

`jongodb` targets fast, deterministic Spring Boot test execution without Docker/container bootstrap.
It supports both in-process test bootstrap and standalone TCP launcher mode.
It is not a production MongoDB replacement.

Latest release: `0.1.3`  
Maven coordinate: `io.github.midagedev:jongodb:0.1.3`

## Why This Project Exists

Container-based integration tests are useful for production fidelity, but they are often slower and more environment-sensitive during day-to-day development.

`jongodb` provides a different tradeoff:
- faster local/CI test feedback
- deterministic in-process behavior
- explicit support boundary for unsupported MongoDB features

## Value for Spring Boot Test Suites

Typical gains when replacing Testcontainers for supported scenarios:
- lower startup overhead (no image pull/container bootstrap)
- fewer environment-related flakes (Docker daemon/network/port state)
- faster debug loops for service-level integration tests
- simpler test infrastructure in CI and local dev

This is most useful for:
- CRUD-heavy service tests
- repository/template behavior tests
- transaction envelope flows in a single-process test runtime

## jongodb vs Testcontainers

| Topic | `jongodb` | MongoDB Testcontainers |
| --- | --- | --- |
| Startup/runtime overhead | Low (in-process) | Higher (container bootstrap + networking) |
| Determinism | High for supported feature paths | Good, but depends on container/runtime environment |
| Feature fidelity vs real MongoDB | Partial by design | High |
| Infra requirement | No Docker required | Docker required |
| Best fit | Fast development and CI feedback loops | High-fidelity compatibility checks |

Recommended strategy:
- default profile: `jongodb`
- high-fidelity profile: Testcontainers/real `mongod` for unsupported or deployment-level behavior

## When to Use / Not Use

| Decision | Use when... |
| --- | --- |
| Use | You need fast deterministic integration tests with common MongoDB command paths |
| Use | You want to remove container startup cost in routine CI/local runs |
| Do not use | You require full MongoDB parity (replica set, distributed semantics, advanced unsupported operators) |
| Do not use | Your test harness strictly depends on external `mongodb://` runtime behavior |

## Quick Start

### 1) Add dependency

```kotlin
dependencies {
    testImplementation("io.github.midagedev:jongodb:0.1.3")
}
```

### 2) Spring Boot integration

Single-hook annotation style:

```java
@SpringBootTest
@JongodbMongoTest
class MyIntegrationTest {
}
```

Initializer style:

```java
@SpringBootTest
@ContextConfiguration(initializers = JongodbMongoInitializer.class)
class MyIntegrationTest {
}
```

Dynamic property style:

```java
@SpringBootTest
class MyIntegrationTest {
  @DynamicPropertySource
  static void mongoProps(DynamicPropertyRegistry registry) {
    JongodbMongoDynamicPropertySupport.register(registry);
  }
}
```

### 3) Run tests

```bash
gradle test
```

Requirements:
- Java 17+
- Gradle 8+

## Standalone TCP Launcher

`jongodb` can run as a standalone TCP process and expose a standard `mongodb://` URI.

For detailed launcher usage (Java/binary runtime selection and framework examples), see:
- `packages/memory-server/README.md`

## Node Adapter

Node adapter package: `@jongodb/memory-server` (source under `packages/memory-server`).

Current state:
- framework-agnostic runtime helper (`./runtime`)
- launcher modes: `auto` / `binary` / `java`
- standalone launcher process management
- optional single-node replica-set semantic profile (`topologyProfile=singleNodeReplicaSet`)
- platform binary package targets: darwin-arm64, linux-x64-gnu, win32-x64
- Jest/Vitest/Nest-Jest helpers
- CI release workflow: `.github/workflows/npm-node-release.yml`

Node runtime note:
- default `auto` mode: binary first, then Java classpath fallback
- Java runtime requires `classpath` option or `JONGODB_CLASSPATH`
- detailed usage/examples: `packages/memory-server/README.md`

## Transaction Contract (Current)

Supported in current scope:
- single transaction-manager flow with one or multiple MongoTemplate/Repository paths
- transactional commit/abort lifecycle (`startTransaction` / `commitTransaction` / `abortTransaction`)
- transaction-scoped snapshot reads for `find`, `aggregate`, and `countDocuments`
- out-of-transaction writes in different namespaces are preserved at commit
- out-of-transaction writes in same namespace with different `_id` are preserved at commit

Deterministic conflict policy:
- if transactional and non-transactional writes target the same `_id`, commit applies the transactional version

Out of scope:
- distributed transaction semantics
- multi-node replica-set deployment behavior (elections/lag/step-down)
- advanced retry semantics across real process/network faults

## Compatibility Snapshot

This project targets integration-test compatibility for common Spring data paths, not full MongoDB parity.

| Area | Current level | Notes |
| --- | --- | --- |
| Command surface | 22 handlers | Mix of `Supported` and `Partial` |
| Query language | Core comparison/logical/array/regex + partial `$expr` | Advanced parity incomplete |
| Aggregation | Core stages + selected Tier-2 stages | Full operator coverage not implemented |
| Transactions | Single-process session/transaction flow | Namespace-aware commit merge + snapshot reads (`find`/`aggregate`/`countDocuments`) + deterministic retry labels/contracts |
| Deployment profile | Standalone + single-node replica-set semantic profile | Replica-set profile exposes primary-only handshake/URI semantics for driver compatibility |
| Wire protocol | `OP_MSG` + `OP_QUERY` | In-process ingress and standalone TCP launcher mode, with OP_QUERY namespace-based `$db` fallback |

Support manifest summary:
- `Supported`: 7
- `Partial`: 4
- `Unsupported`: 1

Details:
- `docs/SUPPORT_MATRIX.md`
- `docs/COMPATIBILITY.md`

## Migration Notes

### From Testcontainers

Before:

```java
@Container
static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

@DynamicPropertySource
static void mongoProps(DynamicPropertyRegistry registry) {
  registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
}
```

After:

```java
@DynamicPropertySource
static void mongoProps(DynamicPropertyRegistry registry) {
  JongodbMongoDynamicPropertySupport.register(registry);
}
```

## Implemented Command Handlers

- `hello`, `isMaster`, `ping`, `buildInfo`, `getParameter`
- `insert`, `find`, `aggregate`, `getMore`, `killCursors`
- `createIndexes`, `listIndexes`, `update`, `delete`, `bulkWrite`
- `findAndModify`, `countDocuments`, `replaceOne`, `findOneAndUpdate`, `findOneAndReplace`
- `commitTransaction`, `abortTransaction`

## Known Limitations

- partial query and aggregation parity
- collation/TTL are currently metadata-focused, not full runtime parity
- limited update operator coverage
- no multi-node replica-set or sharded topology semantics

Unsupported branches are standardized progressively as:
- `codeName=NotImplemented`
- `errorLabels=["UnsupportedFeature"]`

## Documentation

- Usage: `docs/USAGE.md`
- Spring integration: `docs/SPRING_TESTING.md`
- Compatibility boundary: `docs/COMPATIBILITY.md`
- Support matrix: `docs/SUPPORT_MATRIX.md`
- Compatibility scorecard: `docs/COMPATIBILITY_SCORECARD.md`
- Release checklist: `docs/RELEASE_CHECKLIST.md`
- Roadmap: `docs/ROADMAP.md`
- Changelog: `CHANGELOG.md`
- Contributing: `CONTRIBUTING.md`
- Research notes: `docs/research/README.md`

## Release Process (Maintainers)

1. Push tag `vX.Y.Z`.
2. Ensure CI test gates are green (`CI Test Suite` and compatibility workflows as needed).
3. GitHub Actions workflow `.github/workflows/maven-central-release.yml` publishes to Maven Central.
4. Workflow creates GitHub Release notes.

Required secrets:
- `MAVEN_CENTRAL_USERNAME`
- `MAVEN_CENTRAL_PASSWORD`
- `GPG_PUBLIC_KEY`
- `GPG_SECRET_KEY`
- `GPG_PASSPHRASE`
