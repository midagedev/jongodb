# jongodb

`jongodb` is an in-memory MongoDB-compatible command engine for integration testing.

In-memory MongoDB-compatible test engine for Spring Boot integration tests.
Practical alternative to MongoDB Testcontainers when fast deterministic test runs are required.

It is designed for fast, deterministic integration-test backends.
It is not a production MongoDB replacement.

Status snapshot (2026-02-24): active development with R3 certification evidence published.

Search terms:
- in-memory MongoDB for Spring Boot tests
- MongoDB Testcontainers alternative
- embedded MongoDB replacement for integration tests
- MongoDB-compatible wire protocol test backend

## Use It / Do Not Use It

| Decision | When |
| --- | --- |
| Use it | You want fast in-memory integration tests with deterministic behavior and in-process command/wire handling |
| Use it | Your tests focus on common CRUD, basic aggregation, and single-process transaction flows |
| Do not use it | You need full MongoDB feature parity or production-grade distributed behavior |
| Do not use it | Your test harness strictly requires an external TCP MongoDB endpoint |

## Quick Start

### 1) Add dependency

```kotlin
dependencies {
    testImplementation("io.github.midagedev:jongodb:<version>")
}
```

### 2) Pick integration mode

Command API mode:

```java
import org.bson.BsonDocument;
import org.jongodb.command.CommandDispatcher;
import org.jongodb.command.CommandStore;
import org.jongodb.command.EngineBackedCommandStore;
import org.jongodb.engine.InMemoryEngineStore;

CommandStore store = new EngineBackedCommandStore(new InMemoryEngineStore());
CommandDispatcher dispatcher = new CommandDispatcher(store);

BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
    "{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1,\"name\":\"alice\"}]}"
));
```

Wire `OP_MSG` mode (in-process):

```java
import org.bson.BsonDocument;
import org.jongodb.server.WireCommandIngress;
import org.jongodb.wire.OpMsg;
import org.jongodb.wire.OpMsgCodec;

WireCommandIngress ingress = WireCommandIngress.inMemory();
OpMsgCodec codec = new OpMsgCodec();

OpMsg request = new OpMsg(
    101, 0, 0,
    BsonDocument.parse("{\"find\":\"users\",\"$db\":\"app\",\"filter\":{}}")
);

OpMsg response = codec.decode(ingress.handle(codec.encode(request)));
System.out.println(response.body().toJson());
```

### 3) Run tests

```bash
gradle test
```

Requirements:
- Java 17+
- Gradle 8+
- This repository currently does not include `gradlew`

## Spring Integration Notes

- `jongodb` is currently an in-process backend (no external TCP server process).
- It is suitable when your test bootstrap can route command/wire messages in-process.
- For tests that must connect through standard `mongodb://` endpoint semantics, keep a real/embedded `mongod` backend for that test profile.

### Spring Boot Test Setup (Initializer)

```java
@SpringBootTest
@ContextConfiguration(initializers = JongodbMongoInitializer.class)
class MyIntegrationTest {
}
```

### Spring Boot Test Setup (DynamicPropertySource)

```java
@SpringBootTest
class MyIntegrationTest {
  @DynamicPropertySource
  static void mongoProps(DynamicPropertyRegistry registry) {
    JongodbMongoDynamicPropertySupport.register(registry);
  }
}
```

See full migration guidance:
- `docs/SPRING_TESTING.md`

## Compatibility Snapshot (Current)

This project targets integration-test compatibility for common Spring data paths.
It does not target full MongoDB server parity.

| Area | Current level | Notes |
| --- | --- | --- |
| Command surface | 17 command handlers implemented | Mix of `Supported` and `Partial` behaviors by command |
| Support manifest | 7 `Supported`, 4 `Partial`, 1 `Unsupported` feature groups | Source: `docs/SUPPORT_MATRIX.md` |
| Query language | Core comparison/logical/array/regex + partial `$expr` | Advanced expression/operator parity is incomplete |
| Aggregation | Core stages + selected Tier-2 stages | Expression depth and many advanced operators are missing |
| Transactions | Single-process session/transaction flow (`start`/`commit`/`abort`) | No distributed/replica-set semantics |
| Wire protocol | `OP_MSG` in-process ingress | No external TCP server process |

Detailed boundaries:
- `docs/COMPATIBILITY.md`
- `docs/SUPPORT_MATRIX.md`

## R3 Certification Snapshot (2026-02-24)

| Gate | Result | Evidence |
| --- | --- | --- |
| Official UTF sharded differential | PASS | Run `22332998372` |
| R3 failure ledger | PASS | Run `22332937657` |
| External Spring canary certification | PASS | Run `22332937633` |
| Compatibility scorecard | PASS | `docs/COMPATIBILITY_SCORECARD.md` |
| Support boundary manifest | Published | `docs/SUPPORT_MATRIX.md` |

Certification references:
- `docs/COMPATIBILITY_SCORECARD.md`
- `docs/SUPPORT_MATRIX.md`
- `docs/RELEASE_CHECKLIST.md`

## Migration Checklist (From mongo-java-server / Similar Backends)

1. Replace test dependency with `io.github.midagedev:jongodb:<version>`.
2. Switch test bootstrap to in-process command/wire mode.
3. Run integration tests and classify failures:
   - expected unsupported: `codeName=NotImplemented`, `errorLabels=["UnsupportedFeature"]`
   - unexpected regression: behavior mismatch vs current baseline
4. Keep a fallback profile for tests requiring unsupported features.

## Implemented Command Handlers

- `hello`, `isMaster`, `ping`, `buildInfo`, `getParameter`
- `insert`, `find`, `aggregate`, `getMore`, `killCursors`
- `createIndexes`, `listIndexes`, `update`, `delete`, `findAndModify`
- `commitTransaction`, `abortTransaction`

## Notable Limitations

Current known gaps:
- no external TCP server process (in-process ingress only)
- query and aggregation parity is partial, not full MongoDB parity
- collation and TTL are currently metadata-level for indexes; runtime semantics are limited
- update operator coverage is intentionally limited

Unsupported paths are increasingly standardized with:
- `codeName=NotImplemented`
- `errorLabels=["UnsupportedFeature"]`

Coverage is still being expanded.

## Scope

Core packages:
- `src/main/java/org/jongodb/command`: command handlers and dispatcher
- `src/main/java/org/jongodb/engine`: in-memory storage/query/aggregation engine
- `src/main/java/org/jongodb/server`: in-process wire command ingress
- `src/main/java/org/jongodb/testkit`: differential harness and evidence generators

## Evidence Tasks

Key verification tasks:
- `gradle springCompatibilityMatrixEvidence`
- `gradle utfCorpusEvidence -PutfSpecRoot=<path> -PutfMongoUri=<uri>`
- `gradle r3FailureLedger -Pr3SpecRepoRoot=<path> -Pr3FailureLedgerMongoUri=<uri> -Pr3FailureLedgerFailOnFailures=true`
- `gradle r3CanaryCertificationEvidence -Pr3CanaryInputJson=<path>`

Full task list and artifact paths:
- `docs/USAGE.md`

## Maven Central Publishing

This repository is configured for Maven Central publishing through Central Publisher Portal.

Local release command:

```bash
gradle \
  -PpublishVersion=0.1.0 \
  -PpublishGroup=io.github.midagedev \
  -PpublishArtifactId=jongodb \
  centralRelease
```

Required environment variables for release:
- `JRELEASER_GITHUB_TOKEN`
- `JRELEASER_MAVENCENTRAL_APP_USERNAME`
- `JRELEASER_MAVENCENTRAL_APP_PASSWORD`
- `JRELEASER_GPG_PUBLIC_KEY`
- `JRELEASER_GPG_SECRET_KEY`
- `JRELEASER_GPG_PASSPHRASE`

GitHub Actions workflow:
- `.github/workflows/maven-central-release.yml` (manual `workflow_dispatch`)

## Documentation

- Usage: `docs/USAGE.md`
- Spring test integration: `docs/SPRING_TESTING.md`
- Compatibility matrix: `docs/COMPATIBILITY.md`
- Compatibility scorecard: `docs/COMPATIBILITY_SCORECARD.md`
- Support matrix: `docs/SUPPORT_MATRIX.md`
- Release checklist: `docs/RELEASE_CHECKLIST.md`
- Roadmap: `docs/ROADMAP.md`
- Contribution guide: `CONTRIBUTING.md`
- Research notes: `docs/research/README.md`
