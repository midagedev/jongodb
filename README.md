# jongodb

`jongodb` is an in-memory MongoDB-compatible command engine for integration testing.

The current implementation is focused on:
- deterministic command handling
- in-process wire command ingress (`OP_MSG`)
- differential testing against real `mongod`
- release-readiness evidence artifacts

It is not a full drop-in replacement for production MongoDB.

Status snapshot (2026-02-23): active development, R2 compatibility infrastructure in place.

## Compatibility Level (Current)

This project targets integration-test compatibility for common Spring data paths.
It does not target full MongoDB server parity.

| Area | Current level | Notes |
| --- | --- | --- |
| Command surface | 16 command handlers implemented | Mix of `Supported` and `Partial` behaviors by command |
| R2 support manifest | 7 `Supported`, 4 `Partial`, 1 `Unsupported` feature groups | Source: `R2CompatibilityScorecard` default manifest |
| Query language | Core comparison/logical/array/regex + partial `$expr` | Advanced expression/operator parity is incomplete |
| Aggregation | Core stages + selected Tier-2 stages | Expression depth and many advanced operators are missing |
| Transactions | Single-process session/transaction flow (`start`/`commit`/`abort`) | No distributed/replica-set semantics |
| Wire protocol | `OP_MSG` in-process ingress | No external TCP server process |

Recommended usage:
- fast integration tests that need deterministic command behavior
- Spring projects that use common CRUD and basic aggregation paths

Not a fit yet:
- full MongoDB compatibility requirements
- heavy use of advanced update operators, collation semantics, or TTL runtime behavior

## Scope

Core packages:
- `src/main/java/org/jongodb/command`: command handlers and dispatcher
- `src/main/java/org/jongodb/engine`: in-memory storage/query/aggregation engine
- `src/main/java/org/jongodb/server`: in-process wire command ingress
- `src/main/java/org/jongodb/testkit`: differential harness and evidence generators

## Quick Start

Requirements:
- Java 17+
- Gradle 8+ (this repository currently does not include `gradlew`)

Run tests:

```bash
gradle test
```

Use the latest release from Maven Central in another project:

```kotlin
dependencies {
    testImplementation("io.github.midagedev:jongodb:<version>")
}
```

Use the command dispatcher directly:

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

Use wire ingress (`OP_MSG`) in-process:

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

## Command Support Snapshot

Implemented command handlers:
- `hello`, `isMaster`, `ping`, `buildInfo`, `getParameter`
- `insert`, `find`, `aggregate`, `getMore`, `killCursors`
- `createIndexes`, `listIndexes`, `update`, `delete`
- `commitTransaction`, `abortTransaction`

Details:
- `docs/COMPATIBILITY.md`

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

## Evidence Tasks

Verification tasks (Gradle):
- `gradle m3GateEvidence`
- `gradle realMongodDifferentialBaseline -PrealMongodUri=<uri>`
- `gradle springCompatibilityMatrixEvidence`
- `gradle utfCorpusEvidence -PutfSpecRoot=<path> -PutfMongoUri=<uri>`
- `gradle r2CompatibilityEvidence`
- `gradle r2CanaryCertificationEvidence -Pr2CanaryInputJson=<path>`
- `gradle finalReadinessEvidence`

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
- Compatibility matrix: `docs/COMPATIBILITY.md`
- Roadmap: `docs/ROADMAP.md`
- Contribution guide: `CONTRIBUTING.md`
- Research notes: `docs/research/README.md`
