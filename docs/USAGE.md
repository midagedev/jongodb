# Usage Guide

## Prerequisites

- Java 17+
- Gradle 8+

This repository does not currently include a Gradle wrapper script (`gradlew`).

## Build and Test

Run the full test suite:

```bash
gradle test
```

Run a specific test class:

```bash
gradle test --tests org.jongodb.command.CommandDispatcherE2ETest
```

## Consume from Another Spring Project

Use Maven Central coordinates in your test scope:

```kotlin
dependencies {
    testImplementation("io.github.midagedev:jongodb:<version>")
}
```

If you need command-level integration without sockets, instantiate `WireCommandIngress` in your test bootstrap and route BSON commands through `OpMsgCodec`.

## In-Process Runtime Usage

### 1) Command Dispatcher API

Use this if your test harness can call command handlers directly:

```java
CommandStore store = new EngineBackedCommandStore(new InMemoryEngineStore());
CommandDispatcher dispatcher = new CommandDispatcher(store);

BsonDocument response = dispatcher.dispatch(BsonDocument.parse(
    "{\"find\":\"users\",\"$db\":\"app\",\"filter\":{\"status\":\"active\"}}"
));
```

### 2) Wire Command Ingress (`OP_MSG`)

Use this if your test harness expects wire message flow:

```java
WireCommandIngress ingress = WireCommandIngress.inMemory();
OpMsgCodec codec = new OpMsgCodec();

OpMsg req = new OpMsg(
    1001, 0, 0,
    BsonDocument.parse("{\"insert\":\"users\",\"$db\":\"app\",\"documents\":[{\"_id\":1}]}")
);

OpMsg res = codec.decode(ingress.handle(codec.encode(req)));
```

## Verification and Evidence Tasks

Run differential baseline against real MongoDB:

```bash
gradle realMongodDifferentialBaseline -PrealMongodUri="mongodb://localhost:27017"
```

Run UTF corpus evidence:

```bash
gradle utfCorpusEvidence \
  -PutfSpecRoot="/path/to/specs" \
  -PutfMongoUri="mongodb://localhost:27017"
```

Replay a single failure bundle from UTF artifacts:

```bash
gradle replayFailureBundle \
  -PreplayBundleDir="build/reports/unified-spec/failure-replay-bundles" \
  -PreplayFailureId="utf::some-case-id"
```

Generate R3 failure ledger artifacts:

```bash
gradle r3FailureLedger \
  -Pr3SpecRepoRoot="third_party/mongodb-specs/.checkout/specifications" \
  -Pr3FailureLedgerMongoUri="mongodb://localhost:27017" \
  -Pr3FailureLedgerFailOnFailures=true
```

Run Spring compatibility matrix:

```bash
gradle springCompatibilityMatrixEvidence
```

Run R2 compatibility scorecard + support manifest:

```bash
gradle r2CompatibilityEvidence
```

Run R2 canary certification from canary result JSON:

```bash
gradle r2CanaryCertificationEvidence \
  -Pr2CanaryInputJson="/path/to/projects.json"
```

Run final readiness aggregation:

```bash
gradle finalReadinessEvidence
```

## Publish to Maven Central

This project publishes through Central Publisher Portal via JReleaser.

Release command:

```bash
gradle \
  -PpublishVersion=0.1.0 \
  -PpublishGroup=io.github.midagedev \
  -PpublishArtifactId=jongodb \
  centralRelease
```

What `centralRelease` does:
- publish `mavenJava` artifacts to `build/staging-deploy`
- run `jreleaserDeploy` with Maven Central rules enabled

Required environment variables:
- `JRELEASER_GITHUB_TOKEN`
- `JRELEASER_MAVENCENTRAL_APP_USERNAME`
- `JRELEASER_MAVENCENTRAL_APP_PASSWORD`
- `JRELEASER_GPG_PUBLIC_KEY`
- `JRELEASER_GPG_SECRET_KEY`
- `JRELEASER_GPG_PASSPHRASE`

GitHub Actions workflow:
- `.github/workflows/maven-central-release.yml`

## Artifact Paths (Defaults)

- UTF differential:
  - `build/reports/unified-spec/utf-differential-report.json`
  - `build/reports/unified-spec/utf-differential-report.md`
  - `build/reports/unified-spec/failure-replay-bundles/manifest.json`
- R3 failure ledger:
  - `build/reports/r3-failure-ledger/r3-failure-ledger.json`
  - `build/reports/r3-failure-ledger/r3-failure-ledger.md`
  - `build/reports/r3-failure-ledger/r3-failure-ledger-trend.md`
- Spring matrix:
  - `build/reports/spring-matrix/spring-compatibility-matrix.json`
  - `build/reports/spring-matrix/spring-compatibility-matrix.md`
- R2 compatibility:
  - `build/reports/r2-compatibility/r2-compatibility-scorecard.json`
  - `build/reports/r2-compatibility/r2-compatibility-scorecard.md`
  - `build/reports/r2-compatibility/r2-support-manifest.json`
- R2 canary:
  - `build/reports/r2-canary/r2-canary-certification.json`
  - `build/reports/r2-canary/r2-canary-certification.md`
- Final readiness:
  - `build/reports/release-readiness/r1-final-readiness-report.json`
  - `build/reports/release-readiness/r1-final-readiness-report.md`

## Canary Input Format

`r2CanaryCertificationEvidence` expects a JSON file with this shape:

```json
{
  "projects": [
    {
      "projectId": "petclinic",
      "repository": "https://example/petclinic",
      "revision": "abc123",
      "canaryPassed": true,
      "rollback": {
        "attempted": true,
        "success": true,
        "recoverySeconds": 42
      },
      "notes": "optional"
    }
  ]
}
```

The certification gate requires:
- at least 3 projects
- all canaries pass
- rollback rehearsal success for all projects
