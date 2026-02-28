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

## Spring Boot Test Integration

### Option A: Single-hook annotation

Use `JongodbMongoTest` to opt in with one annotation instead of custom initializer or `@DynamicPropertySource` boilerplate:

```java
import org.jongodb.spring.test.JongodbMongoTest;

@SpringBootTest
@JongodbMongoTest
class AccountIntegrationTest {
}
```

Optional database override:
- add `@TestPropertySource(properties = "jongodb.test.database=<dbName>")`.

### Option B: `ApplicationContextInitializer`

Use `JongodbMongoInitializer` when your tests already use `@ContextConfiguration` initializers:

```java
import org.jongodb.spring.test.JongodbMongoInitializer;
import org.springframework.test.context.ContextConfiguration;

@SpringBootTest
@ContextConfiguration(initializers = JongodbMongoInitializer.class)
class AccountIntegrationTest {
}
```

Optional database override:
- set `jongodb.test.database=<dbName>` as a test property.

Optional shared-server mode for multi-context suites:
- set `jongodb.test.sharedServer=true` as a test property.
- when enabled, Spring contexts in the same JVM reuse one in-process TCP server.
- use unique `jongodb.test.database` values per test class when sharing to avoid data bleed.

Fast reset without restarting Spring context:
- call `JongodbMongoResetSupport.reset(applicationContext)` from `@BeforeEach` or class-level hooks.
- reset clears all databases in the backing in-memory server.

### Option C: `@DynamicPropertySource`

Use `JongodbMongoDynamicPropertySupport` as a drop-in replacement pattern for many Testcontainers setups:

```java
import org.jongodb.spring.test.JongodbMongoDynamicPropertySupport;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

@SpringBootTest
class AccountIntegrationTest {
  @DynamicPropertySource
  static void mongoProps(DynamicPropertyRegistry registry) {
    JongodbMongoDynamicPropertySupport.register(registry);
  }
}
```

See migration details:
- `docs/SPRING_TESTING.md`

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

Current certified reference runs (2026-02-24):
- Official Suite Sharded: `22332998372`
- R3 Failure Ledger: `22332937657`
- R3 External Canary Certification: `22332937633`

Official Suite Sharded UTF gate modes:
- `warn` (default for `pull_request` and manual dispatch): publish aggregate `total/match/mismatch/error` + pass rate summary, emit warning when mismatch/error remain.
- `strict` (default for scheduled `main` run): fail workflow when aggregate mismatch/error is non-zero.
- `off`: summary only, no mismatch/error gate.
- Manual dispatch input: `utf_gate_mode` (`off | warn | strict`).

Run differential baseline against real MongoDB:

```bash
gradle realMongodDifferentialBaseline \
  -PrealMongodUri="mongodb://localhost:27017" \
  -PrealMongodMaxMismatch=0 \
  -PrealMongodMaxError=0 \
  -PrealMongodMinPassRate=1.0 \
  -PrealMongodFailOnGate=true
```

`realMongodDifferentialBaseline` gate options:
- `realMongodMaxMismatch` (default `0`)
- `realMongodMaxError` (default `0`)
- `realMongodMinPassRate` (optional, range `0.0..1.0`)
- `realMongodFailOnGate` (`true` by default)

Run UTF corpus evidence:

```bash
gradle utfCorpusEvidence \
  -PutfSpecRoot="/path/to/specs" \
  -PutfMongoUri="mongodb://localhost:27017"
```

Run complex-query certification evidence and gate:

```bash
gradle complexQueryCertificationEvidence \
  -PcomplexQueryMongoUri="mongodb://localhost:27017/?replicaSet=rs0" \
  -PcomplexQueryFailOnGate=true
```

Optional local triage overrides:
- `-PcomplexQuerySeed=<text>`
- `-PcomplexQueryPatternLimit=<n>`
- `-PcomplexQueryOutputDir=<path>`

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

R3 Failure Ledger workflow (`.github/workflows/r3-failure-ledger.yml`) also:
- emits top `failureId` + `firstDiffPath` summary in trend markdown.
- compares ledger failure IDs with latest `Official Suite Sharded` shard artifacts.
- posts a schedule-time triage comment to issue `#378` when `failureCount > 0`.

Run Spring compatibility matrix:

```bash
gradle springCompatibilityMatrixEvidence
```

Spring matrix artifacts include a dedicated `Complex Query Matrix` section aligned with
`docs/COMPLEX_QUERY_CERTIFICATION.md` pattern IDs.
Current default catalog scope:
- targets: 5
- scenarios: 15
- certification-mapped scenarios: 8
Artifact summaries now include a `Certification Pattern Mapping` section
(`scenarioId ↔ certificationPatternId`) for quick cross-reference with certification runs.
CI workflow: `.github/workflows/spring-complex-query-matrix.yml`

Run R2 compatibility scorecard + support manifest:

```bash
gradle r2CompatibilityEvidence
```

Run R2 canary certification from canary result JSON:

```bash
gradle r2CanaryCertificationEvidence \
  -Pr2CanaryInputJson="/path/to/projects.json"
```

Run R3 external canary certification from canary result JSON:

```bash
gradle r3CanaryCertificationEvidence \
  -Pr3CanaryInputJson="testkit/canary/r3/projects.mainline.json"
```

Note:
- `r3CanaryCertificationEvidence` currently reuses `R2CanaryCertification` output naming.
- Artifact file names are still `r2-canary-certification.json/.md` under the `r3-canary` directory.
- Workflow `.github/workflows/r3-external-canary.yml` runs on schedule and emits
  `build/reports/r3-canary/r3-canary-gate-summary.md` with gate + diagnostics + reproduce command.

Run final readiness aggregation:

```bash
gradle finalReadinessEvidence
```

## Publish to Maven Central

This project publishes through Central Publisher Portal via JReleaser.

Recommended release path:
1. Push a tag with semantic version format: `vX.Y.Z`
2. Workflow `.github/workflows/maven-central-release.yml` resolves version from the tag
3. If that version is not published yet, it runs `centralRelease`
4. It then creates a GitHub Release with generated notes

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

Manual fallback command:

```bash
gradle \
  -PpublishVersion=0.1.5 \
  -PpublishGroup=io.github.midagedev \
  -PpublishArtifactId=jongodb \
  centralRelease
```

## Artifact Paths (Defaults)

- UTF differential:
  - `build/reports/unified-spec/utf-differential-report.json`
  - `build/reports/unified-spec/utf-differential-report.md`
  - `build/reports/unified-spec/failure-replay-bundles/manifest.json`
- R3 failure ledger:
  - `build/reports/r3-failure-ledger/r3-failure-ledger.json`
  - `build/reports/r3-failure-ledger/r3-failure-ledger.md`
  - `build/reports/r3-failure-ledger/r3-failure-ledger-trend.md`
- Complex-query certification:
  - `build/reports/complex-query-certification/complex-query-certification.json`
  - `build/reports/complex-query-certification/complex-query-certification.md`
  - `build/reports/complex-query-certification/failure-replay-bundles/manifest.json`
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
- R3 canary:
  - `build/reports/r3-canary/r2-canary-certification.json`
  - `build/reports/r3-canary/r2-canary-certification.md`
  - `build/reports/r3-canary/r3-canary-gate-summary.md`
- Final readiness:
  - `build/reports/release-readiness/r1-final-readiness-report.json`
  - `build/reports/release-readiness/r1-final-readiness-report.md`

## Canary Input Format

`r2CanaryCertificationEvidence` and `r3CanaryCertificationEvidence` expect a JSON file with this shape:

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

## Related Docs

- `docs/FIXTURE_MANIFEST.md`
- `docs/FIXTURE_PLAYBOOK.md`
- `docs/SPRING_TESTING.md`
