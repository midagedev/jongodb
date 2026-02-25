# Spring Test Integration Guide

Status date: 2026-02-24

This guide explains how to use `jongodb` from Spring Boot tests and how to migrate common MongoDB Testcontainers setups.

## Supported Integration Styles

- Single-hook annotation: `org.jongodb.spring.test.JongodbMongoTest`
- `ApplicationContextInitializer`: `org.jongodb.spring.test.JongodbMongoInitializer`
- `@DynamicPropertySource`: `org.jongodb.spring.test.JongodbMongoDynamicPropertySupport`

Both styles wire:
- `spring.data.mongodb.uri`
- `spring.data.mongodb.host`
- `spring.data.mongodb.port`

## Quick Adoption

### 1) Add dependency

```kotlin
dependencies {
    testImplementation("io.github.midagedev:jongodb:<version>")
}
```

### 2) Replace test bootstrap

Single-hook annotation style (recommended for most Spring Boot tests):

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

Optional test database name:
- `@TestPropertySource(properties = "jongodb.test.database=<dbName>")` (single-hook/initializer style)
- `jongodb.test.database=<dbName>` (initializer style)
- `JongodbMongoDynamicPropertySupport.register(registry, "<dbName>")` (dynamic style)

Optional shared-server mode for initializer/single-hook style:
- `@TestPropertySource(properties = "jongodb.test.sharedServer=true")`
- `jongodb.test.sharedServer=true`

When enabled, multiple Spring contexts in the same JVM reuse one in-process TCP server.
The shared server closes automatically after the last shared context closes.

Isolation caveat:
- shared mode reuses process/port for speed, so prefer unique `jongodb.test.database` values per test class.
- keep shared mode disabled (default) if your suite requires strict per-context process isolation.

## Fast Reset API (No Context Restart)

For initializer/single-hook style tests, use `JongodbMongoResetSupport` to clear in-memory state
without rebuilding the Spring context:

```java
import org.jongodb.spring.test.JongodbMongoResetSupport;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;

class MyIntegrationTest {
  @Autowired
  ApplicationContext context;

  @BeforeEach
  void resetMongo() {
    JongodbMongoResetSupport.reset(context);
  }
}
```

Reset caveats:
- reset clears all databases in the backing in-memory server.
- avoid concurrent tests calling reset against the same shared server unless you explicitly coordinate isolation.

## Migration from MongoDB Testcontainers

### Before (typical pattern)

```java
@Container
static MongoDBContainer mongo = new MongoDBContainer("mongo:7");

@DynamicPropertySource
static void mongoProps(DynamicPropertyRegistry registry) {
  registry.add("spring.data.mongodb.uri", mongo::getReplicaSetUrl);
}
```

### After (jongodb pattern)

```java
@DynamicPropertySource
static void mongoProps(DynamicPropertyRegistry registry) {
  JongodbMongoDynamicPropertySupport.register(registry);
}
```

## Fit / Non-Fit

Use `jongodb` when:
- you need fast in-memory integration tests
- your tests focus on common CRUD, core aggregation, and single-process transaction paths

Keep Testcontainers/real `mongod` for tests that require:
- full MongoDB feature parity
- advanced transaction/retry semantics outside current support scope
- deployment-level behavior

## Transaction Contract (Current)

| Scenario | Support | Notes |
| --- | --- | --- |
| Single-template transactional path | Supported | `startTransaction` -> CRUD -> `commitTransaction` / `abortTransaction` |
| Multi-template same transaction manager path | Supported | Multiple namespaces can participate in one transaction envelope |
| Out-of-scope write, different namespace during open transaction | Supported | Non-transactional writes are preserved after commit |
| Out-of-scope write, same namespace with different `_id` | Supported | Non-transactional writes are preserved after commit |
| Same `_id` touched by both transactional and non-transactional writes | Deterministic | Commit applies the transactional version for that `_id` |

Not covered by this contract:
- distributed transaction semantics
- multi-node replica-set and deployment-level transaction behavior
- advanced commit retry semantics across real network/process failures

## Troubleshooting

- If you see `codeName=NotImplemented` with `UnsupportedFeature`, the test is using an unsupported feature path.
- For unsupported paths, keep a fallback profile that still runs the same test on Testcontainers/real `mongod`.
- If test isolation requires a clean DB per class, use a unique database name per test class.

## Related Docs

- `docs/COMPATIBILITY.md`
- `docs/SUPPORT_MATRIX.md`
- `docs/COMPATIBILITY_SCORECARD.md`
- `docs/USAGE.md`
