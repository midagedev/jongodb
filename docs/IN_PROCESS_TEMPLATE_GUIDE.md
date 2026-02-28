# In-Process Integration Template Guide

`#261` 구현 가이드입니다.

## 제공 기능
- JUnit/Spring 진입점
  - `org.jongodb.testkit.InProcessIntegrationTemplate`
  - `@JongodbInProcessTest` + `JongodbInProcessInitializer`
- fixture helper
  - `seedDocuments(database, collection, documents)`
  - `findAll(database, collection)`
  - `reset()`
  - `JongodbInProcessResetSupport.reset(context)`
- 실패 시 trace artifact 자동 수집(옵션)
  - snapshot / invariant / triage / repro / response 파일 생성

## JUnit 사용 예

```java
InProcessIntegrationTemplate template = new InProcessIntegrationTemplate();
template.seedDocuments("app", "users", List.of(BsonDocument.parse("{\"_id\":1}")));
BsonDocument result = template.findAll("app", "users");
template.reset();
```

## Spring 사용 예

```java
@ExtendWith(SpringExtension.class)
@JongodbInProcessTest(classes = TestConfig.class)
class SampleTest {
  @Autowired InProcessIntegrationTemplate template;
}
```

## Trace Artifact 옵션
- Spring Property
  - `jongodb.inprocess.traceArtifacts.enabled=true`
  - `jongodb.inprocess.traceArtifacts.dir=<outputDir>`
- 실패 명령 발생 시 `<outputDir>` 아래 파일 생성:
  - `*-snapshot.json`
  - `*-invariant.json`
  - `*-triage.json`
  - `*-repro.jsonl`
  - `*-response.json`
