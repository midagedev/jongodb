# 04. Java Driver & Spring Data Integration Notes

목표: "내 서버가 Java/Spring에서 실제로 어떻게 호출되는지"를 기준으로 호환성 우선순위를 정한다.

## 1) 공식 레퍼런스

- Java Driver Transactions:
  [https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/](https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/)
- ClientSession API:
  [https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/session/ClientSession.html](https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/session/ClientSession.html)
- Spring Data MongoDB Transactions:
  [https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html](https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html)

## 2) Spring 통합테스트 관점 최소 기대 동작

대부분의 Spring Data 사용 패턴은 다음 흐름을 따른다:

1. `MongoClient` 연결 및 handshake
2. collection/index metadata 조회
3. CRUD 및 aggregation
4. (옵션) `MongoTransactionManager`를 통한 세션/트랜잭션 경계

따라서 초기 호환 범위는 다음 3축이 핵심:

- 연결/핸드셰이크가 빠르게 성공할 것
- CRUD + cursor 동작이 일관될 것
- 트랜잭션 경계(`start/commit/abort`)와 오류 라벨이 드라이버 기대와 맞을 것

## 3) 실제 명령 집합을 자동 수집하는 방법 (권장)

문서 기반 추정보다, 현재 프로젝트의 통합테스트가 실제로 어떤 명령을 보내는지 먼저 수집하는 것이 효율적이다.

예시 (Java Driver CommandListener):

```java
MongoClientSettings settings = MongoClientSettings.builder()
    .addCommandListener(new CommandListener() {
        @Override
        public void commandStarted(CommandStartedEvent event) {
            System.out.println("CMD START: " + event.getCommandName() + " " + event.getCommand().toJson());
        }
        @Override
        public void commandSucceeded(CommandSucceededEvent event) {}
        @Override
        public void commandFailed(CommandFailedEvent event) {
            System.out.println("CMD FAIL: " + event.getCommandName() + " " + event.getThrowable().getMessage());
        }
    })
    .build();
```

이렇게 얻은 명령 로그를 기준으로 "반드시 구현할 커맨드"를 고정하면 과구현을 줄일 수 있다.

## 4) 트랜잭션 연동 시 주의 포인트

- 세션 없는 트랜잭션 호출은 즉시 오류 처리해야 한다.
- 트랜잭션 내 커맨드의 필드 조합 검사(`lsid`, `txnNumber`, `autocommit`, `startTransaction`)가 중요하다.
- commit 실패 시 라벨(`UnknownTransactionCommitResult`)이 상위 재시도 로직과 연결된다.

## 5) 성능 관점 (테스트 체감 속도)

언어보다 중요한 건 기동 전략이다:

- 테스트 JVM 프로세스 내 in-process 서버로 기동
- test suite 단위 1회 기동
- 테스트 케이스별 전체 재기동 대신 namespace reset
- Spring `ApplicationContext` 캐시 깨는 패턴 최소화

이 조합이 컨테이너/외부 바이너리 기반 대비 체감 시간을 크게 줄인다.

