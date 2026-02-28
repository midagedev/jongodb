# Runtime/Testkit Module Boundaries

`#222` 구조 분리 가이드입니다.

## 목적
- 런타임 코드와 검증 하네스(testkit) 코드를 모듈 단위로 분리합니다.
- 런타임 산출물에 `org.jongodb.testkit.*` 클래스가 포함되지 않도록 보장합니다.
- 런타임에서 testkit 패키지로 향하는 의존을 CI에서 빠르게 차단합니다.

## 모듈 구조
- 루트 프로젝트(`:`): runtime 모듈 (`src/main/java` 중 `org/jongodb/testkit/**` 제외)
- `:jongodb-testkit`: testkit 모듈 (`src/main/java/org/jongodb/testkit/**`)
- `:jongodb-spring-suite`: spring suite 모듈 (`testkit/spring-suite/src/main/java/**`)

## 실행 방식
- 기존 커맨드(`gradle fixtureRefresh`, `gradle r3FailureLedger`, `gradle springCompatibilityMatrixEvidence`)는 루트에서 그대로 실행됩니다.
- 해당 태스크들은 내부적으로 testkit/spring-suite 모듈 런타임 classpath를 사용합니다.

## 가드레일
- `RuntimeLayeringGuardTest`가 런타임 패키지에서 `org.jongodb.testkit..` 의존을 금지합니다.
- 위반 시 `gradle test`에서 실패합니다.
