# 09. R1 Replacement Reference Baseline

목표: `R1 (Real MongoDB Replacement Program)`에서 "몽고디비 대체 가능"을 판정할 때
근거로 삼을 기준 문서와 우선순위를 고정한다.

## 1) Canonical Reference Stack

### Tier A: Normative Specs (최상위 기준)

- MongoDB Specifications:
  - https://github.com/mongodb/specifications
- Unified Test Format:
  - https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/
- CRUD:
  - https://specifications.readthedocs.io/en/latest/crud/crud/
- Sessions:
  - https://specifications.readthedocs.io/en/latest/sessions/driver-sessions/
- Transactions:
  - https://specifications.readthedocs.io/en/latest/transactions/transactions/
- Retryable Writes:
  - https://specifications.readthedocs.io/en/latest/retryable-writes/retryable-writes/
- Read/Write Concern:
  - https://specifications.readthedocs.io/en/latest/read-write-concern/read-write-concern/
- Command Logging and Monitoring:
  - https://specifications.readthedocs.io/en/latest/command-logging-and-monitoring/command-logging-and-monitoring/
- SDAM:
  - https://specifications.readthedocs.io/en/latest/server-discovery-and-monitoring/server-discovery-and-monitoring/

R1에서 의미론 충돌이 발생하면 Tier A를 우선 적용한다.

### Tier B: Server Command/Wire References

- MongoDB Wire Protocol (OP_MSG):
  - https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/
- Command References:
  - find: https://www.mongodb.com/docs/manual/reference/command/find/
  - getMore: https://www.mongodb.com/docs/manual/reference/command/getMore/
  - killCursors: https://www.mongodb.com/docs/manual/reference/command/killcursors/
  - update: https://www.mongodb.com/docs/manual/reference/command/update/
  - createIndexes: https://www.mongodb.com/docs/manual/reference/command/createindexes/
  - commitTransaction: https://www.mongodb.com/docs/manual/reference/command/committransaction/
  - abortTransaction: https://www.mongodb.com/docs/manual/reference/command/abortTransaction/
- Error Codes:
  - https://www.mongodb.com/docs/v8.0/reference/error-codes/

Tier B는 command payload/response shape와 code/codeName 정합성 기준이다.

### Tier C: Driver / Spring Compatibility References

- MongoDB Java Driver:
  - https://github.com/mongodb/mongo-java-driver
- Spring Data MongoDB Sessions/Transactions:
  - https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html
- Spring Data MongoDB Query Methods:
  - https://docs.spring.io/spring-data/mongodb/reference/mongodb/repositories/query-methods.html
- Spring Data MongoDB Aggregation:
  - https://docs.spring.io/spring-data/mongodb/reference/mongodb/aggregation-framework.html

Tier C는 "실사용 프레임워크 경로에서의 drop-in 동작" 기준이다.

## 2) R1 Compatibility Target Profile

R1은 다음 범위를 "대체 가능" 기준으로 본다.

- 명령/프로토콜:
  - handshake/metadata + CRUD + cursor(getMore/killCursors) + transactions + indexes
- 의미론:
  - tier-1 query/update/transaction/error label parity
- 검증:
  - real `mongod` baseline differential + Spring compatibility matrix
- 운영품질:
  - flake/latency/startup/repro-time 게이트 자동화

R1 비포함 범위(의도적 제외):

- sharding, replication, auth/tls, change streams, distributed transaction

## 3) Conflict Resolution Rules

동일 동작에 대해 자료가 충돌하면 아래 우선순위를 사용한다.

1. Tier A 규범 스펙(`MUST/SHOULD` + 공식 spec tests)
2. real `mongod` 실측 결과(고정 버전 기준)
3. Tier B/C 문서 설명
4. 내부 구현 편의

## 4) Version Pinning Policy

R1 검증은 반드시 버전을 고정한다.

- Baseline `mongod` 버전: CI matrix에 명시(예: LTS 라인 + 최신 라인)
- Java Driver / Spring Data 버전: matrix로 고정
- Spec corpus commit: SHA로 고정하여 회귀 추적 가능하게 유지

## 5) Evidence Requirements (Release Readiness)

R1 최종 판정(#34)은 아래 아티팩트를 필수로 남긴다.

- differential parity report (`json`, `md`)
- spring compatibility matrix report (`json`, `md`)
- performance/stability gate report (`json`, `md`)
- diagnostics/repro evidence summary (`json`, `md`)

