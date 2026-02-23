# 01. Spec Map (Primary Sources)

목표: 인메모리 Mongo 호환 엔진 구현 시, 어떤 공식 문서를 어떤 우선순위로 따라야 하는지 빠르게 결정하기 위한 맵.

## 1) 최우선 기준 (Driver Specifications)

MongoDB 드라이버 명세는 "드라이버가 서버에 무엇을 기대하는지"를 가장 직접적으로 정의한다.
Spring Data MongoDB도 결국 Java Driver 위에서 동작하므로, 서버 호환성의 실질 기준점으로 사용 가능하다.

- Specifications Home: [https://specifications.readthedocs.io/en/latest/](https://specifications.readthedocs.io/en/latest/)
- Repository: [https://github.com/mongodb/specifications](https://github.com/mongodb/specifications)

핵심 스펙:

- Transactions:
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)
- Driver Sessions:
  [https://specifications.readthedocs.io/en/latest/sessions/driver-sessions/](https://specifications.readthedocs.io/en/latest/sessions/driver-sessions/)
- Snapshot Sessions:
  [https://specifications.readthedocs.io/en/latest/sessions/snapshot-sessions/](https://specifications.readthedocs.io/en/latest/sessions/snapshot-sessions/)
- Read/Write Concern:
  [https://specifications.readthedocs.io/en/latest/read-write-concern/read-write-concern/](https://specifications.readthedocs.io/en/latest/read-write-concern/read-write-concern/)
- CRUD:
  [https://specifications.readthedocs.io/en/latest/crud/crud/](https://specifications.readthedocs.io/en/latest/crud/crud/)
- OP_MSG:
  [https://specifications.readthedocs.io/en/latest/message/OP_MSG/](https://specifications.readthedocs.io/en/latest/message/OP_MSG/)
- MongoDB Handshake:
  [https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/](https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/)
- find/getMore/killCursors:
  [https://specifications.readthedocs.io/en/latest/find_getmore_killcursors_commands/find_getmore_killcursors_commands/](https://specifications.readthedocs.io/en/latest/find_getmore_killcursors_commands/find_getmore_killcursors_commands/)
- Server Write Commands:
  [https://specifications.readthedocs.io/en/latest/server_write_commands/server_write_commands/](https://specifications.readthedocs.io/en/latest/server_write_commands/server_write_commands/)
- Unified Test Format:
  [https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/](https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/)

## 2) 서버 동작 의미론 기준 (MongoDB Manual)

드라이버 스펙이 "프로토콜 계약"이라면, 매뉴얼은 "연산 의미"를 정의한다.

기본 커맨드/프로토콜:

- Wire Protocol: [https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/](https://www.mongodb.com/docs/manual/reference/mongodb-wire-protocol/)
- `find`: [https://www.mongodb.com/docs/manual/reference/command/find/](https://www.mongodb.com/docs/manual/reference/command/find/)
- `getMore`: [https://www.mongodb.com/docs/manual/reference/command/getMore/](https://www.mongodb.com/docs/manual/reference/command/getMore/)
- `insert`: [https://www.mongodb.com/docs/manual/reference/command/insert/](https://www.mongodb.com/docs/manual/reference/command/insert/)
- `update`: [https://www.mongodb.com/docs/manual/reference/command/update/](https://www.mongodb.com/docs/manual/reference/command/update/)
- `delete`: [https://www.mongodb.com/docs/manual/reference/command/delete/](https://www.mongodb.com/docs/manual/reference/command/delete/)
- `createIndexes`: [https://www.mongodb.com/docs/manual/reference/command/createIndexes/](https://www.mongodb.com/docs/manual/reference/command/createIndexes/)
- `commitTransaction`: [https://www.mongodb.com/docs/manual/reference/command/commitTransaction/](https://www.mongodb.com/docs/manual/reference/command/commitTransaction/)
- `abortTransaction`: [https://www.mongodb.com/docs/manual/reference/command/abortTransaction/](https://www.mongodb.com/docs/manual/reference/command/abortTransaction/)
- `hello`: [https://www.mongodb.com/docs/manual/reference/command/hello/](https://www.mongodb.com/docs/manual/reference/command/hello/)

쿼리/타입/인덱스 의미:

- BSON Type Comparison Order:
  [https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/](https://www.mongodb.com/docs/manual/reference/bson-type-comparison-order/)
- Query Arrays:
  [https://www.mongodb.com/docs/manual/tutorial/query-arrays/](https://www.mongodb.com/docs/manual/tutorial/query-arrays/)
- Query Array of Documents:
  [https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/](https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/)
- Positional `$` Update:
  [https://www.mongodb.com/docs/manual/reference/operator/update/positional/](https://www.mongodb.com/docs/manual/reference/operator/update/positional/)
- `$elemMatch`:
  [https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/](https://www.mongodb.com/docs/manual/reference/operator/query/elemMatch/)
- Unique Index:
  [https://www.mongodb.com/docs/manual/core/index-unique/](https://www.mongodb.com/docs/manual/core/index-unique/)

트랜잭션/격리:

- Transactions:
  [https://www.mongodb.com/docs/manual/core/transactions/](https://www.mongodb.com/docs/manual/core/transactions/)
- Read Isolation, Consistency, and Recency:
  [https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/](https://www.mongodb.com/docs/manual/core/read-isolation-consistency-recency/)

## 3) Java/Spring 연동 기준

- MongoDB Java Driver Transactions:
  [https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/](https://www.mongodb.com/docs/drivers/java/sync/current/crud/transactions/)
- ClientSession API:
  [https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/session/ClientSession.html](https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/session/ClientSession.html)
- ClientSessionOptions API:
  [https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/ClientSessionOptions.html](https://mongodb.github.io/mongo-java-driver/5.6/apidocs/driver-core/com/mongodb/ClientSessionOptions.html)
- Spring Data MongoDB Transactions:
  [https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html](https://docs.spring.io/spring-data/mongodb/reference/mongodb/client-session-transactions.html)

## 4) 어떤 순서로 구현 기준을 고정할지

1. Driver Spec으로 `명령 필드 계약` 고정 (`lsid`, `txnNumber`, `autocommit`, 커맨드 구조)
2. MongoDB Manual로 `연산 의미` 고정 (쿼리/업데이트/인덱스/격리)
3. Java/Spring 문서로 `클라이언트 동작` 고정 (세션 시작/트랜잭션 API/예외 흐름)
4. Unified Tests + Differential Test로 실제 동작 검증

## 5) 라이선스 메모

- `mongodb/specifications` 저장소 문서는 `CC BY-NC-SA 3.0` 라이선스로 배포된다.
  - 참조: [https://github.com/mongodb/specifications/blob/master/LICENSE.md](https://github.com/mongodb/specifications/blob/master/LICENSE.md)
- 구현 코드 자체는 해당 문서 "아이디어/행동 규칙"을 따르되, 원문 복제는 최소화하는 것이 안전하다.

