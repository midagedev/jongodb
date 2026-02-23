# 03. Wire Protocol & Command Compatibility

목표: "Spring Data + Java Driver 통합테스트가 붙을 수 있는 최소 서버 표면"을 정의.

## 1) Wire Protocol 최소 범위

근거:
- OP_MSG Spec:
  [https://specifications.readthedocs.io/en/latest/message/OP_MSG/](https://specifications.readthedocs.io/en/latest/message/OP_MSG/)
- Handshake Spec:
  [https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/](https://specifications.readthedocs.io/en/latest/mongodb-handshake/handshake/)

MVP 결론:

- `OP_MSG`만 지원해도 충분한 시작점이 될 수 있다.
- 초기 handshake는 `hello`(또는 legacy 경로 고려) 응답이 가능해야 한다.
- 클라이언트가 판단하는 주요 capability 값(`maxWireVersion`, `maxBsonObjectSize`, `maxMessageSizeBytes`, `maxWriteBatchSize`, `logicalSessionTimeoutMinutes`)을 일관되게 제공해야 한다.

## 2) 커맨드 최소셋 (트랜잭션 없는 CRUD)

근거:
- find/getMore/killCursors Spec:
  [https://specifications.readthedocs.io/en/latest/find_getmore_killcursors_commands/find_getmore_killcursors_commands/](https://specifications.readthedocs.io/en/latest/find_getmore_killcursors_commands/find_getmore_killcursors_commands/)
- Server Write Commands Spec:
  [https://specifications.readthedocs.io/en/latest/server_write_commands/server_write_commands/](https://specifications.readthedocs.io/en/latest/server_write_commands/server_write_commands/)
- CRUD Spec:
  [https://specifications.readthedocs.io/en/latest/crud/crud/](https://specifications.readthedocs.io/en/latest/crud/crud/)

필수:

- `find`, `getMore`, `killCursors`
- `insert`, `update`, `delete`
- `createIndexes`
- `listCollections`, `listIndexes` (메타데이터 조회 안정성)
- `drop`, `dropDatabase` (테스트 fixture 정리)

## 3) 트랜잭션 최소셋

근거:
- Transactions Spec:
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)

필수:

- `commitTransaction`
- `abortTransaction`
- 트랜잭션 내 CRUD 명령의 필드/상태 검증 (`lsid`, `txnNumber`, `autocommit`, `startTransaction`)

## 4) 커맨드 응답 형식에서 흔히 놓치는 항목

구현 초기에 누락되기 쉬운 포인트:

- write commands:
  - `ok`만으로 충분하지 않다.
  - `n`, `nModified`, `upserted`, `writeErrors`, `writeConcernError` 형태를 드라이버 기대에 맞춰야 한다.
- cursor commands:
  - `cursor.id`, `firstBatch`/`nextBatch`, `ns` 필드 형식 유지
- 오류 응답:
  - `code`, `codeName`, `errmsg`, `errorLabels` 조합 필요

## 5) 핸드셰이크 응답 체크리스트

- `isWritablePrimary` 또는 호환 필드
- `maxWireVersion`
- `maxBsonObjectSize`
- `maxMessageSizeBytes`
- `maxWriteBatchSize`
- `logicalSessionTimeoutMinutes`
- `ok: 1`

주의:
- capability 값을 "임의 상수"로 고정하면 드라이버 분기에서 예상치 못한 코드 경로로 빠질 수 있다.
- 테스트용 서버라도 내부 지원 범위와 capability 응답은 일치시켜야 디버깅 비용이 줄어든다.

## 6) 구현 단계별 커맨드 오픈 순서

1. bootstrap: `hello`, `ping`, `buildInfo` (필요 시)
2. CRUD: `insert/update/delete/find/getMore/killCursors`
3. index/meta: `createIndexes/listIndexes/listCollections`
4. transaction: `commitTransaction/abortTransaction` + 트랜잭션 필드 검증
5. polish: 오류 라벨, writeConcern/readConcern edge cases

