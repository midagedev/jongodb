# 02. Transaction & Session Behavior Notes

목표: 트랜잭션과 세션에서 반드시 지켜야 할 서버 동작을 MVP 수준으로 고정.

## 1) 세션/트랜잭션 필드 규칙 (MUST 성격)

근거:
- Transactions Spec:
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)
- Sessions Spec:
  [https://specifications.readthedocs.io/en/latest/sessions/driver-sessions/](https://specifications.readthedocs.io/en/latest/sessions/driver-sessions/)

핵심 규칙:

- 트랜잭션 내 모든 커맨드는 `lsid`를 가져야 한다.
- `txnNumber`는 `startTransaction` 시점에 증가하고, 같은 트랜잭션 안에서는 동일 값을 유지한다.
- 트랜잭션 첫 커맨드는 `startTransaction: true`, `autocommit: false`를 포함한다.
- 트랜잭션 후속 커맨드는 `autocommit: false`는 유지하지만 `startTransaction`은 포함하지 않는다.
- `commitTransaction`/`abortTransaction`에도 동일한 `lsid`, `txnNumber`, `autocommit: false`가 포함된다.

## 2) Read Preference / Read Concern

근거:
- Transactions Spec (read preference 제약):
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)
- Read/Write Concern Spec:
  [https://specifications.readthedocs.io/en/latest/read-write-concern/read-write-concern/](https://specifications.readthedocs.io/en/latest/read-write-concern/read-write-concern/)

MVP에서 최소 보장할 내용:

- 트랜잭션 읽기는 우선 `primary` read preference 기준으로 맞춘다.
- 트랜잭션 첫 명령의 `readConcern` 적용 규칙을 구현한다.
- `readConcern: {}` (서버 기본값) 과 `readConcern: { level: "local" }` 를 동일 취급하면 안 된다.

## 3) 에러 라벨 동작

근거:
- Transactions Spec:
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)

애플리케이션 재시도와 직접 연결되는 규칙:

- `TransientTransactionError`:
  - 트랜잭션 실행 중 재시도 가능한 실패를 나타내는 라벨.
  - 해당 라벨이 붙으면 트랜잭션 전체 재시도 경로가 열림.
- `UnknownTransactionCommitResult`:
  - `commitTransaction` 결과를 확정할 수 없는 상황 라벨.
  - "커밋됐는지 모른다"라는 의미이므로 커밋 재시도 정책과 연결된다.

이 라벨 처리 정확도는 Spring/드라이버 상위 레벨 재시도 체감 품질에 직결된다.

## 4) Snapshot Session (확장 범위)

근거:
- Snapshot Sessions Spec:
  [https://specifications.readthedocs.io/en/latest/sessions/snapshot-sessions/](https://specifications.readthedocs.io/en/latest/sessions/snapshot-sessions/)

초기 설계 시 고려:

- snapshot session은 causal consistency와 동시에 켜지지 않는다.
- snapshot reads는 `atClusterTime` 획득/전파 규칙이 있다.
- snapshot session과 transaction 조합에는 제약이 있다.

MVP에서는 트랜잭션+일반 세션을 먼저 완료하고, snapshot을 확장 단계로 미루는 것이 현실적이다.

## 5) 구현 체크리스트 (엔진 관점)

- 세션 상태:
  - `NONE -> STARTED -> IN_PROGRESS -> COMMITTED/ABORTED` 식 상태 전이 명시
- 트랜잭션 컨텍스트:
  - `sessionId`, `txnNumber`, `startTs/readTs`, write set, locked namespace
- 커맨드 검증:
  - 첫 명령/후속 명령의 필드 차이 검증
  - 잘못된 조합(`startTransaction` 중복, 다른 `txnNumber`) 에러 처리
- commit/abort:
  - idempotent 재호출 시 동작 정의
  - 오류 라벨 매핑 정책 정의

