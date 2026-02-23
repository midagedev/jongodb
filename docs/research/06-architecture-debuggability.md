# 06. Hardened Architecture for Debuggability

목표: 기능 추가보다 "문제 재현, 원인 격리, 안전한 수정"이 쉬운 구조를 먼저 확보한다.

## 1) 핵심 설계 원칙

- Deterministic First:
  - 같은 입력(명령 순서, 시드, 시간 소스)에는 항상 같은 결과.
  - 테스트 모드에서 시간/UUID/랜덤 소스를 주입 가능하게 설계.
- Explicit State:
  - 세션, 트랜잭션, 커서 상태는 모두 명시적 상태머신으로 관리.
  - 암묵적 side effect 금지.
- Fail Fast:
  - 미지원 기능은 조용히 무시하지 않고 명시적 에러 반환.
  - 잘못된 필드 조합은 파싱 단계에서 즉시 차단.
- Observable by Default:
  - 명령 시작/종료/오류, 상태전이, 잠금 충돌 정보를 구조화 로그로 남김.

## 2) 모듈 경계 (권장 패키지)

```text
org.jongodb
  .wire          // OP_MSG codec, framing, handshake
  .command       // command dispatcher, argument validation
  .engine        // document store, query matcher, update applier
  .txn           // session/txn state machine, MVCC, commit protocol
  .index         // index catalog, unique enforcement
  .cursor        // cursor registry, getMore/killCursors lifecycle
  .error         // error codes, labels, mapping policy
  .obs           // logging, tracing, metrics, debug snapshot
  .testkit       // differential harness, unified runner adapters
```

규칙:

- `wire`는 BSON/프로토콜만 알고 도메인 규칙을 모른다.
- `engine`은 네트워크를 모른다.
- `command`가 입력 검증/라우팅의 단일 진입점이다.
- `txn` 상태/격리 책임을 `engine`에서 분리한다.

## 3) 요청 처리 파이프라인

1. Decode:
   - OP_MSG decode + 기본 형식 검증
2. Normalize:
   - command name, namespace, 옵션 정규화
3. Validate:
   - 필수 필드/금지 조합 검증 (`lsid`, `txnNumber`, `autocommit`, `startTransaction`)
4. Execute:
   - command handler -> engine/txn/index/cursor 호출
5. Encode:
   - 성공/실패 응답 문서 구성
6. Emit Observability:
   - 구조화 로그 + 지표 기록 + 필요 시 state snapshot

각 단계가 명시적으로 나뉘어 있으면 디버깅 시 실패 지점을 즉시 특정할 수 있다.

## 4) 디버깅 친화 기능 (필수)

- Correlation ID:
  - 모든 요청에 `requestId`, `sessionId`, `txnNumber`, `cursorId`를 로그 키로 고정.
- Command Journal:
  - 마지막 N개 명령의 입력/결과/오류를 링버퍼로 보관.
- State Snapshot:
  - 특정 실패 시점의 세션/트랜잭션/커서/인덱스 카탈로그를 dump 가능.
- Repro Script Export:
  - journal에서 단일 재현 시나리오(JSON lines) 자동 추출.
- Invariant Checks:
  - commit 후 orphan lock/dirty cursor/invalid txn state 탐지 assert.

## 5) 에러 모델 표준화

목표: 어디서 실패해도 오류 구조가 일관되게 보이게 한다.

- 표준 필드:
  - `ok`, `code`, `codeName`, `errmsg`, `errorLabels`
- 내부 예외 분류:
  - `ProtocolError`, `ValidationError`, `ConcurrencyError`, `StorageError`, `NotSupportedError`
- 매핑 규칙:
  - 내부 예외 -> Mongo 스타일 오류 문서 매핑 테이블 고정
  - 트랜잭션 실패 시 라벨 정책(`TransientTransactionError`, `UnknownTransactionCommitResult`) 중앙화

## 6) 동시성 모델

초기 권장:

- global lock 모델로 시작하지 말고, 최소 `namespace` 단위 잠금으로 시작.
- 읽기 일관성은 MVCC read timestamp로 처리.
- commit 시 write-write conflict 검사 후 원자 반영.

디버깅을 위해 필수 로그:

- lock 획득/대기/타임아웃
- conflict 대상 문서 키
- commit/abort 이유

## 7) 변경 안정성 전략

- Feature Flags:
  - 신규 동작은 flag behind로 배포, default off -> test soak -> on
- Golden Tests:
  - 고정 시나리오에 대한 응답 스냅샷 관리
- Differential Gate:
  - 변경 PR마다 핵심 시나리오를 mongod와 비교

## 8) 운영 모드 vs 테스트 모드

- test mode:
  - deterministic clock, verbose logs, invariant hard fail
- normal mode:
  - lightweight logs, metric sampling

모드 분리가 없으면 성능/가시성 요구가 충돌한다.

