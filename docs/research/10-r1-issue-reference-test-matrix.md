# 10. R1 Issue-to-Reference Test Matrix

목표: `#22~#34` 이슈를 병렬 서브에이전트로 진행할 때,
각 이슈의 레퍼런스/테스트/성공기준을 즉시 확인할 수 있게 고정한다.

## 1) Orchestration Waves

### Wave 0 (즉시 병렬)

- `#23` Protocol command surface parity
- `#25` Engine query parity
- `#30` Obs deterministic replay + invariant checker
- `#31` Testkit differential vs real mongod

### Wave 1 (선행 완료 후 병렬)

- `#24`, `#26`, `#27`, `#28`, `#29`, `#33`

### Wave 2 (통합/출시 판정)

- `#32`, `#34`

## 2) Issue Mapping

### #23 Protocol: Command Surface Parity

- Primary references:
  - Tier A CRUD/Sessions/Transactions specs
  - Tier B command references(find/update/createIndexes/transaction commands)
- Required tests:
  - command option matrix contract tests
  - error mapping tests(code/codeName/errorLabels)
  - unified format subset for command behavior
- Exit evidence:
  - protocol contract pass >= 300 cases
  - error mapping accuracy >= 98%

### #24 Protocol: Cursor Lifecycle + getMore/killCursors

- Primary references:
  - Tier B `find/getMore/killCursors`
  - Tier A unified test format cursor scenarios
- Required tests:
  - multi-batch cursor lifecycle tests
  - session-bound cursor cleanup tests
  - timeout/killCursors tests
- Exit evidence:
  - cursor protocol compatibility >= 99%

### #25 Engine: Query Operator Parity

- Primary references:
  - Tier A CRUD query semantics
  - Tier B query behavior docs
- Required tests:
  - operator matrix tests(eq/ne/gt/gte/lt/lte/in/nin/and/or/not/nor/exists/type/size/elemMatch/all/regex)
  - nested path + array-of-document matching tests
  - differential vs real mongod corpus
- Exit evidence:
  - query differential pass >= 97%

### #26 Engine: Update/Replace/Upsert Parity

- Primary references:
  - Tier A CRUD update/replace/upsert semantics
  - Tier B `update` command spec
- Required tests:
  - updateOne/updateMany/replaceOne behavior tests
  - upsert + upsertedId response tests
  - arrayFilters/positional core compatibility tests
- Exit evidence:
  - update differential pass >= 97%

### #27 Engine: Aggregation Pipeline Parity

- Primary references:
  - Tier B aggregate behavior docs
  - Tier C Spring aggregation references
- Required tests:
  - stage matrix(match/project/group/sort/limit/skip/unwind/count)
  - expression core tests
  - Spring Data aggregation compatibility tests
- Exit evidence:
  - aggregation differential pass >= 95%
  - Spring representative aggregation scenarios 100% pass

### #28 Engine: Index Parity + Constraint Semantics

- Primary references:
  - Tier B `createIndexes` + error code references
  - Tier A CRUD/index-related behavior from unified tests
- Required tests:
  - unique compound index constraint tests
  - sparse/partial/ttl subset tests
  - duplicate key edge-case differential tests
- Exit evidence:
  - index differential pass >= 97%
  - duplicate key edge regressions = 0

### #29 Txn: Session/Transaction Semantic Parity

- Primary references:
  - Tier A sessions/transactions/retryable-writes/read-write-concern specs
  - Tier B commit/abort command references
- Required tests:
  - state machine transition tests(lsid/txnNumber/autocommit/startTransaction)
  - commit retry / abort / no-such-transaction path tests
  - errorLabels mapping tests
- Exit evidence:
  - transaction semantics pass >= 97%
  - error label accuracy >= 98%

### #30 Obs: Deterministic Replay + Invariant Checker

- Primary references:
  - Tier A command monitoring spec
  - internal diagnostics architecture docs (`06-architecture-debuggability.md`)
- Required tests:
  - journal/snapshot/repro determinism tests
  - invariant checker fault-injection tests
  - replay-to-same-failure reproducibility tests
- Exit evidence:
  - repro time P50 <= 2분
  - diagnostics completeness >= 99%

### #31 Testkit: Differential vs Real mongod (Nightly)

- Primary references:
  - Tier A unified test format
  - mongo/specifications official test corpus
- Required tests:
  - real mongod adapter integration tests
  - nightly tier-1 corpus run(2000+ scenarios)
  - top regression extraction tests
- Exit evidence:
  - differential report artifacts generated every CI run
  - parity summary tracked historically

### #32 Testkit: Spring Data Mongo Compatibility Matrix

- Primary references:
  - Tier C Spring Data Mongo docs
  - Java driver behavior references
- Required tests:
  - MongoTemplate/Repository/TransactionTemplate suites
  - Spring Boot version matrix(2+ lines)
  - failure minimization output tests
- Exit evidence:
  - Driver + Spring compatibility pass >= 98%

### #33 Orchestrator: Performance + Stability SLA Gates

- Primary references:
  - `08-success-criteria.md` 성능/안정성 지표
- Required tests:
  - cold start/reset/p95 latency/throughput benchmarks
  - flake rate repeated-run tests
  - soak + memory growth + leak checks
- Exit evidence:
  - cold start <= 150ms, reset <= 10ms, CRUD p95 <= 5ms
  - flake <= 0.2%, 1h soak crash/lockup = 0

### #34 Orchestrator: Final Readiness + Cutover Playbook

- Primary references:
  - `#22` Program Exit Gate
  - all child issue evidence artifacts
- Required tests:
  - full gate aggregation validation tests
  - cutover/rollback rehearsal on canary projects
- Exit evidence:
  - all #22 gates PASS
  - canary 2개 이상 전환 검증 완료

## 3) Sub-agent Working Contract

각 이슈를 서브에이전트에 할당할 때 아래 형식을 유지한다.

- Scope lock:
  - 소유 패키지 외 변경 금지
- Done contract:
  - 구현 + 테스트 + 수치 증빙 + GH 코멘트 초안까지 제출
- Hand-off:
  - 변경 파일 목록, 실행 커맨드, 통과 로그 요약, 남은 리스크

## 4) Immediate Start Checklist (Wave 0)

- `#23` command option matrix 뼈대 + 오류 매핑 테이블 초안
- `#25` query operator matrix와 differential seed corpus 확장
- `#30` invariant checker 인터페이스/기본 규칙 구현
- `#31` real mongod baseline adapter 스켈레톤 + nightly artifact 경로 고정

