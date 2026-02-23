# 08. Detailed Success Criteria & Quality Gates

목표: "잘 만들었다"를 감으로 판단하지 않도록, 정량 지표와 게이트를 단계별로 고정.

## 1) Top-Level Success Definition

아래 4개를 동시에 만족하면 MVP 성공으로 본다.

1. Compatibility:
   - 목표 시나리오에서 mongod 대비 동작 일치율이 기준 이상
2. Debuggability:
   - 실패 원인 파악 시간이 기준 이하
3. Performance:
   - 테스트 체감 속도(기동/리셋/단건 응답)가 기준 이하
4. Stability:
   - 반복 실행에서 회귀/비결정성 기준 이하

## 2) Compatibility KPI

- Differential Pass Rate (핵심군):
  - 기준: `>= 95%`
  - 측정: 고정 시드 시나리오 N개, mongod vs jongodb 결과 비교
- Transaction Semantics Pass Rate:
  - 기준: `>= 90%` (MVP), 이후 `>= 97%`
  - 측정: commit/abort/isolation/error label 시나리오
- Error Mapping Accuracy:
  - 기준: `>= 98%`
  - 측정: code/codeName/errorLabels 조합 일치율
- Cursor Protocol Accuracy:
  - 기준: `>= 99%`
  - 측정: `cursor.id`, `firstBatch/nextBatch`, close 동작

## 3) Debuggability KPI

- Repro Time (P50/P95):
  - 기준: P50 `<= 5분`, P95 `<= 15분`
  - 측정: 실패 1건을 재현 스크립트로 재실행해 동일 실패 재현까지 시간
- Root-Cause Isolation Time:
  - 기준: P50 `<= 20분`, P95 `<= 60분`
  - 측정: 담당자가 "실패 모듈"을 특정하는 시간
- Diagnostic Completeness:
  - 기준: `>= 99%`
  - 측정: 실패 로그에 `requestId/sessionId/txnNumber/commandName` 포함 비율
- Invariant Violation Detection:
  - 기준: `100%`
  - 측정: 의도적 fault injection 시 invariant checker 탐지율

## 4) Performance KPI

통합테스트 체감 개선 중심 지표:

- Cold Start Time:
  - 기준: `<= 300ms` (테스트 JVM 내 서버 기동 완료)
- Namespace Reset Time:
  - 기준: `<= 20ms` (테스트 케이스 간 초기화)
- p95 Command Latency (MVP 명령군):
  - 기준: `<= 5ms` (`insert/find/update/delete`, 로컬 단일 프로세스)
- Throughput (단일 스레드 기준):
  - 기준: `>= 10k ops/s` (단순 CRUD 혼합 벤치)

## 5) Stability KPI

- Flake Rate:
  - 기준: `<= 0.5%`
  - 측정: 동일 테스트 200회 반복 시 비결정 실패 비율
- Soak Run:
  - 기준: 1시간 연속 실행 중 crash/oom/lockup `0건`
- Memory Growth:
  - 기준: steady-state에서 30분 구간 heap 증가율 `<= 5%`
- Resource Leak:
  - 기준: 종료 시 active session/cursor/lock `0`

## 6) Phase Gates (진입/탈출 조건)

Gate M0: Skeleton

- 필수:
  - `hello/ping/insert/find` E2E pass
  - basic command logging 동작
- 탈출:
  - cold start `<= 500ms`

Gate M1: CRUD

- 필수:
  - CRUD differential pass rate `>= 90%`
  - unique index 기본 충돌 처리
- 탈출:
  - p95 command latency `<= 8ms`

Gate M2: Transactions

- 필수:
  - txn/session 규칙 테스트 pass `>= 90%`
  - error label 정확도 `>= 95%`
- 탈출:
  - isolation 기본 시나리오 pass

Gate M3: Hardening

- 필수:
  - compatibility pass `>= 95%`
  - flake rate `<= 0.5%`
  - repro time P50 `<= 5분`
- 탈출:
  - CI에서 게이트 자동 통과

## 7) 자동 리포트 포맷 (권장)

매 실행마다 아래를 한 파일로 출력:

- build info: 커밋/브랜치/시드/실행시간
- KPI summary: 각 지표 현재값 + 기준 + pass/fail
- top regressions: 최근 실패 top 10 (시나리오, 모듈, 에러)
- diff samples: mongod vs jongodb 응답 비교 3건

파일 예시:
- `reports/quality-gate-YYYYMMDD-HHMM.json`
- `reports/quality-gate-YYYYMMDD-HHMM.md`

## 8) 실패 정책

- Gate 실패 시:
  - 신규 기능 병합 중지
  - 회귀 원인 우선 처리
- 반복 실패(3회 연속) 시:
  - 범위 축소 또는 아키텍처 보정 검토

## 9) 의사결정 규칙

- 지표 개선 없이 기능만 늘어나는 변경은 거절
- "속도 최적화"가 의미론 정확도를 낮추면 거절
- 디버깅 신호를 줄이는 변경은 거절

