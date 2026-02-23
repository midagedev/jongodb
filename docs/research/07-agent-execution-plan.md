# 07. Agent-Friendly Execution Plan

목표: 여러 에이전트가 충돌 없이 병렬 개발하고, 각 단계가 명확한 완료조건을 갖도록 작업을 구조화.

## 1) 작업 운영 규칙

- 단일 책임 원칙:
  - 한 에이전트는 한 bounded context만 소유.
- 인터페이스 우선:
  - 구현 전에 인터페이스/계약(입력, 출력, 에러)을 먼저 확정.
- 얕은 PR:
  - 한 PR은 한 가지 목적만 담는다.
- 계약 테스트 우선:
  - 모듈 단위 unit test + 계약 테스트가 통과해야 다음 단계 진행.

## 2) 에이전트 트랙 분리

Track A: Protocol & Command Ingress
- 범위:
  - `wire`, `command` 패키지
- 산출물:
  - OP_MSG decode/encode, command dispatcher, validation skeleton
- 완료조건:
  - `hello`, `ping`, unknown command error 처리

Track B: Core Engine
- 범위:
  - `engine`, `index`
- 산출물:
  - document store, query matcher, update applier, unique index enforcement
- 완료조건:
  - insert/update/delete/find 단일 스레드 정합성 통과

Track C: Session/Transaction
- 범위:
  - `txn`, `cursor`
- 산출물:
  - session pool, txn state machine, commit/abort, cursor lifecycle
- 완료조건:
  - `lsid/txnNumber/autocommit/startTransaction` 규칙 통과

Track D: Observability & Diagnostics
- 범위:
  - `obs`
- 산출물:
  - structured logging, command journal, snapshot dump, invariant checker
- 완료조건:
  - 실패 시 5분 내 재현 가능한 진단 정보 자동 수집

Track E: Validation Harness
- 범위:
  - `testkit`
- 산출물:
  - differential runner, unified format subset runner, report generator
- 완료조건:
  - 핵심 시나리오 pass/fail diff 리포트 생성

## 3) 단계별 일정 (권장)

Phase 0 (1주): Skeleton

- A: handshake + dispatcher
- B: in-memory collection + insert/find
- E: baseline differential harness 골격

Exit Gate:
- `hello/ping/insert/find` end-to-end 통과

Phase 1 (1~2주): CRUD Core

- B: update/delete/query operators 핵심
- A: write/read command 응답 형식 정합화
- E: CRUD differential suite 확장

Exit Gate:
- CRUD 핵심 시나리오 pass rate 90%+

Phase 2 (1~2주): Transactions

- C: session + txn state machine
- A: txn field validation
- E: transaction unified tests 편입

Exit Gate:
- commit/abort/isolation 기본 시나리오 통과

Phase 3 (1주): Hardening

- D: diagnostics 완성
- E: 리포트 자동화 + 회귀 파이프라인
- B/C: conflict/edge case 보강

Exit Gate:
- 회귀 테스트 안정성 + 디버깅 SLA 충족

## 4) 작업 항목 템플릿 (모든 태스크 공통)

- Context:
  - 해결할 문제 1문장
- Scope:
  - 수정 가능한 패키지/파일
- Contract:
  - 입력/출력/오류 형식
- Tests:
  - unit + integration + differential 케이스
- Definition of Done:
  - 통과 기준 숫자
- Non-goals:
  - 이번 작업에서 의도적으로 제외한 범위

## 5) 의존성/충돌 관리

- 병렬 가능:
  - B와 D, C와 E 일부
- 선행 필요:
  - A dispatcher 인터페이스 고정 후 B/C 연동
- 충돌 방지:
  - 공용 DTO/오류코드 파일은 전담 에이전트 1명만 수정

## 6) 실패 시 롤백/복구 규칙

- 기능 플래그로 신규 경로를 분리
- differential regression 발생 시 즉시 이전 플래그 경로로 회귀
- 원인 분석 전까지 신규 기능 확장 중단

## 7) 리뷰 체크리스트

- 모듈 경계 위반 없는가
- 에러 코드/라벨 정책과 일치하는가
- 재현 로그가 충분한가
- 테스트가 계약을 실제로 보호하는가
- 성능 최적화가 의미론을 깨지 않는가

