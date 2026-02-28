# Phase R4 Issue Resolution (2026-02-28)

This document records explicit close decisions for remaining `phase:r4` issues after the R3 UTF
expansion closure wave.

## Decision Summary

- Closed as `not planned` (deferred from current execution window):
  - `#222` `[arch][build] Split runtime and testkit modules with layering guardrails`
  - `#228` `[EPIC][arch] Architecture hardening follow-ups from repository review`
  - `#250` `[fixture] 운영 Mongo 안전 추출 CLI 구현`
  - `#251` `[fixture] deterministic 비식별/정규화 파이프라인 구현`
  - `#252` `[fixture] Portable + Fast 이중 아티팩트 포맷 도입`
  - `#253` `[fixture] restore/reset/seed 공통 실행 유틸 구현`
  - `#254` `[fixture] refresh workflow(full/incremental) + 승인 게이트`
  - `#255` `[fixture] 드리프트 지표/리포트 및 임계치 정책`
  - `#256` `[fixture] artifact 버전/보관/호환 거버넌스`
  - `#257` `[fixture] CI/Nightly 자동화 파이프라인 구축`
  - `#248` `[EPIC][fixture] 운영 연계 fixture 생성/갱신/복원 lifecycle 구축`
  - `#93` `[perf][txn] Implement copy-on-write transaction snapshots`
  - `#92` `[EPIC][perf] Reduce jongodb runtime overhead for integration-test workloads`
  - `#260` `[testkit][poc] In-Process 템플릿 벤치마크 + 트레이스 분석 유효성 검증 (Go/No-Go)`
  - `#261` `[testkit] In-Process 통합테스트 템플릿 구현 (POC Go 이후)`
  - `#259` `[EPIC][testkit] In-Process 통합테스트 템플릿 (속도+트레이스 분석)`

## Rationale

- Current execution focus was completion of UTF/R3 compatibility expansion and closure of its linked
  execution issues.
- The R4 backlog above contains cross-cutting architecture/performance/fixture programs that require
  dedicated milestone bandwidth and separate acceptance runs.
- These issues are intentionally closed now to keep the active tracker empty and avoid stale/open
  backlog drift.

## Re-entry Rule

- If any item is re-prioritized, re-open with a new execution issue that includes:
  - bounded scope,
  - measurable acceptance checks,
  - CI/evidence commands required for merge.
