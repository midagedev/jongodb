# jongodb Research Notes

이 디렉터리는 Java 기반 인메모리 MongoDB 호환 엔진을 구현하기 위한 사전 리서치 문서 모음이다.

## 문서 구성

- [`01-spec-map.md`](./01-spec-map.md): 공식 명세/문서 레퍼런스 카탈로그
- [`02-transaction-session-behavior.md`](./02-transaction-session-behavior.md): 트랜잭션/세션 핵심 동작 규칙
- [`03-wire-protocol-command-compat.md`](./03-wire-protocol-command-compat.md): 와이어 프로토콜/커맨드 최소 호환 범위
- [`04-java-spring-integration.md`](./04-java-spring-integration.md): Java Driver + Spring Data 연동 관점 정리
- [`05-test-strategy.md`](./05-test-strategy.md): differential/unified test 기반 검증 전략
- [`06-architecture-debuggability.md`](./06-architecture-debuggability.md): 단단하고 디버깅 친화적인 아키텍처
- [`07-agent-execution-plan.md`](./07-agent-execution-plan.md): 에이전트 병렬 작업 계획
- [`08-success-criteria.md`](./08-success-criteria.md): 정량 성공기준과 단계별 품질 게이트
- [`09-r1-replacement-reference-baseline.md`](./09-r1-replacement-reference-baseline.md): R1 대체 목표의 공식 레퍼런스 기준선
- [`10-r1-issue-reference-test-matrix.md`](./10-r1-issue-reference-test-matrix.md): R1 이슈별 레퍼런스/테스트/게이트 매핑
- [`11-r1-issue-37-canary-cutover-rollback-playbook.md`](./11-r1-issue-37-canary-cutover-rollback-playbook.md): Issue #37 canary 전환/롤백 실행 플레이북
- [`12-r2-scorecard-manifest.md`](./12-r2-scorecard-manifest.md): R2 scorecard/support manifest 산출 규격
- [`13-r2-canary-certification.md`](./13-r2-canary-certification.md): R2 외부 canary 인증 산출 규격
- [`14-r3-issue-50-protocol-parity-baseline.md`](./14-r3-issue-50-protocol-parity-baseline.md): R3 failure ledger 기준 protocol parity baseline

## 권장 읽기 순서

1. `01-spec-map.md`로 공식 소스 지형 파악
2. `02-transaction-session-behavior.md`로 서버 의미론 핵심 확정
3. `03-wire-protocol-command-compat.md`로 초기 호환 범위 정의
4. `04-java-spring-integration.md`로 실제 Spring 테스트 연동 요구사항 반영
5. `05-test-strategy.md`로 구현-검증 루프 구축
6. `06-architecture-debuggability.md`로 구현 구조 고정
7. `07-agent-execution-plan.md`로 병렬 실행 계획 적용
8. `08-success-criteria.md`로 품질 게이트/성공기준 운영
9. `09-r1-replacement-reference-baseline.md`로 R1 판정 기준 고정
10. `10-r1-issue-reference-test-matrix.md`로 이슈 실행/검증 매핑 적용
11. `11-r1-issue-37-canary-cutover-rollback-playbook.md`로 canary cutover/rollback 증빙 운영
12. `12-r2-scorecard-manifest.md`로 R2 산출물 포맷 고정
13. `13-r2-canary-certification.md`로 외부 canary 인증 기준 고정
14. `14-r3-issue-50-protocol-parity-baseline.md`로 R3 protocol baseline 확인

## 리서치 스냅샷

- MongoDB Specifications 저장소 기준 커밋:
  - `99704fa8860777da1d919ef765af1e41e75f5859`
  - 커밋 시각: `2026-02-20 15:05:34 -0500`
