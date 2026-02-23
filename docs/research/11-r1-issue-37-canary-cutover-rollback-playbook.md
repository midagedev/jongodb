# 11. Issue #37 Prep: Canary Cutover + Rollback Playbook

목표: Spring 프로젝트 `2개 이상`에서 `mongod -> jongodb` 전환을 안전하게 검증하고, 실패 시 즉시 복구 가능한 절차를 고정한다.

## 1) Done 기준

- 최소 2개 Spring 프로젝트 canary 전환 완료
- 각 프로젝트에서 `10% -> 50% -> 100%` 단계 게이트 통과
- 각 프로젝트에서 rollback 리허설 1회 이상 성공
- 증빙 체크리스트(아래 표) 전 항목 채움

## 2) Canary 대상 선정 (최소 2개)

| 프로젝트 | Spring 범위 | 필수 커버리지 |
| --- | --- | --- |
| Project A | `MongoTemplate` + `MongoTransactionManager` | CRUD + transaction(commit/abort) + index |
| Project B | `Repository` + Aggregation | query method + aggregation + pagination/cursor |
| Project C (옵션) | Reactive(`ReactiveMongoTemplate`) | reactive CRUD + timeout/retry |

선정 규칙:
- 서로 다른 접근 패턴 2개 이상(Template/Repository/Reactive 중 2개 이상)
- 트래픽/테스트 볼륨이 충분해 회귀를 관찰할 수 있을 것

## 3) Pre-cutover 게이트 (T-2d ~ T-0)

각 프로젝트별로 아래를 모두 만족해야 cutover 시작:

- `mongod` 기준선 테스트 PASS 및 리포트 보관
- 동일 테스트를 `jongodb`로 실행해 PASS율 `>= 98%`
- transaction/error label 핵심 케이스 PASS율 `>= 97%`
- 주요 API/테스트 p95 latency 열화 `<= 20%`
- rollback 경로(설정 플래그/연결 문자열) 사전 점검 완료

## 4) Cutover 절차 (프로젝트별 반복)

### Step 0. Baseline 캡처

1. 현재 `mongod` 설정으로 스모크 + 핵심 통합테스트 실행
2. 결과(성공률, 에러 유형, p95 latency) 저장
3. 동일 커밋/동일 테스트 세트로 `jongodb` 재실행

### Step 1. 10% Canary

1. 트래픽/잡/테스트 대상의 10%만 `jongodb`로 라우팅
2. 30분 이상 관찰
3. 아래 조건 모두 충족 시 다음 단계:
   - 신규 P1 이상 에러 없음
   - 트랜잭션 실패율 증가 `<= 1%p`
   - 응답 p95 열화 `<= 20%`

### Step 2. 50% Canary

1. 50%로 확장 후 60분 이상 관찰
2. Step 1과 동일 조건 + 메모리/커서 누수 없음 확인

### Step 3. 100% Cutover

1. 100% 전환 후 최소 24시간 관찰
2. daily regression suite 1회 이상 PASS
3. 완료 시 프로젝트별 sign-off 기록

## 5) Rollback 트리거와 실행

즉시 rollback 조건(하나라도 충족 시):

- P1 이상 장애 발생
- 데이터 정합성 오류(누락/중복/트랜잭션 경계 위반) 확인
- 트랜잭션 실패율 급증(`> 3%p`)
- 서비스 핵심 API p95 열화 `> 30%`가 10분 이상 지속

Rollback 실행 순서(목표 복구시간: 10분 이내):

1. 추가 배포/실험 중지
2. 라우팅 또는 연결 설정을 `mongod`로 즉시 복귀
3. 애플리케이션 재기동(필요 시 롤링)
4. 스모크 테스트(CRUD + transaction 1세트) 즉시 실행
5. 장애 구간 로그/메트릭/요청 샘플 증빙 저장
6. RCA 액션 아이템 생성 후 재시도 일정 확정

## 6) Evidence 체크리스트 (2개 이상 프로젝트 필수)

증빙 파일 권장 경로: `docs/research/evidence/issue-37/<project>/`

| 항목 | Project A | Project B | Project C (옵션) |
| --- | --- | --- | --- |
| 프로젝트 정보(repo, branch, commit, Spring Boot/Data 버전) | [ ] | [ ] | [ ] |
| `mongod` baseline 테스트 리포트 링크 | [ ] | [ ] | [ ] |
| `jongodb` baseline 테스트 리포트 링크 | [ ] | [ ] | [ ] |
| 10% canary 관찰 로그/대시보드 링크 | [ ] | [ ] | [ ] |
| 50% canary 관찰 로그/대시보드 링크 | [ ] | [ ] | [ ] |
| 100% cutover 24h 안정성 리포트 링크 | [ ] | [ ] | [ ] |
| rollback 리허설 실행 기록(시각, 소요시간, 결과) | [ ] | [ ] | [ ] |
| rollback 후 스모크 테스트 PASS 증빙 | [ ] | [ ] | [ ] |
| 최종 승인자/승인 시각 | [ ] | [ ] | [ ] |

최종 판정 규칙:
- Project A, B가 모두 체크리스트 완료되면 Issue #37 준비 완료로 판정
- 하나라도 미완료면 cutover 완료로 인정하지 않음
