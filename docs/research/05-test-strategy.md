# 05. Test Strategy (Differential + Unified Spec Tests)

목표: "mongod와 동일 입력에서 동일 결과"를 빠르게 측정할 수 있는 검증 루프 구성.

## 1) 전략 요약

권장 방식은 두 축 병행:

- Unified Spec Tests 재사용:
  - MongoDB 공식 테스트 케이스를 최대한 그대로 소비
- Differential Testing:
  - 동일 테스트/명령을 `mongod`와 `jongodb`에 각각 실행 후 결과 비교

이 조합이 구현 누락과 의미론 편차를 가장 빨리 드러낸다.

## 2) Unified 테스트 소스 (커밋 고정)

기준 커밋:
- `99704fa8860777da1d919ef765af1e41e75f5859`

주요 디렉터리:

- transactions tests:
  [https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/transactions/tests/unified](https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/transactions/tests/unified)
- sessions tests:
  [https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/sessions/tests](https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/sessions/tests)
- CRUD tests:
  [https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/crud/tests/unified](https://github.com/mongodb/specifications/tree/99704fa8860777da1d919ef765af1e41e75f5859/source/crud/tests/unified)
- unified format schema:
  [https://github.com/mongodb/specifications/blob/99704fa8860777da1d919ef765af1e41e75f5859/source/unified-test-format/schema-latest.json](https://github.com/mongodb/specifications/blob/99704fa8860777da1d919ef765af1e41e75f5859/source/unified-test-format/schema-latest.json)

우선 추천 파일:

- transactions: `insert.yml`, `update.yml`, `delete.yml`, `commit.yml`, `abort.yml`, `isolation.yml`
- sessions: `driver-sessions-server-support.yml`, `snapshot-sessions.yml`
- CRUD: `bulkWrite-arrayFilters.yml`, `aggregate.yml`, `find*.yml`

## 3) Differential 실행 규칙

같은 케이스를 두 백엔드에 실행:

- A: 실제 `mongod` (참조 구현)
- B: `jongodb` (대상 구현)

비교 대상:

- 성공/실패 여부
- 오류 코드/코드명/라벨
- 결과 문서(정렬/타입 정규화 후)
- write result (`n`, `nModified`, `upserted`, `writeErrors`)
- cursor 관련 필드 (`cursor.id`, `firstBatch/nextBatch`)

정규화 권장:

- 문서 키 순서는 BSON 비교 규칙과 별개로 비교기에 맞게 정규화
- 수치 타입(Int32/Int64/Double/Decimal128) 비교 정책 명시
- 에러 메시지 문자열은 전체 일치 대신 핵심 속성 우선 (`code`, `errorLabels`)

## 4) 단계별 통과 기준

M0 (Bootstrap):

- handshake + `ping` + 단일 collection CRUD 통과

M1 (CRUD core):

- CRUD unified 기본군 통과
- `find/getMore/killCursors` 흐름 검증

M2 (Transactions):

- 트랜잭션 unified 기본군 통과 (`insert/update/delete/commit/abort/isolation`)
- 라벨 동작 (`TransientTransactionError`, `UnknownTransactionCommitResult`) 검증

M3 (Stability):

- 랜덤 시나리오 기반 differential fuzz (필터/업데이트 조합)
- 장시간 반복에서 세션/커서 누수 없음

## 5) 구현 실무 팁

- 실패 시 "명령 로그 + 응답 + 세션/트랜잭션 상태"를 한 묶음으로 저장
- unified 포맷 로더를 먼저 만들고, 미지원 operation은 스킵 목록으로 명시 관리
- CI는 빠른 subset, 로컬/야간은 full suite로 분리

## 6) 참고 스펙

- Unified Test Format:
  [https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/](https://specifications.readthedocs.io/en/latest/unified-test-format/unified-test-format/)
- Transactions:
  [https://specifications.readthedocs.io/en/latest/transactions/transactions/](https://specifications.readthedocs.io/en/latest/transactions/transactions/)
- CRUD:
  [https://specifications.readthedocs.io/en/latest/crud/crud/](https://specifications.readthedocs.io/en/latest/crud/crud/)

