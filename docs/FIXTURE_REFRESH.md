# Fixture Refresh Workflow

`#254` 구현 가이드입니다.

## 목적
- fixture 갱신을 `full` / `incremental` 모드로 표준화합니다.
- 실행마다 diff 리포트(JSON+Markdown)를 자동 생성합니다.
- breaking refresh(삭제/필드 드롭)는 승인 없이 통과하지 않도록 게이트를 제공합니다.

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureRefresh \
  -PfixtureRefreshBaselineDir=build/reports/fixture-prev \
  -PfixtureRefreshCandidateDir=build/reports/fixture-sanitized \
  -PfixtureRefreshOutputDir=build/reports/fixture-refresh \
  -PfixtureRefreshMode=incremental \
  -PfixtureRefreshRequireApproval=true
```

승인된 실행:

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureRefresh \
  -PfixtureRefreshBaselineDir=build/reports/fixture-prev \
  -PfixtureRefreshCandidateDir=build/reports/fixture-sanitized \
  -PfixtureRefreshOutputDir=build/reports/fixture-refresh \
  -PfixtureRefreshMode=full \
  -PfixtureRefreshRequireApproval=true \
  -PfixtureRefreshApproved=true
```

## 산출물
- `<outputDir>/refreshed/*.ndjson`
  - `full`: candidate 전체 결과
  - `incremental`: 추가/변경 문서만 출력
- `<outputDir>/fixture-refresh-report.json`
- `<outputDir>/fixture-refresh-report.md`

리포트에는 컬렉션별 `added/removed/changed`와 `risk(breaking|non-breaking)`가 포함됩니다.

## 승인 게이트
- `--require-approval` 사용 시 breaking change가 발견되면 `--approved` 없이 실패합니다.
- CI에서는 PR 라벨 `fixture-refresh-approved`를 기준으로 승인 여부를 연결할 수 있습니다.
