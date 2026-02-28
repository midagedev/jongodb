# Fixture Automation (CI + Nightly)

`#257` 구현 가이드입니다.

## CI Pipeline
워크플로우: `.github/workflows/fixture-ci-pipeline.yml`

단계:
1. manifest validation (`fixtureManifestPlan`)
2. extract/sanitize/serialize dry-run (`FixtureExtractionToolTest`, `FixtureSanitizationToolTest`, `FixtureArtifactToolTest`)
3. restore smoke (`FixtureRestoreToolTest`)
4. refresh + drift gate dry-run (`fixtureRefresh`)

특징:
- 단계별 retry(최대 2회)
- 실패 시 재현 명령을 `GITHUB_STEP_SUMMARY`에 자동 출력
- refresh 리포트 artifact 업로드

## Nightly Refresh
워크플로우: `.github/workflows/fixture-nightly-refresh.yml`

스케줄:
- 매일 UTC 03:30
- 수동 실행(workflow_dispatch) 가능

동작:
1. `fixtureRefresh`를 nightly baseline/candidate 프로파일에 실행
2. diff/drift 리포트 생성 및 artifact 업로드
3. 요약(summary.md) 생성
4. schedule 실행 시 이슈 `#257`에 요약 코멘트 자동 등록

실패 대응:
- refresh 단계 최대 3회 retry
- 실패 시 재현 명령 자동 출력
- 실패 상태 포함 코멘트 기록

## Nightly Sample Data
- `testkit/fixture/nightly/dev/baseline/*.ndjson`
- `testkit/fixture/nightly/dev/candidate/*.ndjson`

필요 시 운영 프로파일 경로로 교체해서 동일 워크플로우를 재사용하면 됩니다.
