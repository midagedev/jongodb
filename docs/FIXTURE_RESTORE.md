# Fixture Restore Utility

`#253` 구현 가이드입니다.

## 목적
- NDJSON fixture를 로컬/CI Mongo로 복원합니다.
- dual artifact(Portable + Fast) 입력을 자동 인식해 복원합니다.
- `replace|merge` 모드와 DB/namespace 대상 선택을 지원합니다.
- 복원 진단 리포트를 생성합니다.

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureRestore \
  -PfixtureRestoreInputDir=build/reports/fixture-sanitized \
  -PfixtureRestoreMongoUri='mongodb://localhost:27017' \
  -PfixtureRestoreMode=replace
```

## 주요 옵션
- `--mode=replace|merge`
- `--database=<db>`
- `--namespace=<db.collection[,db.collection...]>`
- `--report-dir=<dir>`
- `--no-regenerate-fast-cache` (portable fallback 이후 fast cache 재생성 비활성화)
- `--required-fixture-version=<semver>` (명시 버전 불일치 시 복원 차단)

## 산출물
- `<reportDir>/fixture-restore-report.json`
  - `sourceFormat` (`FAST`, `PORTABLE_FALLBACK`, `NDJSON`)
  - 컬렉션별 source/restored 문서 수
  - 스키마 불일치 경고(top-level signature mix)

## before-test hook
- 테스트 프레임워크에서 `FixtureRestoreSupport.beforeEachReplace(mongoUri, fixtureDir)` 호출로
  테스트 시작 전 상태를 반복 복원할 수 있습니다.
