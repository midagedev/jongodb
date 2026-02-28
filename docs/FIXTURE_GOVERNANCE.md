# Fixture Artifact Governance

`#256` 구현 가이드입니다.

## 목적
- fixture artifact 버전(`fixtureSemVer`)과 스키마 해시(`dataSchemaHash`)를 명시적으로 관리합니다.
- 보관 정책(최근 N개 + freeze 버전 유지)으로 불필요 artifact를 자동 정리합니다.
- 사용처(consumer -> fixtureVersion) 인덱스를 유지해 어떤 테스트가 어떤 버전을 쓰는지 추적합니다.

## Manifest 확장
`fixtureArtifactPack` 결과 manifest(`fixture-artifact-manifest.json`)에는 아래 필드가 포함됩니다.

- `fixtureVersion` (semver)
- `dataSchemaHash`
- `artifactFormatVersion`
- `portableFormatVersion`
- `fastFormatVersion`
- `changelog[]`

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureArtifactGovernance \
  -PfixtureArtifactRoot=build/reports/fixture-artifacts \
  -PfixtureArtifactRetain=5 \
  -PfixtureArtifactFreezeVersion=1.0.0,1.1.0 \
  -PfixtureArtifactDryRun=true
```

consumer 사용처 등록:

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureArtifactGovernance \
  -PfixtureArtifactRoot=build/reports/fixture-artifacts \
  -PfixtureArtifactRegisterConsumer=spring-smoke \
  -PfixtureArtifactRegisterVersion=1.2.0
```

## 산출물
- `<reportDir>/fixture-artifact-governance-report.json`
- `<reportDir>/fixture-artifact-governance-report.md`
- `<reportDir>/fixture-artifact-changelog.md`
- `<artifactRoot>/fixture-usage-index.json` (등록 시)

## 호환성 검사
복원 시 필수 버전을 강제하려면:

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureRestore \
  -PfixtureRestoreInputDir=build/reports/fixture-artifacts/v1.2.0 \
  -PfixtureRestoreMongoUri='mongodb://localhost:27017' \
  -PfixtureRestoreRequiredFixtureVersion=1.2.0
```

필수 버전과 불일치하면 복원을 차단하고 대응 안내 메시지를 출력합니다.
