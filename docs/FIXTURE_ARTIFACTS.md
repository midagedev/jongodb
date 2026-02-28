# Fixture Dual Artifacts (Portable + Fast)

`#252` 구현 가이드입니다.

## 목적
- fixture를 두 가지 포맷으로 동시 관리합니다.
- `portable`: 이식성과 장기 보관 중심 (`.ejsonl.gz`)
- `fast`: 복원 속도 중심 (`.bin` 스냅샷)

## 산출물
`fixtureArtifactPack` 실행 시 아래 파일이 생성됩니다.

- `fixture-artifact-manifest.json`
- `fixture-portable.ejsonl.gz`
- `fixture-fast-snapshot.bin`

manifest에는 다음이 포함됩니다.
- `schemaVersion`, `portableFormatVersion`, `fastFormatVersion`
- `fixtureVersion`, `dataSchemaHash`, `artifactFormatVersion`
- `engineVersion`
- 각 아티팩트의 `sha256`
- 컬렉션별 문서 수
- `changelog`

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureArtifactPack \
  -PfixtureArtifactInputDir=build/reports/fixture-sanitized \
  -PfixtureArtifactOutputDir=build/reports/fixture-artifacts \
  -PfixtureArtifactVersion=1.2.0
```

옵션:
- `-PfixtureArtifactEngineVersion=<version>`
- `-PfixtureArtifactVersion=<semver>`
- `-PfixtureArtifactPreviousManifest=<path>`

## 복원 시 호환성 정책
`fixtureRestore`는 기본적으로 아래 순서로 입력을 선택합니다.

1. fast snapshot 사용 시도
2. fast 포맷/엔진 버전 불일치 시 portable fallback
3. 필요 시 portable로부터 fast cache 재생성
4. artifact가 없으면 기존 ndjson 복원 경로 사용

체크섬 불일치가 감지되면 즉시 실패합니다.

`fixtureRestore` 리포트(`fixture-restore-report.json`)의 `sourceFormat`으로 실제 경로를 확인할 수 있습니다.

버전/보관/사용처 추적 정책은 `docs/FIXTURE_GOVERNANCE.md`를 참고하세요.
