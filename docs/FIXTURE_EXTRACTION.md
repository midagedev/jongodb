# Fixture Extraction CLI

`#250` 구현 가이드입니다.

## 목적
- manifest/profile 기반으로 운영 MongoDB에서 fixture를 안전하게 추출합니다.
- 컬렉션별 건수/해시를 남기고, 실패 컬렉션을 분리 리포트합니다.
- checkpoint 기반 resume를 지원합니다.

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureExtract \
  -PfixtureManifestPath=testkit/fixture/manifests/baseline-dev-smoke-full.json \
  -PfixtureOutputDir=build/reports/fixture-extract \
  -PfixtureMongoUri='mongodb://localhost:27017' \
  -PfixtureAllowUriAlias=prod-main
```

## 주요 옵션
- `--allow-uri-alias=a,b` : manifest `source.uriAlias` allowlist
- `--read-preference=secondaryPreferred` : 읽기 우선순위
- `--timeout-ms=30000`
- `--batch-size=500`
- `--max-docs=...`
- `--rate-limit-ms=...`
- `--resume` : checkpoint 재사용
- `--readonly-check=strict|best-effort|off`

## 산출물
- `<outputDir>/<db>.<collection>.ndjson`
- `<outputDir>/checkpoint-<profile>.json`
- `<outputDir>/fixture-extraction-report.json`
