# Fixture Sanitization Pipeline

`#251` 구현 가이드입니다.

## 목적
- deterministic 비식별/정규화 파이프라인을 제공합니다.
- 정책 기반으로 `drop/hash/tokenize/fake/nullify` 처리합니다.
- PII lint(email/phone/ssn 패턴)로 잔존 민감정보를 검출합니다.

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle fixtureSanitize \
  -PfixtureSanitizeInputDir=build/reports/fixture-extract \
  -PfixtureSanitizeOutputDir=build/reports/fixture-sanitized \
  -PfixtureSanitizePolicyFile=policy/sanitize-policy.json
```

## 정책 예시

```json
{
  "rules": [
    {"path": "pii.ssn", "action": "nullify"},
    {"path": "pii.email", "action": "fake", "fakeType": "email"},
    {"path": "pii.phone", "action": "tokenize"},
    {"path": "credentials.passwordHash", "action": "drop"},
    {"path": "externalId", "action": "hash"}
  ],
  "volatileFields": ["updatedAt", "createdAt"]
}
```

## 산출물
- `<outputDir>/*.ndjson` (정규화/안정 정렬된 결과)
- `<outputDir>/fixture-sanitize-report.json`
  - 파일별 문서 수/해시
  - PII lint 위반 목록
