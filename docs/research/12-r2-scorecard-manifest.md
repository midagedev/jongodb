# R2 Compatibility Scorecard + Support Manifest

Status: active (still used for R3 release evidence composition).

## Purpose
- Aggregate R2 readiness from UTF differential and Spring matrix evidence.
- Publish a deterministic support manifest (`supported/partial/unsupported`) for release decisions.

## Generator
- Class: `org.jongodb.testkit.R2CompatibilityScorecard`
- Gradle task: `r2CompatibilityEvidence`

## Input Artifacts
- UTF report (default): `build/reports/unified-spec/utf-differential-report.json`
- Spring matrix report (default): `build/reports/spring-matrix/spring-compatibility-matrix.json`

## Output Artifacts
- Scorecard JSON: `build/reports/r2-compatibility/r2-compatibility-scorecard.json`
- Scorecard Markdown: `build/reports/r2-compatibility/r2-compatibility-scorecard.md`
- Support manifest JSON: `build/reports/r2-compatibility/r2-support-manifest.json`

## Gate Criteria (current)
- `utf-differential` gate
  - `importSummary.imported > 0`
  - `differentialSummary.mismatch == 0`
  - `differentialSummary.error == 0`
- `spring-compatibility-matrix` gate
  - `summary.passRate >= 0.98` (or equivalent `pass/(pass+fail)`)

## Notes
- Missing artifacts are reported as `MISSING` gate status.
- `--fail-on-gate` exits non-zero when any gate is `FAIL` or `MISSING`.
- R3 certification documents still consume this generator output:
  - `docs/SUPPORT_MATRIX.md`
  - `docs/COMPATIBILITY_SCORECARD.md`
