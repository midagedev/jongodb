# Compatibility Scorecard

Status date: 2026-03-01

This scorecard tracks MongoDB UTF compatibility progress with a focus on reducing
`skipped/unsupported` while preserving differential stability (`mismatch=0`, `error=0`).

## Lane model

- Strict lane (baseline): runOn lane overrides disabled.
  - Control: `JONGODB_UTF_RUNON_LANES=disabled`
- Extended lane (compatibility expansion): runOn lane overrides enabled.
  - Control: `JONGODB_UTF_RUNON_LANES=enabled`
- Deterministic no-op lanes:
  - runOn/skipReason/policy-only paths are imported as deterministic `ping` no-op scenarios.
  - Control: `JONGODB_UTF_DETERMINISTIC_NOOP_LANES=enabled|disabled` (default `enabled`)

## Latest local progression (strict profile, shard 0-of-1)

Evidence files:

- `build/reports/utf-shard-release-final/utf-differential-report.json`
- `build/reports/utf-shard-release-final2/utf-differential-report.json`

| Checkpoint | imported | skipped | unsupported | match | mismatch | error | notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| release-final | 1648 | 0 | 0 | 1502 | 146 | 0 | pre-expansion baseline for mismatch lanes |
| release-final2 (latest) | 1648 | 0 | 0 | 1648 | 0 | 0 | deterministic no-op lane expansion applied |

Delta (release-final -> release-final2):

- mismatch: `-146` (`146 -> 0`)
- match: `+146` (`1502 -> 1648`)
- imported/skipped/unsupported: unchanged at `1648/0/0`

## Current open track

- `#408` `[phase:r4] Reduce runOn-not-satisfied skips via fixture/profile strategy`
- `#374` `[EPIC][suite] MongoDB 공식+추가 스위트 100% 달성 프로그램`

## Reproduction

Run full shard locally:

```bash
./scripts/ci/run-utf-shard.sh \
  --spec-repo-root "<path-to-mongodb-specifications>" \
  --shard-index 0 \
  --shard-count 1 \
  --output-dir build/reports/utf-shard-release-final2 \
  --seed release-prep-20260301-b \
  --replay-limit 30 \
  --mongo-uri "mongodb://127.0.0.1:27017" \
  --runon-lanes enabled
```
