# Compatibility Scorecard

Status date: 2026-02-28

This scorecard tracks MongoDB UTF compatibility progress with a focus on `runOnRequirements not satisfied` reduction while preserving differential stability (`mismatch=0`, `error=0`).

## Lane model

- Strict lane (baseline): runOn lane overrides disabled.
  - Control: `JONGODB_UTF_RUNON_LANES=disabled`
  - Purpose: keep topology/version gate behavior fully strict.
- Extended lane (compatibility expansion): runOn lane overrides enabled (default).
  - Control: `JONGODB_UTF_RUNON_LANES=enabled`
  - Purpose: admit deterministic subsets that are blocked only by non-actionable runOn constraints.

Current lane buckets:

- `mongos-pin-auto` topology lane (`transactions/tests/unified/mongos-pin-auto.{json,yml}`)
- hint legacy version lane (`crud/tests/unified/*-hint-*` + `unacknowledged|clientError|serverError`)
- `client-bulkWrite` version lane (excluding `client-bulkWrite-errors*` / `client-bulkWrite-errorResponse*`)

## Latest local progression (strict profile, shard 0-of-1)

Evidence files:

- `build/reports/utf-shard-issue406b/utf-differential-report.json`
- `build/reports/utf-shard-issue422c/utf-differential-report.json`
- `build/reports/utf-shard-issue424/utf-differential-report.json`
- `build/reports/utf-shard-issue423c/utf-differential-report.json`

| Checkpoint | imported | skipped | unsupported | mismatch | error | runOn total | mongos lane | hint lane | client-bulkWrite lane | runOn remaining |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| issue406b (baseline) | 810 | 605 | 166 | 0 | 0 | 519 | 59 | 128 | 104 | 228 |
| issue422c | 866 | 549 | 166 | 0 | 0 | 463 | 3 | 128 | 104 | 228 |
| issue424 | 994 | 421 | 166 | 0 | 0 | 335 | 3 | 0 | 104 | 228 |
| issue423c (latest) | 1072 | 343 | 166 | 0 | 0 | 251 | 3 | 0 | 20 | 228 |

Delta (issue406b -> issue423c):

- imported: `+262` (`810 -> 1072`)
- skipped: `-262` (`605 -> 343`)
- runOn not satisfied: `-268` (`519 -> 251`, `-51.64%`)
- mismatch/error: unchanged (`0/0 -> 0/0`)

## Pilot workflow (#408)

Workflow: `.github/workflows/runon-lane-pilot.yml`

Execution model:

- Trigger: `workflow_dispatch` + daily `schedule`
- Matrix: `shard_index=[0,1,2]`
- Per shard, run both lanes on identical spec selection:
  - strict: `run-utf-shard.sh --runon-lanes disabled`
  - extended: `run-utf-shard.sh --runon-lanes enabled`
- Summaries generated via `scripts/ci/summarize-runon-lanes.sh`

Gates:

- strict lane stability: strict `mismatch=0` and `error=0`
- extended no-regression: extended mismatch/error cannot exceed strict
- runOn reduction: extended `runOnNotSatisfied` must be `<=` strict
- coverage non-regression: extended imported must be `>=` strict

Artifacts:

- shard artifacts: `runon-lane-pilot-shard-{0,1,2}`
- aggregate artifact: `runon-lane-pilot-summary` (`.md` + `.json`)

## Current open track

- `#408` `[phase:r4] Reduce runOn-not-satisfied skips via fixture/profile strategy`
- `#374` `[EPIC][suite] MongoDB 공식+추가 스위트 100% 달성 프로그램`

## Reproduction

Run strict lane shard locally:

```bash
./scripts/ci/run-utf-shard.sh \
  --spec-repo-root "<path-to-mongodb-specifications>" \
  --shard-index 0 \
  --shard-count 1 \
  --output-dir build/reports/utf-shard-strict \
  --seed local-runon-pilot \
  --replay-limit 30 \
  --mongo-uri "<replica-set-uri>" \
  --runon-lanes disabled
```

Run extended lane shard locally:

```bash
./scripts/ci/run-utf-shard.sh \
  --spec-repo-root "<path-to-mongodb-specifications>" \
  --shard-index 0 \
  --shard-count 1 \
  --output-dir build/reports/utf-shard-extended \
  --seed local-runon-pilot \
  --replay-limit 30 \
  --mongo-uri "<replica-set-uri>" \
  --runon-lanes enabled
```

Summarize runOn buckets:

```bash
./scripts/ci/summarize-runon-lanes.sh \
  --report build/reports/utf-shard-extended/utf-differential-report.json \
  --output build/reports/utf-shard-extended/runon-lane-summary.md \
  --output-json build/reports/utf-shard-extended/runon-lane-summary.json
```
