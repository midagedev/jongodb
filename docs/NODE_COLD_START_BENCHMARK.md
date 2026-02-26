# Node Cold-Start Benchmark Gate

Cold-start benchmark validates `@jongodb/memory-server` startup latency and enforces a budget gate in CI.

## Local Run

```bash
npm run node:build
npm run node:benchmark:cold-start -- \
  --iterations 9 \
  --budget-median-ms 500 \
  --budget-p95-ms 1500 \
  --outputDir build/reports/node-cold-start
```

Output files:
- `build/reports/node-cold-start/report.json`
- `build/reports/node-cold-start/report.md`

## CI Gate

Workflow:
- `.github/workflows/node-cold-start-benchmark.yml`

Current budget:
- median startup <= `500ms`
- p95 startup <= `1500ms`

If either budget is exceeded, workflow fails.

## Report Fields

- `samplesMs`: per-iteration startup times
- `summary`: `min`, `max`, `mean`, `median`, `p95`
- `gate`: budget pass/fail flags
- `settings`: iterations, timeout, budget values

## Notes

- Benchmark is measured against a cold server start (`startJongodbMemoryServer` -> `stop`) per iteration.
- Classpath resolution uses `JONGODB_CLASSPATH` when provided, otherwise resolves via `gradle -q printLauncherClasspath`.
