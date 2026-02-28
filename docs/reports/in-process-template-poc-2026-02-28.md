# In-Process Template PoC

- generatedAt: 2026-02-28T03:29:31.790575Z
- seed: in-process-template-poc-v1
- decision: GO

## Performance

| Metric | TCP template | In-Process template |
| --- | ---: | ---: |
| Cold start P50 (ms) | 2.919 | 0.183 |
| Steady-state P95 latency (ms) | 0.239 | 0.140 |
| Throughput (ops/s) | 6536.2 | 15176.6 |

## Trace Analysis

- journalSize: 2
- invariantViolationCount: 0
- triageRootCauseType: command_failure
- reproLineCount: 2
- traceUseful: true

## Decision Inputs

- p95 improvement ratio=41.36% (threshold=10.00%)
- throughput improvement ratio=132.19% (threshold=10.00%)
- trace usable=true
- all gates satisfied
