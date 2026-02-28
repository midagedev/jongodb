# In-Process Template PoC

`#260` 실험 산출물 생성용 가이드입니다.

## 목적
- TCP 템플릿(`TcpMongoServer` + `MongoClient`)과 In-Process 템플릿(`WireCommandIngress`)의 성능을 비교합니다.
- 실패 시점 트레이스 산출물(`command journal`, `invariant report`, `triage report`, `repro`)의 실효성을 검증합니다.
- 결과를 기반으로 `GO / NO_GO`를 명확히 결정합니다.

## 실행

```bash
.tooling/gradle-8.10.2/bin/gradle inProcessTemplatePocEvidence
```

선택 파라미터:
- `-PinProcessPocOutputDir=...`
- `-PinProcessPocSeed=...`
- `-PinProcessPocColdStartSamples=...`
- `-PinProcessPocWarmupOps=...`
- `-PinProcessPocMeasuredOps=...`
- `-PinProcessPocP95Threshold=0.10`
- `-PinProcessPocThroughputThreshold=0.10`
- `-PinProcessPocFailOnNoGo=true`

## 산출물
- `build/reports/in-process-template-poc/in-process-template-poc.json`
- `build/reports/in-process-template-poc/in-process-template-poc.md`

## 결정 규칙
- 성능 게이트:
  - `p95 개선율 >= p95 threshold` 또는
  - `throughput 개선율 >= throughput threshold`
- 트레이스 게이트:
  - 실패 재현 시 journal/triage/repro가 유효하게 생성되어 `traceUseful=true`
- 최종:
  - 위 두 게이트를 모두 만족하면 `GO`, 아니면 `NO_GO`
