# Complex Query Certification

## Purpose

This certification pack provides deterministic differential evidence for complex query behavior against:

- `wire-backend` (`jongodb`)
- `real-mongod`

It complements the broad scorecard/failure-ledger view with a focused complex-query gate.

## Pack Metadata

- pack version: `complex-query-pack-v3`
- canonical pattern count: `24`
- support classes:
  - `supported`
  - `partial`
  - `explicitly-unsupported`
- expected outcomes:
  - `match`
  - `unsupported-policy`

Every pattern includes:
- stable `id`
- `title`
- `supportClass`
- `expectedOutcome`
- `rationale`
- `sampleUseCase`

The above metadata is published in artifact JSON under:
- `patterns[].id`
- `patterns[].title`
- `patterns[].supportClass`
- `patterns[].expectedOutcome`
- `patterns[].rationale`
- `patterns[].sampleUseCase`

## Pattern IDs

### Supported

- `cq.nested.logic.and-or-dotted`
- `cq.nested.logic.nor-and-field-not`
- `cq.array.scalar-contains-and`
- `cq.array.elemmatch-range`
- `cq.array.all-elemmatch-docs`
- `cq.path.deep-array-document`
- `cq.path.array-index-eq`
- `cq.path.array-index-comparison`
- `cq.expr.basic-comparison`
- `cq.expr.logical-composition`
- `cq.regex.case-insensitive-and-ne`
- `cq.exists-null-in`
- `cq.type-and-size`
- `cq.nested.array-branch-composition`
- `cq.unsupported.query-mod`
- `cq.unsupported.expr-add`
- `cq.unsupported.query-bitsallset`

### Partial

- `cq.lookup.local-foreign-unwind`
- `cq.lookup.pipeline-let-match`
- `cq.aggregate.facet-group-sort`
- `cq.aggregate.sortbycount-after-project`
- `cq.aggregate.unionwith-and-match`
- `cq.expr.array-index-comparison`
- `cq.unsupported.aggregate-graphlookup`

### Explicitly Unsupported

- _(none in current canonical pack)_

### `$graphLookup` Certification Subset

Supported option set in current engine subset:
- `from`
- `startWith`
- `connectFromField`
- `connectToField`
- `as`
- `maxDepth` (optional non-negative integer)

Out-of-subset options (for example `depthField`, `restrictSearchWithMatch`) are rejected as `NotImplemented` by design.

## Runner

Main class:
- `org.jongodb.testkit.ComplexQueryCertificationRunner`

Gradle task:

```bash
gradle complexQueryCertificationEvidence \
  -PcomplexQueryMongoUri="mongodb://localhost:27017/?replicaSet=rs0" \
  -PcomplexQueryFailOnGate=true
```

Optional overrides:
- `-PcomplexQuerySeed=<text>`
- `-PcomplexQueryPatternLimit=<n>`
- `-PcomplexQueryOutputDir=<path>`

## Artifacts

Default output dir:
- `build/reports/complex-query-certification`

Artifacts:
- `complex-query-certification.json`
- `complex-query-certification.md`
- `failure-replay-bundles/manifest.json`
- `failure-replay-bundles/*.json`

## Gate Policy

The gate fails when:

- supported subset has any non-match outcome
  - mismatch, error, or unsupported-policy
- unapproved unsupported-policy deltas increase

Primary metrics:
- `totalPatterns`
- `supportedPassRate`
- `mismatchCount`
- `errorCount`
- `unsupportedByPolicyCount`
- `unsupportedDeltaCount`

## CI

Workflow:
- `.github/workflows/complex-query-certification.yml`

The workflow bootstraps a single-node replica set, runs `complexQueryCertificationEvidence` with `failOnGate=true`, and uploads artifacts and diagnostics.

## Spring Matrix Alignment

Spring-focused complex scenarios are published by:
- `springCompatibilityMatrixEvidence`
- artifact: `build/reports/spring-matrix/spring-compatibility-matrix.json/.md`

Complex Spring matrix rows include `certificationPatternId` to map back to this pack.
CI workflow:
- `.github/workflows/spring-complex-query-matrix.yml`
