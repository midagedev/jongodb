# Reference-to-Implementation Status

Status date: 2026-02-24

Purpose:
- keep reference notes current by mapping them to implemented code paths and evidence artifacts.
- clarify which notes are still active guidance vs historical context.

## Status Legend

- `Active`: directly used for current implementation and release evidence.
- `Partially applied`: used, but implementation scope is intentionally narrower than the full reference.
- `Historical`: useful for context; superseded by newer artifacts for release decisions.

## Mapping

| Note | Status | Primary implementation/evidence mapping |
| --- | --- | --- |
| `01-spec-map.md` | Active | `third_party/mongodb-specs/manifest.json`, `scripts/spec-sync/*` |
| `02-transaction-session-behavior.md` | Partially applied | `commitTransaction`/`abortTransaction` handlers and txn envelope checks; advanced semantics still partial |
| `03-wire-protocol-command-compat.md` | Active | `src/main/java/org/jongodb/server/*`, `src/main/java/org/jongodb/wire/*` |
| `04-java-spring-integration.md` | Active | `src/main/java/org/jongodb/testkit/springsuite/*`, `springCompatibilityMatrixEvidence` |
| `05-test-strategy.md` | Active | `UnifiedSpecCorpusRunner`, `R3FailureLedgerRunner`, sharded CI workflows |
| `06-architecture-debuggability.md` | Active | deterministic errors, replay bundles, failure-ledger outputs |
| `07-agent-execution-plan.md` | Historical | execution plan completed through PR-based issue orchestration |
| `08-success-criteria.md` | Active | mirrored by scorecard/checklist: `docs/COMPATIBILITY_SCORECARD.md`, `docs/RELEASE_CHECKLIST.md` |
| `09-r1-replacement-reference-baseline.md` | Historical | baseline objective delivered by later R2/R3 artifacts |
| `10-r1-issue-reference-test-matrix.md` | Historical | issue matrix completed and closed |
| `11-r1-issue-37-canary-cutover-rollback-playbook.md` | Partially applied | canary certification implemented; project-specific rollout operations remain external |
| `12-r2-scorecard-manifest.md` | Active | `R2CompatibilityScorecard`, `docs/SUPPORT_MATRIX.md` |
| `13-r2-canary-certification.md` | Active | `R2CanaryCertification`, reused by `r3CanaryCertificationEvidence` |
| `14-r3-issue-50-protocol-parity-baseline.md` | Active | `R3FailureLedgerRunner`, protocol track baseline in scorecard |
| `15-r3-issue-53-transaction-baseline.md` | Active | txn/session suite summary in R3 ledger artifacts |
| `16-r3-issue-58-external-canary-certification.md` | Active | `.github/workflows/r3-external-canary.yml`, `r3CanaryCertificationEvidence` |

## Release Decision Source of Truth

Use these documents for final status checks:
- `docs/COMPATIBILITY_SCORECARD.md`
- `docs/SUPPORT_MATRIX.md`
- `docs/RELEASE_CHECKLIST.md`
