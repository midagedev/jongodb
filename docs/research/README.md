# jongodb Research Notes

This directory contains the design and reference notes used to implement `jongodb`.

Status date: 2026-02-24

## How to Use This Folder

- `01` to `11`: reference and planning documents from early implementation phases.
- `12` to `16`: implementation-era execution notes for R2/R3 workstreams.
- `17`: current implementation mapping of each reference note to code/artifacts.

For current release-level status, see:
- `docs/COMPATIBILITY_SCORECARD.md`
- `docs/SUPPORT_MATRIX.md`
- `docs/RELEASE_CHECKLIST.md`

## Document List

- [`01-spec-map.md`](./01-spec-map.md): Official spec and product-doc reference catalog.
- [`02-transaction-session-behavior.md`](./02-transaction-session-behavior.md): Transaction and session behavior rules.
- [`03-wire-protocol-command-compat.md`](./03-wire-protocol-command-compat.md): Wire protocol and command compatibility boundary.
- [`04-java-spring-integration.md`](./04-java-spring-integration.md): Java driver and Spring integration requirements.
- [`05-test-strategy.md`](./05-test-strategy.md): Differential/UTF-based validation strategy.
- [`06-architecture-debuggability.md`](./06-architecture-debuggability.md): Debuggable and deterministic architecture guide.
- [`07-agent-execution-plan.md`](./07-agent-execution-plan.md): Parallel agent orchestration plan.
- [`08-success-criteria.md`](./08-success-criteria.md): Quantitative quality gates and success criteria.
- [`09-r1-replacement-reference-baseline.md`](./09-r1-replacement-reference-baseline.md): R1 replacement baseline criteria.
- [`10-r1-issue-reference-test-matrix.md`](./10-r1-issue-reference-test-matrix.md): Issue-to-test/reference mapping.
- [`11-r1-issue-37-canary-cutover-rollback-playbook.md`](./11-r1-issue-37-canary-cutover-rollback-playbook.md): Canary cutover and rollback playbook.
- [`12-r2-scorecard-manifest.md`](./12-r2-scorecard-manifest.md): R2 scorecard and support-manifest generation rules.
- [`13-r2-canary-certification.md`](./13-r2-canary-certification.md): R2 canary evidence format and gating.
- [`14-r3-issue-50-protocol-parity-baseline.md`](./14-r3-issue-50-protocol-parity-baseline.md): R3 protocol parity baseline.
- [`15-r3-issue-53-transaction-baseline.md`](./15-r3-issue-53-transaction-baseline.md): R3 transaction/session baseline.
- [`16-r3-issue-58-external-canary-certification.md`](./16-r3-issue-58-external-canary-certification.md): R3 external canary certification flow.
- [`17-reference-implementation-status.md`](./17-reference-implementation-status.md): Current mapping from reference notes to implemented code/artifacts.

## Snapshot

- Pinned MongoDB Specifications commit:
  - `99704fa8860777da1d919ef765af1e41e75f5859`
  - commit timestamp: `2026-02-20 15:05:34 -0500`
