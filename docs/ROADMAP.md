# Roadmap

Status date: 2026-02-23

This roadmap is implementation-focused and tied to testable outcomes.

## Principles

- Keep behavior deterministic.
- Prefer explicit unsupported contracts over silent fallback.
- Drive parity from differential/spec-based evidence.

## Completed Tracks

### R1 Foundation

Completed:
- in-memory command engine and dispatcher
- transaction envelope and session-based transaction flow
- wire command ingress (`OP_MSG`) in-process path
- baseline differential and readiness automation

### R2 Infrastructure

Completed:
- UTF importer and corpus runner (`utf-differential-report`)
- partial Tier-2 query/aggregation expansion
- unsupported feature error contract (`NotImplemented` + labels)
- multi-target Spring matrix framework
- R2 compatibility scorecard and support manifest generators
- canary certification evidence aggregator (input-driven)

## In Progress

### R2 Parity Hardening

Target outcomes:
- increase query/expression parity depth
- increase aggregation expression/operator depth
- reduce unsupported coverage in high-frequency Spring usage paths

Primary measure:
- lower mismatch/error counts in UTF and real differential runs

## Remaining Exit Criteria for R2 Sign-Off

1. Run canary evidence with real project data for at least 3 Spring projects.
2. Verify rollback rehearsal success for all canary projects.
3. Run scorecard and canary gates in one CI pass with no FAIL/MISSING.

## Planned Next (Post-R2)

- Expand collation semantics beyond metadata round-trip.
- Implement TTL runtime behavior (not only index metadata).
- Expand update/operator coverage.
- Continue closing gaps against official spec suites.

## Out of Scope (Current Phase)

- production-grade distributed deployment behavior
- full external MongoDB server feature parity
- full protocol surface beyond current command/test scope
