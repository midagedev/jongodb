# Roadmap

Status date: 2026-02-24

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

## R3 Certification (Completed)

Completed:
- official UTF sharded differential gate on pinned spec ref
- deterministic R3 failure-ledger gate (`failureCount=0`)
- external canary certification gate for 3 Spring projects
- final scorecard/support manifest publication

## Current Focus

### R4 Compatibility Expansion

Target outcomes:
- reduce unsupported surface in official UTF suites
- increase transaction/retry semantics coverage for supported scenario bands
- increase Spring project cutover confidence on more app profiles

Primary measures:
- lower `unsupported` counts in UTF shard artifacts
- preserve zero mismatch/error on imported differential cases
- maintain green canary certification for at least 3 external projects

## Planned Next

- Expand collation semantics beyond metadata round-trip.
- Implement TTL runtime behavior beyond index metadata registration.
- Expand update/operator coverage (`arrayFilters`, positional updates, pipeline updates).
- Expand supported transaction operations in unified suites while preserving deterministic behavior.

## Out of Scope (Current Phase)

- production-grade distributed deployment behavior
- full external MongoDB server feature parity
- full protocol surface beyond current command/test scope
