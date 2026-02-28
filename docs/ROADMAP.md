# Roadmap

Status date: 2026-02-28

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

## R4 Compatibility Expansion (In Progress)

Completed in current wave:
- official + ledger imported differential coverage expanded to `508` with `mismatch=0`, `error=0`
- complex-query pack updated to `complex-query-pack-v3` with `$expr.$add` certification subset
- minimal `$graphLookup` certification subset implemented with deterministic option boundary
- listLocalSessions regression coverage normalized in UTF importer regression pack
- release-readiness streak artifacts added (`utf-shard-streak`, `r3-release-readiness-streak`)

## Current Focus

### Pre-Release Hardening

Target outcomes:
- keep imported differential parity at zero mismatch/error while reducing unsupported categories
- close release-readiness streak gate (`minStreak=3`) on scheduled runs
- refresh certification evidence on release-candidate commit before tagging

Primary measures:
- preserve `mismatch=0`, `error=0` across Official Suite and R3 Ledger
- increase zero-result streak counters (`officialZeroMismatchStreak`, `r3LedgerZeroFailureStreak`)
- maintain green canary certification for at least 3 external projects

## Planned Next

- close remaining aggregate-stage unsupported surface (`$merge` and advanced non-alias stages)
- expand collation semantics beyond current subset (`locale`/`strength`/`caseLevel`)
- implement TTL runtime behavior beyond index metadata registration
- expand update/operator coverage beyond current `arrayFilters` subset (advanced positional/pipeline expressions)
- expand supported transaction operations in unified suites while preserving deterministic behavior

## Out of Scope (Current Phase)

- production-grade distributed deployment behavior
- full external MongoDB server feature parity
- full protocol surface beyond current command/test scope
