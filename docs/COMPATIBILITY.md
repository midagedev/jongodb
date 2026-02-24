# Compatibility Matrix

Status date: 2026-02-23

This page describes implemented behavior in this repository. It is a code-level matrix, not a MongoDB claim.

## Command-Level Compatibility

| Command | Status | Notes |
| --- | --- | --- |
| `hello`, `isMaster` | Supported | Basic handshake shape |
| `ping` | Supported | Returns `{ok: 1}` |
| `buildInfo` | Partial | Stable subset of fields |
| `getParameter` | Partial | Only selected parameters |
| `insert` | Partial | Single/batch insert supported; unsupported bulk modes are skipped in differential corpus |
| `find` | Partial | Core filter support; not full query language |
| `aggregate` | Partial | Tier-1/Tier-2 subset |
| `getMore` | Supported | Cursor paging |
| `killCursors` | Supported | Cursor cancellation |
| `createIndexes` | Partial | Key metadata accepted; runtime semantics partial |
| `listIndexes` | Partial | Metadata round-trip |
| `update` | Partial | Operator set intentionally limited |
| `delete` | Supported | `limit` 0/1 behavior |
| `commitTransaction`, `abortTransaction` | Supported | Session/txn envelope supported |

## Query Operators

Implemented operators:
- Equality/comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- Set/membership: `$in`, `$nin`
- Existence/type: `$exists`, `$type`
- Array: `$size`, `$elemMatch`, `$all`
- Regex: `$regex` + `$options`, pattern literals
- Logical: `$and`, `$or`, `$nor`, top-level `$not`, field-level `$not`
- Expression: top-level `$expr` subset

`$expr` subset currently includes:
- comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- logical: `$and`, `$or`, `$not`
- literal: `$literal`

Not implemented:
- full expression operator set
- full BSON collation-aware comparison behavior

## Aggregation Stages

Implemented stages:
- `$match`
- `$project`
- `$group` (currently `$sum` accumulator only)
- `$sort`
- `$limit`, `$skip`
- `$unwind` (without `includeArrayIndex`)
- `$count`
- `$addFields`
- `$sortByCount`
- `$facet`
- `$lookup` (local/foreign and pipeline+let subset)
- `$unionWith`

Not implemented or partial:
- unsupported stages return deterministic fail-fast
- many advanced expression operators are still missing
- `$group` accumulators other than `$sum` are not available
- `$unwind.includeArrayIndex` is not available
- `$out`, `$merge`, `$listLocalSessions` stages are excluded from current differential corpus
- `bypassDocumentValidation` for aggregate is excluded from current differential corpus

## Update Semantics

Supported:
- operator updates: `$set`, `$inc`, `$unset`
- replacement updates (with `multi=false`)
- upsert for operator and replacement forms

Not supported:
- `arrayFilters`
- positional updates (`$`, `$[]`, `$[<id>]`)
- update operators outside the supported set
- pipeline updates (`u` as array)
- replacement updates with `multi=true`

## R3 Query/Update Corpus Exclusions

The R3 failure-ledger runner currently treats the following UTF cases as unsupported and excludes them from
differential parity counts:

- unordered `insertMany` (`ordered=false`)
- documents containing dot or dollar-prefixed field paths in insert payloads
- update operations using `arrayFilters`
- update pipeline form (`u` as an array pipeline)
- replacement updates requested with `multi=true`

## Index Semantics

Supported metadata paths:
- `unique`
- `sparse`
- `partialFilterExpression`
- `collation` metadata
- `expireAfterSeconds` metadata

Current limitations:
- collation runtime semantics are not fully implemented
- TTL expiration loop/clock behavior is not fully implemented

## Transactions

Supported:
- session + transaction envelope validation (`lsid`, `txnNumber`, `autocommit=false`)
- start/commit/abort flow
- transaction snapshot publish on commit
- expected transaction error labels for no-such-transaction cases

Current scope:
- single in-memory process semantics
- no distributed transaction behavior

## Error Contract

Known deterministic error classes:
- unknown command: `CommandNotFound` (`code=59`)
- validation failures: `BadValue` / `TypeMismatch` (`code=14`)
- duplicate index/key: `DuplicateKey` (`code=11000`)
- cursor lifecycle: `CursorNotFound` (`code=43`)
- transaction state: `NoSuchTransaction` (`code=251`)

Unsupported feature paths are being standardized to:
- `codeName=NotImplemented` (`code=238`)
- `errorLabels=["UnsupportedFeature"]`

This path is implemented for major query/aggregation unsupported branches and is being expanded.
