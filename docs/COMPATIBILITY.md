# Compatibility Matrix

Status date: 2026-02-24

This page describes implemented behavior in this repository. It is a code-level matrix, not a MongoDB claim.

Certification context:
- final scorecard: `docs/COMPATIBILITY_SCORECARD.md`
- support boundary: `docs/SUPPORT_MATRIX.md`
- release sign-off checklist: `docs/RELEASE_CHECKLIST.md`

## Command-Level Compatibility

| Command | Status | Notes |
| --- | --- | --- |
| `hello`, `isMaster` | Supported | Basic handshake shape; optional single-node replica-set profile adds `setName`/`hosts`/`primary`/`topologyVersion` fields |
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
| `update` | Partial | Operator set intentionally limited; update pipeline subset supports `$set`/`$unset` stages without expression evaluation |
| `delete` | Supported | `limit` 0/1 behavior |
| `bulkWrite` | Partial | Ordered mode only (`ordered=true`); supports `insertOne/updateOne/updateMany/deleteOne/deleteMany/replaceOne` and stops on first write error |
| `clientBulkWrite` | Partial | UTF importer subset rewrites ordered single-namespace models to `bulkWrite`; mixed namespaces, `ordered=false`, and `verboseResults=true` are deterministic unsupported paths |
| `count` | Partial | Alias path routed through `countDocuments` semantics in the test-backend profile |
| `countDocuments` | Partial | Filter + skip/limit + hint/collation/readConcern shape validation |
| `runCommand` | Partial | UTF importer subset supports `ping`, `buildInfo`, `listIndexes`, `count`; other command names fail with deterministic unsupported reasons |
| `replaceOne` | Partial | Rewrites to single replacement `update` path (`multi=false`) |
| `findOneAndUpdate` | Partial | Rewrites to `findAndModify`; supports operator updates plus update-pipeline subset (`$set`/`$unset`, no expression evaluation); projection include/exclude subset (including `_id` override) |
| `findOneAndReplace` | Partial | Rewrites to `findAndModify`; replacement updates only; supports projection include/exclude subset (including `_id` override) |
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
- update pipeline subset: `$set`/`$unset` stages with literal values
- replacement updates (with `multi=false`)
- upsert for operator and replacement forms
- same update constraints apply to `bulkWrite` update/replace operations

Not supported:
- `arrayFilters`
- positional updates (`$`, `$[]`, `$[<id>]`)
- update operators outside the supported set
- update pipeline stages outside `$set`/`$unset`
- update pipeline expressions (field references/operator expressions inside stage values)
- replacement updates with `multi=true`

## R3 Query/Update Corpus Exclusions

The R3 failure-ledger runner currently treats the following UTF cases as unsupported and excludes them from
differential parity counts:

- unordered `insertMany` (`ordered=false`)
- unordered `bulkWrite` (`ordered=false`)
- `clientBulkWrite` with mixed namespaces, `ordered=false`, or `verboseResults=true`
- documents containing dot or dollar-prefixed field paths in insert payloads
- `runCommand` command names outside the imported subset (`ping`, `buildInfo`, `listIndexes`, `count`)
- update operations using `arrayFilters`
- update pipeline forms outside the supported subset (`$set`/`$unset` stages with literal values)
- replacement updates requested with `multi=true`

## UTF Import Profiles

Supported importer profiles:
- `strict` (default): deterministic mode; `failPoint` remains policy-excluded and `targetedFailPoint` remains unsupported.
- `compat`: allows safe failpoint-disable subset (`mode: off` or `mode.times <= 0`) and maps those operations to deterministic no-op commands.

In `compat` profile:
- unsupported failpoint modes remain explicit unsupported cases
- scorecard/ledger artifacts should be interpreted together with profile metadata

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
- namespace-aware commit merge for transaction writes
- expected transaction error labels for no-such-transaction cases
- deterministic retry replay behavior for duplicate commit/abort paths (including `UnknownTransactionCommitResult` and `TransientTransactionError` label contracts by failure type)
- deterministic conflict rule: transactional writes win when transactional and non-transactional paths touch the same `_id`

Current scope:
- single in-memory process semantics
- no distributed transaction behavior

## Deployment Profiles

Supported runtime profiles:
- `standalone` (default): classic direct standalone semantics
- `singleNodeReplicaSet`: single-process replica-set semantic contract for driver/framework compatibility

`singleNodeReplicaSet` profile contract:
- URI includes `replicaSet=<name>`
- `hello`/`isMaster` include deterministic replica-set topology fields
- primary-only read preference (`mode=primary`); non-primary modes fail deterministically
- constrained concern subset:
  - writeConcern: `w=1` or `w="majority"` (with validated `wtimeout`)
  - readConcern levels: `local`, `majority`, `snapshot`

Not in scope for this profile:
- elections/step-down
- replication lag simulation
- multi-node member state transitions

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
