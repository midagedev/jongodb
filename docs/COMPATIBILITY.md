# Compatibility Matrix

Status date: 2026-03-01

This page describes implemented behavior in this repository. It is a code-level matrix, not a MongoDB claim.

Certification context:
- final scorecard: `docs/COMPATIBILITY_SCORECARD.md`
- support boundary: `docs/SUPPORT_MATRIX.md`
- complex-query certification: `docs/COMPLEX_QUERY_CERTIFICATION.md`
- release sign-off checklist: `docs/RELEASE_CHECKLIST.md`

## Command-Level Compatibility

| Command | Status | Notes |
| --- | --- | --- |
| `hello`, `isMaster` | Supported | Basic handshake shape; optional single-node replica-set profile adds `setName`/`hosts`/`primary`/`topologyVersion` fields |
| `ping` | Supported | Returns `{ok: 1}` |
| `buildInfo` | Partial | Stable subset of fields |
| `getParameter` | Partial | Only selected parameters |
| `insert` | Partial | Single/batch insert supported; unsupported bulk modes are skipped in differential corpus |
| `find` | Partial | Core filter support; collation subset (`locale`/`strength`/`caseLevel`) applied to query + sort |
| `aggregate` | Partial | Tier-1/Tier-2 subset plus minimal `$graphLookup`; collation subset applied to `$match`/`$sort`/`$sortByCount` |
| `getMore` | Supported | Cursor paging |
| `killCursors` | Supported | Cursor cancellation |
| `createIndexes` | Partial | Key metadata accepted; runtime semantics partial |
| `listIndexes` | Partial | Metadata round-trip |
| `update` | Partial | Operator set intentionally limited; update pipeline subset supports `$set`/`$unset` stages without expression evaluation |
| `delete` | Supported | `limit` 0/1 behavior |
| `bulkWrite` | Partial | Ordered mode only (`ordered=true`); supports `insertOne/updateOne/updateMany/deleteOne/deleteMany/replaceOne` and stops on first write error |
| `clientBulkWrite` | Partial | UTF importer subset rewrites ordered single-namespace models to `bulkWrite`; mixed namespaces, `ordered=false`, and `verboseResults=true` are deterministic unsupported paths |
| `count` | Partial | Alias path routed through `countDocuments` semantics in the test-backend profile |
| `countDocuments` | Partial | Filter + skip/limit + hint/readConcern; collation subset applied to filter comparison |
| `runCommand` | Partial | UTF importer subset supports `ping`, `buildInfo`, `listIndexes`, `listCollections`, `count`; non-subset commands are deterministic no-op lanes in strict UTF import |
| `replaceOne` | Partial | Rewrites to single replacement `update` path (`multi=false`) |
| `findOneAndUpdate` | Partial | Rewrites to `findAndModify`; supports operator updates plus update-pipeline subset (`$set`/`$unset`, no expression evaluation), `arrayFilters` subset, and projection include/exclude subset (including `_id` override) |
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
- Positional dotted paths: array-index path resolution (for example `items.0.sku`, `$expr` path `metrics.0`)

`$expr` subset currently includes:
- comparison: `$eq`, `$ne`, `$gt`, `$gte`, `$lt`, `$lte`
- logical: `$and`, `$or`, `$not`
- literal: `$literal`
- arithmetic: `$add` (numeric expression subset)

Not implemented:
- full expression operator set
- advanced collation features beyond `locale`/`strength`/`caseLevel`

## Aggregation Stages

Implemented stages:
- `$match`
- `$project`
- `$group` (subset: `$sum`, `$first`, `$addToSet`)
- `$sort`
- `$limit`, `$skip`
- `$unwind` (without `includeArrayIndex`)
- `$count`
- `$addFields`
- `$set` (alias subset of `$addFields`)
- `$unset` (field removal subset: string / array / document-key form)
- `$sortByCount`
- `$replaceRoot`
- `$replaceWith` (alias-style root replacement subset)
- `$facet`
- `$lookup` (local/foreign and pipeline+let subset)
- `$unionWith`
- `$graphLookup` (minimal subset: `from`, `startWith`, `connectFromField`, `connectToField`, `as`, optional `maxDepth`)
- `$out` (terminal string-target subset: replaces target collection contents and returns empty result set)

Not implemented or partial:
- unsupported stages return deterministic fail-fast
- many advanced expression operators are still missing
- `$group` accumulators other than `$sum` are not available
- `$unwind.includeArrayIndex` is not available
- `$merge` stage is excluded from current differential corpus
- `$graphLookup` options outside current subset (for example `depthField`, `restrictSearchWithMatch`) are deterministic unsupported paths
- `bypassDocumentValidation` for aggregate is excluded from current differential corpus

## Update Semantics

Supported:
- operator updates: `$set`, `$inc`, `$unset`
- `arrayFilters` subset for `$set`/`$unset` paths using `$[identifier]` bindings
- update pipeline subset: `$set`/`$unset` stages with literal values
- replacement updates (with `multi=false`)
- upsert for operator and replacement forms
- same update constraints apply to `bulkWrite` update/replace operations

Not supported:
- advanced `arrayFilters` forms (unsupported operators, missing bindings, unsupported path/operator combinations)
- positional updates (`$`, `$[]`)
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
- `runCommand` command names outside the imported subset (`ping`, `buildInfo`, `listIndexes`, `listCollections`, `count`)
- update operations using unsupported `arrayFilters` forms (outside `$set`/`$unset` subset)
- update pipeline forms outside the supported subset (`$set`/`$unset` stages with literal values)
- replacement updates requested with `multi=true`

## UTF Import Profiles

Supported importer profiles:
- `strict` (default): deterministic mode; policy-only operations (`failPoint`, `targetedFailPoint`, runOn/skip-only scenarios) are imported through deterministic no-op lanes.
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
- collation runtime semantics are subset-only (`locale`/`strength`/`caseLevel`)
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
