# Support Matrix

Status date: 2026-02-24

This matrix is a versioned support boundary for integration-test usage.
Source artifact: `build/reports/r2-compatibility/r2-support-manifest.json`.

## Summary

| Status | Count |
| --- | --- |
| Supported | 7 |
| Partial | 4 |
| Unsupported | 1 |

## Feature-Level Matrix

| Feature ID | Category | Status | Note |
| --- | --- | --- | --- |
| `query.eq-ne-compare` | query | Supported | Core comparison operators |
| `query.elemMatch-all-regex` | query | Supported | Array and regex operators |
| `query.expr-subset` | query | Partial | Subset: eq/ne/gt/gte/lt/lte/and/or/not |
| `aggregation.match-project-group` | aggregation | Supported | Tier-1 pipeline stages |
| `aggregation.lookup-union-facet` | aggregation | Partial | Tier-2 subset without full expression parity |
| `aggregation.expression-operators` | aggregation | Partial | Limited expression coverage |
| `index.unique-sparse-partial` | index | Supported | Unique/sparse/partial |
| `index.collation-metadata` | index | Supported | Collation metadata round-trip |
| `index.collation-semantic` | index | Unsupported | Locale-aware comparison semantics |
| `transactions-single-session` | transaction | Supported | Session + txn command flow |
| `transactions-retryable-advanced` | transaction | Partial | Partial compatibility |
| `protocol-unsupported-contract` | protocol | Supported | NotImplemented + UnsupportedFeature labels |

## Operational Notes

- The matrix is intended for test-environment substitution scenarios.
- Unsupported UTF coverage is tracked in `docs/COMPATIBILITY_SCORECARD.md` with reason counts.
- New feature expansion should update both this matrix and `docs/COMPATIBILITY.md`.

