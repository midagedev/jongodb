# Fixture Manifest

`#249` introduces a declarative manifest contract for fixture extraction planning.

Goal:
- keep extraction criteria in one versioned file
- standardize scenario profiles (`dev`, `smoke`, `full`)
- guarantee deterministic profile-level plans with a reproducible fingerprint

Current schema version:
- `fixture-manifest.v1`

## Schema (v1)

Top-level fields:
- `schemaVersion`: must be `fixture-manifest.v1`
- `source.uriAlias`: source connection alias (for example `prod-main`)
- `profiles.dev|smoke|full`: profile-specific extraction policy

Profile fields:
- `refreshMode`: `full` or `incremental`
- `fieldRules.include|exclude`: optional default include/exclude rules
- `collections[]`: per-collection extraction definitions

Collection fields:
- `database`, `collection`
- `filter`, `projection`, `sort`, `limit`
- `sample.size`, `sample.seed`
- `fieldRules.include|exclude` (collection override/append)

Validation highlights:
- all three profiles (`dev`, `smoke`, `full`) are required
- `limit` requires `sort`
- `sample` requires positive `size`, non-empty `seed`, and `sort`
- `incremental` profile requires `sort` for each collection
- include/exclude overlap is rejected
- all errors are aggregated with path-aware messages

## Deterministic Profile Plan

`FixtureExtractionPlanner.plan(...)` builds a profile plan only from a manifest.
There is no planner API without a manifest input.

The generated `FixtureExtractionPlan` includes:
- source/profile/refresh metadata
- canonical collection operations
- deterministic `fingerprint` (FNV-1a 64-bit hex)

This fingerprint can be stored in CI artifacts to verify that the same manifest/profile produced the same extraction intent.

## Sample Manifests

- `testkit/fixture/manifests/baseline-dev-smoke-full.json`
- `testkit/fixture/manifests/incremental-events.json`
- `testkit/fixture/manifests/sensitive-field-rules.json`

These templates are intended for onboarding and can be copied as a starting point.

## Java Usage

```java
Path manifestPath = Path.of("testkit/fixture/manifests/baseline-dev-smoke-full.json");
FixtureManifest manifest = FixtureManifestLoader.load(manifestPath);

FixtureExtractionPlan smokePlan = FixtureExtractionPlanner.plan(
        manifest,
        FixtureManifest.ScenarioProfile.SMOKE);

System.out.println(smokePlan.fingerprint());
```
