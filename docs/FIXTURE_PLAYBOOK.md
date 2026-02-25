# Fixture Playbook

Audience:
- Developer: generate/update fixture intent for feature work.
- Reviewer: verify safety, determinism, and approval gates.
- Operator: enforce security/branch policy and release hygiene.

Scope:
- `jongodb` fixture workflow based on manifest-driven planning (`fixture-manifest.v1`).
- extraction transport/restore artifact pipeline will expand with `#250`/`#253`, but this playbook is executable now for manifest governance and Spring test reset workflow.

## 1) Developer Guide

### 1.1 First-Time Setup (Create)

1. Start from a template:
   - `testkit/fixture/manifests/baseline-dev-smoke-full.json`
   - `testkit/fixture/manifests/incremental-events.json`
   - `testkit/fixture/manifests/sensitive-field-rules.json`
2. Adjust `source.uriAlias`, collection clauses, and field rules.
3. Validate and render deterministic plan:

```bash
gradle fixtureManifestPlan \
  -PfixtureManifestPath=testkit/fixture/manifests/baseline-dev-smoke-full.json \
  -PfixtureProfile=dev
```

Optional canonical JSON output:

```bash
gradle fixtureManifestPlan \
  -PfixtureManifestPath=testkit/fixture/manifests/baseline-dev-smoke-full.json \
  -PfixtureProfile=dev \
  -PfixturePlanJson=true
```

4. Save the emitted `fingerprint` in your PR description for reviewer comparison.

### 1.2 Local Restore/Test Loop

For Spring integration tests, use reset-first workflow to avoid data bleed:

1. Use `@JongodbMongoTest` or `JongodbMongoInitializer`.
2. Reset state before each test slice:

```java
@Autowired
ApplicationContext context;

@BeforeEach
void reset() {
    JongodbMongoResetSupport.reset(context);
}
```

3. Seed fixture data through repository/template setup code in test fixtures.

This gives deterministic local replay even before dedicated restore CLI (`#253`) lands.

### 1.3 Refresh Request and Approval

When requesting fixture updates:

1. Include manifest diff + old/new `fingerprint`.
2. Explain why refresh mode is `full` or `incremental`.
3. Declare field-rule changes (especially PII-related include/exclude edits).
4. Request explicit reviewer approval before merging.

## 2) Reviewer Guide

Reviewer checklist:

1. `schemaVersion` is `fixture-manifest.v1`.
2. All three profiles exist: `dev`, `smoke`, `full`.
3. Determinism guardrails hold:
   - `limit` has `sort`.
   - `sample` has `size`, `seed`, and `sort`.
   - incremental profile collections have `sort`.
4. Field rules are safe:
   - no include/exclude overlap.
   - no newly exposed sensitive fields without justification.
5. PR contains `fixtureManifestPlan` fingerprint evidence.

## 3) Troubleshooting

### 3.1 Restore Failure / Data Bleed

Symptoms:
- tests pass individually but fail in suite order.

Actions:
1. Ensure reset hook (`JongodbMongoResetSupport.reset`) runs before each test.
2. If shared server mode is enabled, verify per-test database naming policy.
3. Disable `jongodb.test.sharedServer` temporarily for isolation triage.

### 3.2 Version Mismatch

Symptoms:
- manifest tool fails with schema validation errors.

Actions:
1. Confirm `schemaVersion` is `fixture-manifest.v1`.
2. Re-run:

```bash
gradle fixtureManifestPlan -PfixtureManifestPath=<path> -PfixtureProfile=dev
```

3. If still failing, fix path-specific errors from validator output.

### 3.3 Drift Threshold Exceeded (Policy)

Suggested policy:
- if profile fingerprint changes unexpectedly between adjacent PRs, mark as drift candidate and require additional approval.

Actions:
1. Compare previous PR fingerprint vs current fingerprint for same profile.
2. If drift is unplanned, block merge and request manifest clause-level explanation.

## 4) Operator Guide

### 4.1 Security Principles

MUST:
- use read-only credentials for extraction-oriented workflows.
- keep secrets in CI secret store or local secure vault, never in manifest.
- mask/exclude sensitive fields in `fieldRules`.

MUST NOT:
- commit raw secrets, connection strings, or production credentials.
- broaden include rules for sensitive namespaces without security review.

### 4.2 Branch and Release Strategy

Recommended branch flow:
1. `issue/<id>-...` branch per fixture change.
2. PR includes manifest diff + fingerprint evidence.
3. Merge only after CI + reviewer checklist completion.
4. Keep fixture policy docs (`docs/FIXTURE_MANIFEST.md`, this playbook) aligned with release notes.

### 4.3 Incident Handling

If a fixture-related incident is reported:
1. Freeze new fixture refresh merges.
2. Identify first bad fingerprint change.
3. Revert manifest change or re-approve with corrected field/sort constraints.
4. Document root cause in PR and update this playbook.

## Related Docs

- `docs/FIXTURE_MANIFEST.md`
- `docs/SPRING_TESTING.md`
- `docs/USAGE.md`
