# Node Runtime Log Redaction Policy

This policy defines how `@jongodb/memory-server` redacts sensitive values in runtime startup logs and failure messages.

## Scope

Applies to launcher output lines captured from:
- child `stdout`
- child `stderr`
- startup timeout/failure error text assembled from captured log tails

## Redaction Rules

The runtime replaces secret values with `<redacted>` for:

1. MongoDB URI credentials
   - example: `mongodb://user:password@host/db`
2. Key/value assignments containing sensitive keys
   - keys: `password`, `passwd`, `pwd`, `token`, `secret`, `apiKey`, `accessToken` (including `_`/`-` variants)
3. Query-string style secret parameters
   - example: `...?password=...&token=...`
4. JSON-style secret fields
   - example: `"password":"..."`, `"token":"..."`

## Behavior Guarantees

- Redaction is applied before log lines are stored in the in-memory log tail buffers.
- Startup failure messages and timeout diagnostics use redacted log tails.
- Redaction does not change launcher contract detection:
  - `JONGODB_URI=...`
  - `JONGODB_START_FAILURE=...`

## Non-Goals

- This policy does not classify arbitrary business-domain secrets.
- Redaction is pattern-based and best-effort; avoid printing sensitive payloads from custom launcher code whenever possible.

## Validation

Security regression coverage is included in:
- `packages/memory-server/src/test/binary-launcher.test.ts`
  - test: secret-bearing launcher failure must not leak raw secret tokens
