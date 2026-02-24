# Spec Sync Tests

Run the spec-sync integration test:

```bash
./testkit/spec-sync/test-spec-sync.sh
```

The test creates temporary local git repositories, runs sync/verify scripts,
and validates both success and mismatch-failure behavior.
