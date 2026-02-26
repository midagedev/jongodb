# Node Artifact Cache Policy

`@jongodb/memory-server` stores classpath auto-discovery artifacts in a local cache to reduce repeated Gradle probes.

## Default Policy

- directory: `.jongodb/cache`
- max entries: `32`
- max size: `5 MiB` (`5_242_880` bytes)
- TTL: `7 days` (`604_800_000 ms`)

Pruning runs before each classpath auto-discovery read/write:
- remove expired entries (`mtime > TTL`)
- enforce entry-count limit (oldest-first removal)
- enforce total-size limit (oldest-first removal)

## Configuration

Runtime options:
- `artifactCacheDir`
- `artifactCacheMaxEntries`
- `artifactCacheMaxBytes`
- `artifactCacheTtlMs`

Environment variables:
- `JONGODB_ARTIFACT_CACHE_DIR`
- `JONGODB_ARTIFACT_CACHE_MAX_ENTRIES`
- `JONGODB_ARTIFACT_CACHE_MAX_BYTES`
- `JONGODB_ARTIFACT_CACHE_TTL_MS`

## Operational Notes

- Cache is best-effort; malformed cache files are discarded automatically.
- Delete `.jongodb/cache` to force a clean probe path.
- Keep the cache under project workspace storage (avoid shared global temp paths in CI).
