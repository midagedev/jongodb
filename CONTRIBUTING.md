# Contributing

## Development Setup

- Java 17+
- Gradle 8+

Run the full test suite before opening a PR:

```bash
gradle test
```

## Contribution Scope

Useful contribution areas:
- command compatibility gaps
- query/aggregation parity improvements
- test corpus expansion (UTF and differential)
- diagnostics and reproducibility tooling
- documentation quality and precision

## Pull Request Checklist

- tests added or updated for behavior changes
- no breaking behavior change without explicit note
- compatibility impact documented in `docs/COMPATIBILITY.md`
- roadmap impact reflected in `docs/ROADMAP.md` when relevant

## Style Notes

- prefer small, reviewable commits
- keep error behavior deterministic
- avoid introducing silent fallbacks for unsupported features
- keep language in docs clear and concrete
