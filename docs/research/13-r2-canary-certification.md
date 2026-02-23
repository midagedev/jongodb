# R2 External Spring Canary Certification

## Purpose
- Validate practical replacement readiness on real Spring projects.
- Enforce minimum project count and rollback rehearsal success.

## Generator
- Class: `org.jongodb.testkit.R2CanaryCertification`
- Gradle task: `r2CanaryCertificationEvidence`

## Input
- JSON file (default): `build/reports/spring-canary/projects.json`
- Schema:
  - `projects`: array of project entries
  - project fields:
    - `projectId` (string)
    - `repository` (string)
    - `revision` (string)
    - `canaryPassed` (boolean)
    - `rollback` (object)
      - `attempted` (boolean)
      - `success` (boolean)
      - `recoverySeconds` (number)
    - `notes` (optional string)

## Output
- JSON: `build/reports/r2-canary/r2-canary-certification.json`
- Markdown: `build/reports/r2-canary/r2-canary-certification.md`

## Gate Criteria
- Projects must be at least 3.
- `canaryPassed` must be true for all projects.
- Rollback rehearsal must be attempted and successful for all projects.

## Notes
- This generator evaluates provided project results; it does not execute projects itself.
- Use it as the final evidence pack validator before R2 sign-off.
