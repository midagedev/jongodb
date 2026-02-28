# R3 Issue #58: External Canary Certification Flow

Date: 2026-02-24
Status: implemented and certified

## Objective

Provide a deterministic certification flow for external Spring canary projects with rollback evidence.

## Added Flow

- Gradle task: `r3CanaryCertificationEvidence`
- Workflow: `.github/workflows/r3-external-canary.yml`
- Maintained schedule input: `testkit/canary/r3/projects.mainline.json`
- Sample input: `testkit/canary/r3/projects.sample.json`

The flow validates:

- at least 3 project results
- all `canaryPassed=true`
- rollback rehearsal attempted and successful for all projects

## Reproduction

```bash
gradle r3CanaryCertificationEvidence \
  -Pr3CanaryInputJson="testkit/canary/r3/projects.mainline.json" \
  -Pr3CanaryOutputDir="build/reports/r3-canary" \
  -Pr3CanaryFailOnGate=true
```

## Output

- `build/reports/r3-canary/r2-canary-certification.json`
- `build/reports/r3-canary/r2-canary-certification.md`
- `build/reports/r3-canary/r3-canary-gate-summary.md`

Certification reference:
- GitHub Actions run `22332937633` (`R3 External Canary Certification`)
- summary: `overallStatus=PASS`, `projectCount=3`, `rollbackSuccess=3`

## Notes

- The current implementation reuses `R2CanaryCertification` logic with R3-specific task/workflow wiring.
- Scheduled `main` runs use the maintained input set and publish a concise gate summary for triage.
