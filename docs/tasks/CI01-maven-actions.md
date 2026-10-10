# CI01: Run existing Maven checks on GitHub

Status: in progress

## Scope and plan

- User authorized basic GitHub Actions before L03, including publication and run verification.
- Base: `d612d57` (merged L02); branch `feat/maven-ci`.
- Spec 002: automate existing B01/B17 validation and B05/B15 library checks,
  including simulated B09. No additional runtime acceptance claimed.
- Expected behavior: PRs targeting main and pushes to main run Java 21 Maven
  packaging (including tests); failures fail the job. Retain test reports even
  after failure and the JAR after success.
- Files: workflow, README, learning ledger, task record.
- Validation: local Maven test/package, workflow inspection, real PR workflow run
  and artifact inspection. A controlled failing test will verify failure reporting.
- Boundaries: no merge, deployment, branch protection changes, Kafka integration,
  security scanning, or agent-in-CI exercise. These remain later L06 work.

## Tools and boundaries

- Shell/Git/Maven operate locally; gh publishes branch/PR and reads Actions results.
- Shell filesystem/network unrestricted; authorization is task scope, not sandboxing.
- CI uses contents:read, no persisted checkout credentials, and no repository secrets.
- Actions pinned to release commit SHAs verified from official repositories on
  2026-10-10: checkout v7.0.1, setup-java v6.0.1, upload-artifact v7.0.2.
- Official references: https://github.com/actions/checkout,
  https://github.com/actions/setup-java, https://github.com/actions/upload-artifact.
- Branch protections/rulesets remain unverified. A workflow alone does not block merging.

## Progress and decisions

- Use ubuntu-24.04, Temurin 21, Maven dependency caching, and a 15-minute timeout.
- Cancel obsolete runs on the same PR/ref. Use pull_request, not pull_request_target.
- Package includes tests, so CI runs one Maven lifecycle. Local checks follow repository instructions.
- Retain reports/JAR for 14 days. Missing expected artifacts fail their upload step.

## Evaluation

| Check | Revision | Command/workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Unit/package | `d612d57` + workflow/docs | `mvn -B -ntp test`; `mvn -B -ntp package` | passed: 94 tests each, no failures/errors/skips | local Surefire reports; baseline commands repeated in CI |
| Real PR run | pending | Maven CI | pending | pending |
| Controlled failure | `beea4e2` | temporary failing test | expected failure: 95 tests, one intentional failure; reports uploaded, JAR skipped | [run 38018094448](https://github.com/spakai/agentic-job-scheduler/actions/runs/38018094448) |
| Kafka/security scans | n/a | n/a | not run | out of scope |

## Failure analysis and correction

- Controlled negative evaluation at `beea4e2`: intentional JUnit failure caused
  the Maven step/job to fail. The report upload succeeded and JAR upload was skipped.
  Downloaded the test-reports artifact and confirmed the failure was the deliberate
  CI01 assertion. Removed the temporary test for the final positive run.
- This is expected evaluation evidence, not a production defect.

## Handoff and review

- [PR #3](https://github.com/spakai/agentic-job-scheduler/pull/3) created as draft
  while the negative probe was present. Next: verify final passing run and mark ready.
- No production runtime code changes intended. L03 remains a separate increment.

## Learning check

- Invariant: a failing Maven build must produce a failed CI job and retain diagnostic reports.
- Evidence: a real successful run and a controlled failed run, linked to commits.
- Check: does a red Actions check itself prevent merging? Only if applicable repository rules require it.
