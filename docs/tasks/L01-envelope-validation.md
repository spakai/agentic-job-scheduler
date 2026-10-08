# L01: Validate main and retry job envelopes

Status: user review accepted; PR preparation

## Scope and plan

- User authorization: create a feature branch, record the plan, implement Java 21
  Maven foundation and main/retry envelope validation with unit tests; prepare review.
- Base: `eda1b72953690f2558031e4ce90f6bb49a14c1e2`.
- Branch: `feat/l01-envelope-validation`.
- Acceptance: B01 and the malformed/oversized input portion of B17. Retry envelope
  validation supports B07; it does not establish retry execution behavior.
- Invariant: invalid wire input never becomes a validated envelope; valid input
  retains identity/payload and applies the documented maxAttempts default.
- Plan: (1) pin build dependencies and define models, (2) implement bounded strict
  UTF-8/JSON validation, (3) exercise positive/negative/boundary cases, (4) inspect
  the diff and record actual results and remaining gaps.
- Files: pom.xml, messaging classes/tests, README, gitignore, this record, and
  documentation updates for implemented scope and concrete retry wire fields.
- Non-goals: Vert.x runtime, consumers, Kafka routing/partition verification,
  handlers, scheduling, limiter, offsets, retry publication, DLQ, Docker, CI.

## Tools and boundaries

- Local shell/Git edits this repository; Maven resolves dependencies and runs tests.
- Official release documentation verifies pinned dependency choices.
- Available: OpenJDK 21.0.12.1 and Maven 3.6.3. No Docker needed for unit tests.
- Implementation initially used local tools. The user subsequently authorized
  commit/push and PR preparation. MCP configuration, branch protection, and CI
  enforcement remain unverified; no restricted-tool boundary is demonstrated.

## Progress and decisions

- 2026-10-08: Created feature branch from a clean main working tree.
- Keep validation independent of Vert.x/Kafka clients; add those at integration.
- Clarify retry wire format with `job`, `attempt`, `handoffId`, `originalSource`,
  `failedSource`, and `errorCode`, plus top-level schemaVersion. Handoff ID is
  deterministic SHA-256 of failed-source coordinates and next attempt. Original
  source remains stable when later retries fail. No nested retry envelopes.
- Canonical identifiers use lowercase, full UUID strings. Registered job types
  are provided to the validator rather than hardcoded as production handlers.
- Unknown fields, duplicate JSON properties, malformed UTF-8, trailing JSON,
  coercible-but-wrong types, and explicit null defaults are invalid.

## Evaluation

| Check | Revision | Command | Result | Evidence |
| --- | --- | --- | --- | --- |
| Unit validation | base + L01 working tree | mvn -B -ntp test | passed, initial 70 cases | superseded by final package run below |
| Final clean test/package | base + L01 working tree | mvn -B -ntp clean package | passed, 77 tests, no failures/errors/skips; Java 21 bytecode | [evidence](evidence/L01-validation.txt) |
| Diff whitespace | base + L01 working tree | git diff --check | passed | local check |
| Kafka/DLQ integration | n/a | n/a | out of scope | no broker tests |
| Security scanning | n/a | n/a | not run | no scan claim |

## Failure analysis and correction

- Initial run passed 70 cases. Review added an independent handoff hash vector
  and source-offset boundary cases; the following clean build passed 76 cases.
- Self-review found a boundary risk: a main job at JSON depth 100 gains one level
  when wrapped for retry. Using depth 100 for both parsers could reject that job.
- Corrected retry depth to 101 and added a regression proving a depth-boundary
  main job remains valid when wrapped. The final clean build passed 77 cases.
  This was a review finding, not a claimed observed production failure.
- Corrected the spec's hash notation to a newline escape, consistent with the
  independent SHA-256 vector. No runtime failures or security scan results claimed.

## Handoff and review

- Implemented five messaging classes and one JUnit test class (77 executed cases),
  with pinned Java 21/Maven build and README instructions. Updated current-spec
  wire details and repository/learning status; no deferred runtime was added.
- Implementing-agent self-review completed. On 2026-10-08 the user accepted
  the review ("looks good, go ahead") and authorized task cleanup and PR preparation.
  No separate automated reviewer or GitHub approval is claimed.
- Review focus: exact retry wire names and hashing convention, strict rejection
  behavior, and the distinction between unit validation and full B17 coverage.
- Implementation commit `9c25206` was pushed to `feat/l01-envelope-validation`.
  Base is `eda1b72953690f2558031e4ce90f6bb49a14c1e2`; evaluated source hashes
  remain in the evidence file as a historical build snapshot. This follow-up
  changes documentation only. PR preparation follows; merge is pending.
- Remaining gaps: partition metadata/routing, actual DLQ publication, configured
  topic checks, Kafka consumption, limiter, retries, external handler behavior,
  security scanning, and CI. No Vert.x dependency is needed for pure validation.
- Next concrete step: open the L01 PR against main and retain its review history.
  Later integration connects this validator to Kafka routing and DLQ behavior.

## Learning check

A unit rejection proves the validator rejects input. It does not prove a consumer
publishes a DLQ record or commits the correct offset. Review tests for both
accepted input and a counterexample to each claimed validation rule.
