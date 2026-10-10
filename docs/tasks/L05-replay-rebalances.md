# L05: Ignore work callbacks after partition ownership is revoked

Status: ready for review

## Scope and plan

- Requirement: implement the library-side replay and rebalance boundary from
  Spec 002; acceptance IDs B09–B12.
- Base commit and branch: base commit `156139c` (L04); working branch
  `feat/l05-replay-rebalances`.
- Authorized scope and unresolved decisions: add ownership invalidation to the
  fixed-assignment dispatcher. Kafka revoke callbacks, handler cancellation, and
  broker commits remain adapter responsibilities; no unresolved design choice
  blocks this scoped change.
- Expected behavior and invariant: after revocation, an owner cannot admit or
  dispatch work, and handler, handoff, or commit callbacks from the stale owner
  cannot advance completion or offsets. A fresh dispatcher/tracker serves a new
  assignment. Completed work may replay; handlers preserve jobId/executionId and
  tolerate duplicates. Main and retry dispatchers remain independent and may
  overlap for the same jobId.
- Files/components expected to change: `ImmediateJobDispatcher`, focused
  dispatcher tests, `arch.md`, and this task record.
- Steps and validation approach: implement a context-confined revoke transition;
  guard admission/dispatch and stale callbacks; test revoked handler/handoff/commit
  paths and replay through a fresh dispatcher; run `mvn -B -ntp clean test`,
  `mvn -B -ntp package`, and `git diff --check`.
- Non-goals: Kafka runtime or consumer-group integration, cancellation/fencing of
  already-started external effects, durable deduplication, cross-topic exclusion,
  global ordering, or claims of live broker crash/rebalance behavior.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local file
  inspection and patching, Git branch/status operations, and Maven tests/package
  in this repository. Local Git/Maven capabilities are not a sandbox or an
  enforced restriction.
- Read/write operations needed: read Spec 002 and existing execution contracts;
  modify the dispatcher, related tests, architecture summary, and task evidence.
- Environment constraints and recovery path: no Kafka worker runtime or integration
  suite exists in this increment. If a test/build fails, retain the failure output,
  diagnose the smallest relevant scope, and rerun after correction.
- Evidence of enforced restrictions, or explicitly unverified restrictions: no
  broker, ownership callback, or external-effect fencing is exercised here.

## Progress and decisions

- 2026-10-10: started L05 from the committed L04 baseline. Existing coverage
  includes simulated crash/replay (B09), invocation identity/no deduplication
  (B10), and independent main/retry overlap (B12). The new behavior is stale-owner
  invalidation (B11).
- Decision: revocation is serialized on the dispatcher's owner context. It does
  not cancel running handlers or claim to fence their external effects. A fresh
  dispatcher and tracker are required for a new ownership epoch.
- Decision: a handler-stop signal can remain incomplete if ownership is revoked
  before its callback is processed; the old submission and dispatcher must be
  discarded. The callback cannot release a gate, advance offsets, or dispatch
  queued work after revocation.
- 2026-10-10: implemented revocation guards and three controlled tests for late
  handler completion, handoff acknowledgment, and commit result. Updated the
  architecture summary to distinguish the local lifecycle hook from real Kafka
  rebalance integration.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Focused dispatcher tests | `156139c` + working tree | `mvn -B -ntp -Dtest=ImmediateJobDispatcherTest test` | passed: 17 tests, 0 failures/errors/skips | [L05 validation evidence](evidence/L05-validation.txt) |
| Full suite, including replay/identity/overlap (B09/B10/B12) and stale callbacks (B11) | `156139c` + working tree | `mvn -B -ntp clean test` | passed: 115 tests, 0 failures/errors/skips | [L05 validation evidence](evidence/L05-validation.txt) |
| Clean library package | `156139c` + working tree | `mvn -B -ntp clean package` | passed: 115 tests; JAR built | [L05 validation evidence](evidence/L05-validation.txt) |
| Diff whitespace | `156139c` + working tree | `git diff --check` | passed | [L05 validation evidence](evidence/L05-validation.txt) |
| Editor diagnostics | `156139c` + working tree | Problems panel for dispatcher and test | no errors | [L05 validation evidence](evidence/L05-validation.txt) |
| Kafka rebalance/crash integration | n/a | runtime/suite not implemented | not run | no integration claim |

Negative cases verify that a late handler completion cannot dispatch queued work,
a stale handoff acknowledgment cannot complete the source, and an old commit
result cannot release records. A fresh owner can replay and complete the work.

## Failure analysis and correction

- Observed failure and reproduction: none.
- Classification: n/a
- Root cause supported by evidence: n/a
- Code, instruction, or tool change: no corrective change was needed; initial
  focused and full suite runs passed.
- Rerun and remaining uncertainty: tests pass; real consumer rebalances and external
  operation completion remain outside this library test boundary.

## Handoff and review

- Latest verified commit and working-tree changes: base
  `156139c180302b01c4f9ff1d2b7d784a87b6f1b2` plus L05 changes on
  `feat/l05-replay-rebalances`; validation input hashes are in
  [L05 validation evidence](evidence/L05-validation.txt). Pre-existing untracked
  `.vscode/` is unrelated and was preserved.
- Completed steps: reviewed Spec 002, added context-confined revocation and stale
  callback guards, added replay/revoke tests, updated architecture and recorded
  passing focused/full/package checks.
- Next concrete step: review the diff; then decide whether to commit and publish a PR.
- Open risks/decisions and owner: callers must discard a revoked dispatcher and
  create a fresh instance for reassigned partitions; adapter and handler owners
  must tolerate replay and possible continuing external work.
- Reviewer findings and disposition: pending
- PR / final outcome: local branch is ready for review; no commit or PR created.

## Learning check

- What invariant did this task establish? After ownership revocation, the old
  dispatcher cannot admit/dispatch work or accept stale completion/commit effects.
- What failure would invalidate the claim? A delayed old-owner callback completing
  a source record or allowing queued work to start after revocation.
- Which artifact proves what actually happened? The L05 unit tests and the
  recorded Maven results in this task record.
