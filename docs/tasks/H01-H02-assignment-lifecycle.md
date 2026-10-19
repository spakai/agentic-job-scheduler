# H01-H02: Retire revoked assignment reservations and invalidate rate-gated retries

Status: ready for review

## Scope and plan

- Requirement / issue: implement the assignment-lifecycle acceptance criteria
  from Spec 003; acceptance IDs H01, H02.
- Spec 002 acceptance IDs: none (Spec 003 scope; see
  [specs/003-worker-runtime-correctness.md](../../specs/003-worker-runtime-correctness.md)).
- Base commit and branch: base commit `06ad660` (merge of
  `feat/spec03-worker-runtime-correctness`); working branch
  `feat/h01-h02-assignment-lifecycle`.
- Authorized scope and unresolved decisions: retire reservations for revoked
  assignments (queued-and-never-started, and running-but-settles-after-revoke)
  without ever completing/committing the revoked source record, and invalidate
  retry requests still waiting behind the shared retry rate gate once their
  owning assignment is revoked or the worker is shutting down. No unresolved
  design choice blocks this scoped change.
- Expected behavior and invariant: one execution per jobId at a time within
  each topic under stable ownership; different IDs may run concurrently; main
  and retry workers remain independent and may overlap for the same jobId.
  Never-started QUEUED work discarded at revoke time releases its local
  reservation immediately but never completes/commits its source record. A
  RUNNING handler keeps its reservation until its future settles, even if
  revoked mid-flight; at settle time, if its assignment was revoked, the
  reservation is released without completing/committing the source (duplicate
  handoffs remain possible). A retry request still sitting behind the shared
  rate gate when its owner is revoked (by rebalance or by worker shutdown) is
  never dispatched and never fabricates an outcome; its reservation is
  released via the same discard path. Already-running handlers are not
  affected by rate-gate invalidation; they keep their permits until they
  settle.
- Files/components expected to change: `ImmediateJobDispatcher`,
  `KafkaWorker` (`RateGate`, `executeHandler`, `discardQueuedWork`,
  `releaseDiscardedReservation`, `releaseGroup`), focused dispatcher tests,
  `KafkaWorkerIT`, and this task record.
- Steps and validation approach: expose `queuedSources`/`queuedRecords` from
  the dispatcher under an owner-context guard; discard queued work and
  release its reservation at revoke time (rebalance revoke and worker
  shutdown); re-check ownership at handler-settle time so a late-settling
  running handler still releases its reservation; thread an `isValid`/
  `onInvalidated` pair through the retry rate gate so a queued-but-not-yet-
  dispatched request is skipped once its owner is revoked. Test the unit-level
  dispatcher contract directly; extend/add integration-level worker tests for
  the end-to-end rebalance and forced-shutdown scenarios. Run
  `mvn -B -ntp clean test`, `mvn -B -ntp clean package`, and
  `git diff --check`.
- Non-goals: durable deduplication, external-effect fencing/cancellation,
  global ordering, delayed jobs, a scheduling-state topic, RocksDB indexes,
  or EDR (all explicitly deferred per Spec 003 and repository direction).

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local
  file inspection and patching, Git branch/status/commit operations, and
  Maven build/test/package in this repository. Local Git/Maven capabilities
  are not a sandbox or an enforced restriction.
- Read/write operations needed: read Spec 003 and the existing dispatcher/
  worker implementation; modify `ImmediateJobDispatcher`, `KafkaWorker`,
  their tests, and this task record/evidence.
- Environment constraints and recovery path: no Docker is available in this
  environment, so the Testcontainers-based `KafkaWorkerIT` suite can be
  written and compiled here but cannot be executed. If a compile/unit-test
  failure is found, retain the failure output, diagnose the smallest
  relevant scope, and rerun after correction (this happened once; see
  Failure analysis below).
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  no real Kafka broker, consumer-group rebalance, or Testcontainers run was
  exercised in this environment; the new/extended IT scenarios are unverified
  beyond compilation.

## Progress and decisions

- Production code for H01 (`queuedSources`/`queuedRecords`,
  `discardQueuedWork`, `releaseDiscardedReservation`, `releaseGroup`, the
  settle-time revoke re-check in `executeHandler`) and H02 (the `isValid`/
  `onInvalidated` parameters on `RateGate.submit` and its `startNext` skip
  loop) was implemented in a prior segment, ahead of this task record.
- Decision: `queuedSources`/`queuedRecords` are confined to the dispatcher's
  owner `Context`, matching the existing admission/dispatch guard pattern, so
  a caller cannot query or discard queued work for a dispatcher it does not
  own.
- Decision: the retry rate gate's `startNext` loop drains and discards
  invalid (owner-revoked) requests until it finds a still-valid one or the
  queue is empty, calling `onInvalidated` for each skipped request so its
  reservation is released; it never invokes the operation or settles the
  promise for a skipped request.
- Added `queuedSourcesReturnsOnlyNeverStartedSubmissionsAndEnforcesOwnerContext`
  to `ImmediateJobDispatcherTest`, asserting `queuedSources` returns exactly
  the QUEUED submission(s), excludes RUNNING ones, throws
  `IllegalArgumentException` from a non-owner context, and returns empty once
  the queued submission starts. Ran in isolation and as part of the full
  suite; passed.
- Extended the existing B11 test
  `workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner` in `KafkaWorkerIT`
  with H01 assertions: reservations drop by one immediately after rebalance
  (QUEUED discard), and drop by a second one once the stale RUNNING handler
  settles (settle-after-revoke release). Updated its Javadoc to mention H01.
- Added a new `KafkaWorkerIT` test,
  `revokedOwnershipInvalidatesRateGateQueuedRetryBeforeItEverDispatches`, for
  H02: a RETRY worker with `concurrency=2`, `topicPartitions=2`,
  `startsPerSecond=1`, and a 500ms forced shutdown timeout; two jobs on
  distinct partitions both reach the shared rate gate, one is dispatched
  immediately (the winner) and the other is queued behind the 1s pacing
  interval (the loser); the test discovers winner/loser dynamically (call
  order is not assumed), forces `close()` so the shutdown timer revokes
  ownership well before the rate gate's pending timer fires, then asserts the
  loser is never dispatched, never republished as a new retry attempt or
  dead-lettered (new helper `assertNoHandoffForJob`), and that only the
  still-running winner's reservation remains retained.
- While validating with `mvn -B -ntp clean test` (not previously run in this
  segment with `clean`), found that `RateGate.startNext()`'s loop variable
  was captured by a lambda while not effectively final, a real compile error
  in the prior segment's production code that a non-clean incremental build
  had been masking. Fixed by hoisting the dequeued, validated request into a
  separate `final` local assigned exactly once outside the loop. See Failure
  analysis below.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Focused dispatcher test (new H01 unit test) | `0133a84` | `mvn -B -ntp -Dtest=ImmediateJobDispatcherTest test` | passed: 21 tests, 0 failures/errors/skips | [H01-H02 validation evidence](evidence/H01-H02-validation.txt) |
| Full unit suite | `0133a84` | `mvn -B -ntp clean test` | passed: 127 tests, 0 failures/errors/skips | [H01-H02 validation evidence](evidence/H01-H02-validation.txt) |
| Clean library package | `0133a84` | `mvn -B -ntp clean package` | passed: 127 tests; JAR built | [H01-H02 validation evidence](evidence/H01-H02-validation.txt) |
| Diff whitespace | `0133a84` | `git diff --check` | passed | [H01-H02 validation evidence](evidence/H01-H02-validation.txt) |
| H01 rebalance scenario (`KafkaWorkerIT.workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner`, extended) | n/a | Testcontainers/Kafka integration; no Docker in this environment | not run (compiled only) | no execution claim |
| H02 forced-shutdown scenario (`KafkaWorkerIT.revokedOwnershipInvalidatesRateGateQueuedRetryBeforeItEverDispatches`) | n/a | Testcontainers/Kafka integration; no Docker in this environment | not run (compiled only) | no execution claim |

Negative cases verify that: a QUEUED submission discarded at revoke time
never starts and its reservation is released without completing its source
record; a RUNNING handler that settles after revoke still releases its
reservation without completing its source record; and a retry request
invalidated behind the rate gate is neither dispatched, nor republished as a
new attempt, nor dead-lettered, while the still-running winner keeps its
reservation. The H01 negative cases are proven by the extended `KafkaWorkerIT`
scenario (written, unexecuted here); the H02 negative case is proven by the
new `KafkaWorkerIT` scenario (written, unexecuted here).

## Failure analysis and correction

- Observed failure and reproduction: `mvn -B -ntp clean test` failed to
  compile `KafkaWorker.java` with "local variables referenced from a lambda
  expression must be final or effectively final" at the two
  `request.result.tryComplete()`/`tryFail(...)` call sites inside
  `RateGate.startNext()`'s trailing `operation.onComplete(...)` lambda. A
  prior, non-`clean` `mvn test` run in this segment had not caught this
  because stale `target/classes` output was reused instead of recompiling.
- Classification: tool use (relied on an incremental, non-`clean` build to
  gate correctness) and stale context (the prior segment's "code-complete,
  127 tests passing" claim was based on that masked build).
- Root cause supported by evidence: `RateGate.startNext()` declared
  `RateRequest request;` and reassigned it (including to `null`) inside a
  `do…while` loop body, then captured `request` in a lambda after the loop.
  Because the assignment statement is lexically inside a loop, javac does
  not treat the variable as effectively final even though it is assigned to
  its final value exactly once per invocation at runtime.
- Code, instruction, or tool change: restructured `startNext()` to loop over
  a `RateRequest next` (reassigned only inside the loop, never captured) and
  assign a separate `final RateRequest request = next;` exactly once,
  outside the loop, for the lambda to capture. No test or spec change was
  needed; the fix is confined to the already-in-scope `RateGate`.
- Rerun and remaining uncertainty: `mvn -B -ntp clean test` and
  `mvn -B -ntp clean package` both pass cleanly after the fix (127 tests, 0
  failures). The underlying H01/H02 runtime behavior this fix sits inside
  remains unverified beyond compilation in this environment, since the
  `KafkaWorkerIT` scenarios that exercise it require Docker.

## Handoff and review

- Latest verified implementation commit: `0133a84`
  (`feat: retire revoked reservations and invalidate rate-gated retries
  (H01/H02)`) on `feat/h01-h02-assignment-lifecycle`; validation command
  output is recorded in
  [H01-H02 validation evidence](evidence/H01-H02-validation.txt).
  Pre-existing untracked `.vscode/` is unrelated and was preserved.
- Completed steps: reviewed Spec 003's H01/H02 criteria and the existing
  dispatcher/worker ownership model; added a focused unit test for
  `queuedSources`/`queuedRecords`; extended the existing rebalance IT test
  with H01 reservation-count assertions; added a new IT test for H02's
  rate-gate invalidation under forced shutdown; found and fixed a real
  compile bug in `RateGate.startNext()` surfaced by a clean rebuild; ran and
  recorded the full unit suite, clean package, and whitespace checks.
- Next concrete step: get a reviewer to confirm the H01/H02 test designs
  before relying on them, and run the `KafkaWorkerIT`/`KafkaWorkerCrashIT`
  suite against a real broker (Docker-enabled environment) to execute the
  two new/extended scenarios for the first time.
- Open risks/decisions and owner: the new and extended `KafkaWorkerIT`
  scenarios have never actually run; they are believed correct from careful
  reading of the implementation but could still contain a timing assumption
  that does not hold against a real broker. Owner: whoever picks up the next
  Docker-enabled validation pass.
- Reviewer findings and disposition: pending.
- PR / final outcome: not yet opened; pending explicit instruction to push.

## Learning check

- What invariant did this task establish? A revoked assignment's reservations
  are always retired (queued work discarded, running work released at
  settle-time) without ever completing/committing the revoked source, and a
  retry request still queued behind the shared rate gate when its owner is
  revoked is skipped rather than dispatched or fabricated.
- What failure would invalidate the claim? A queued submission starting after
  its owner was revoked, a running handler's late completion silently
  completing/committing a revoked source, or a rate-gate-queued request
  dispatching (or being treated as if it had) after its owner was revoked.
- Which artifact proves what actually happened? The passing unit suite in
  [H01-H02 validation evidence](evidence/H01-H02-validation.txt) proves the
  dispatcher-level contract; the `KafkaWorkerIT` scenarios in this commit are
  the intended end-to-end proof but remain unexecuted pending a
  Docker-enabled environment.
