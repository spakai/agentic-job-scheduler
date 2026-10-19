# H01-H02: Make integration reservation assertions batch-aware

Status: ready for review

## Scope and plan

- Requirement / issue: correct the H01/H02 integration assertions that failed
  on the first Docker-enabled run; Spec 003 acceptance IDs H01 and H02.
- Spec 002 acceptance IDs: none; this follow-up remains within Spec 003.
- Base commit and branch: base commit `763eac5` (merged H01/H02 PR #15);
  working branch `fix/h01-h02-integration-reservation-tests`.
- Authorized scope and unresolved decisions: adjust only the H01/H02
  Testcontainers scenarios and their evidence record. No production behavior
  change is planned unless the rerun shows a production defect.
- Expected behavior and invariant: each test must distinguish a retired
  record's reservation from unrelated records that share the same fetched-batch
  reservation. H01 must demonstrate that at least one queued reservation is
  retired on rebalance and that the replayed job's running reservation is
  retired only after its stale handler settles. H02 must demonstrate that an
  invalidated rate-gate request is not dispatched or handed off and that its
  reservation is retired while the still-running job retains its reservation.
- Files/components expected to change: `KafkaWorkerIT.java`, this task record,
  and `docs/tasks/evidence/H01-H02-integration-reservation-tests.txt`.
- Steps and validation approach: submit records in separate, observed Kafka
  fetch batches so each is represented by an independently releasable
  `ProcessingWindow.Reservation`; change rebalance assertions to compare
  against the observed post-revoke baseline; isolate stale-handler reservation
  retirement in a deterministic one-partition forced-shutdown/replay scenario;
  rerun the targeted `KafkaWorkerIT` methods and the full
  `mvn -B -ntp clean verify` suite with Docker; run `git diff --check`.
- Non-goals: changing the batch-level reservation contract, altering H01/H02
  production code, or implementing H03/H04.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local
  source/test inspection, patching, Git branch operations, and Maven tests
  in this repository. Docker Desktop is available through WSL integration.
  These capabilities are not a sandbox or an enforced restriction.
- Read/write operations needed: modify the scoped integration test and add
  task/evidence documentation; preserve unrelated `.vscode/` user files.
- Environment constraints and recovery path: the first targeted Failsafe run
  had Docker access and completed all 12 `KafkaWorkerIT` test methods, but two
  new H01/H02 assertions failed. Continue from the reported reservation counts;
  do not claim integration success until the rerun passes.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  Docker accessibility was verified with `docker info`; no restrictions are
  claimed as enforced.

## Progress and decisions

- 2026-10-19: PR #15 was verified merged at `763eac5`. The merged Maven CI
  passed its unit-test/package workflow, which does not execute Failsafe ITs.
- 2026-10-19: ran `mvn -B -ntp -Dit.test=KafkaWorkerIT verify` with Docker
  available. Unit tests passed (127/127). Failsafe executed 12 `KafkaWorkerIT`
  methods: 10 passed and 2 failed on reservation-count assertions.
- Decision: `ProcessingWindow` reserves a fetched Kafka batch as one
  reservation; `KafkaWorker.ReservationGroup` releases that reservation only
  after every record represented by it is retired. Therefore, assertions that
  expect one-record decrements are valid only when the test establishes
  one-record fetch batches; tests must not infer batch boundaries from producer
  send calls alone.
- Failure 1: H02 expected `retainedRecords()==1`, but the two retry records
  shared a single batch reservation, so retirement of the queued loser did not
  release the batch reservation while the winner remained running.
- Failure 2: H01 expected exactly one reservation record to disappear at
  rebalance, but multiple queued successors in revoked partitions were retired
  together; the observed count dropped from 5 to 3. The test also needs to
  measure the actual post-revoke count before asserting the later running
  handler's one-record release.
- First correction: H02 now submits the first retry, observes its dispatch and
  reservation, then submits the second; this gives the running winner and
  rate-gate-queued loser independent single-record reservations. The B11
  rebalance assertion now checks that the aggregate decreases rather than
  assuming exactly one queued record was revoked. An attempted stale-handler
  aggregate assertion then observed 6 retained records where it expected 2;
  the aggregate changed during the multi-member group reassignment and was not
  a stable signal for one stale handler's reservation.
- Final test design: keep the rebalance test's assertion that queued work was
  retired (without assuming the number of revoked partitions), and add a
  one-partition forced-shutdown test with separately observed batches. It
  verifies the queued successor releases before the old handler settles, the
  running reservation remains until settlement, and a replacement worker
  replays both uncommitted records.
- 2026-10-19: the two corrected H01/H02 tests plus the existing B11 rebalance
  test passed. Then the complete clean verify lifecycle passed: 127 unit tests,
  13 `KafkaWorkerIT` tests, 1 `KafkaWorkerCrashIT` test, and 1
  `ProcessingWindowKafkaIT` test; all had zero failures/errors/skips.
- 2026-10-19: committed the verified test/evidence changes as `419a153`
  (`test: make H01/H02 integration assertions batch-aware`) and opened PR #16
  against `main`.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Initial Docker-enabled H01/H02 integration run | `763eac5` | `mvn -B -ntp -Dit.test=KafkaWorkerIT verify` | failed: 127 unit tests passed; 10/12 ITs passed, 2 assertion failures | [H01-H02 integration validation evidence](evidence/H01-H02-integration-reservation-tests.txt) |
| Targeted corrected H01/H02 and B11 integration cases | `419a153` | `mvn -B -ntp '-Dit.test=KafkaWorkerIT#revokedOwnershipInvalidatesRateGateQueuedRetryBeforeItEverDispatches+revokedAssignmentRetiresQueuedAndSettledHandlerReservations+workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner' verify` | passed: 127 unit tests; 3 selected ITs, 0 failures/errors/skips | [H01-H02 integration validation evidence](evidence/H01-H02-integration-reservation-tests.txt) |
| Full clean verification, including unit, package, and broker-backed integration tests | `419a153` | `mvn -B -ntp clean verify` | passed: 127 unit tests + 15 ITs, 0 failures/errors/skips; package built | [H01-H02 integration validation evidence](evidence/H01-H02-integration-reservation-tests.txt) |
| Diff whitespace | `419a153` | `git diff --check` | passed | [H01-H02 integration validation evidence](evidence/H01-H02-integration-reservation-tests.txt) |

## Failure analysis and correction

- Observed failure and reproduction: the initial run showed H02 retained 2
  records instead of 1 and the rebalance test retained 3 instead of the
  expected 5. After separating batches and baselining the rebalance count, the
  stale-handler assertion still failed: it expected 2 but observed 6 after
  group reassignment.
- Classification: test design / reasoning.
- Root cause supported by evidence: the test counted records as if each had an
  independent window reservation. `ProcessingWindow` reserves a fetched batch,
  and a group is released only when all records in that batch are retired.
  Additionally, the multi-member rebalance test's aggregate window count can
  change as the old consumer receives a new assignment and refetches uncommitted
  records, so it cannot isolate one stale handler's release.
- Code, instruction, or tool change: observe each record's admission before
  sending the next; make the rebalance test assert only a decrease for queued
  retirement; add a controlled single-partition shutdown/replacement scenario
  to assert queued retirement and stale-handler settlement separately.
- Rerun and remaining uncertainty: the three targeted integration cases passed.
  `mvn -B -ntp clean verify` also passed the full unit, packaging, and all
  broker-backed integration tests. No H01/H02 test remains unexecuted in this
  verification scope.

## Handoff and review

- Latest verified commit: `419a153`
  (`test: make H01/H02 integration assertions batch-aware`) on
  `fix/h01-h02-integration-reservation-tests`, based on `763eac5`.
- Completed steps: confirmed PR #15 merge; enabled Docker-backed Testcontainers
  execution; captured and diagnosed the initial assertion failures; made the
  test scenarios batch-aware; reran the focused cases and the complete
  `clean verify` suite successfully.
- Next concrete step: review and merge PR #16, then proceed to Spec 003 H03/H04.
- Open risks/decisions and owner: none identified by this validation run;
  normal code review remains pending.
- Reviewer findings and disposition: pending.
- PR / final outcome: open PR #16; awaiting review/merge.

## Learning check

- What invariant did this task establish? A reservation assertion must respect
  batch-level accounting; queued work is retired at revocation, and running
  work remains accounted until its stale handler settles.
- What failure would invalidate the claim? A test assumes a batch boundary
  without observing one, or accepts a retained count that hides an unreleased
  reservation.
- Which artifact proves what actually happened? The targeted and complete
  Failsafe reports plus command outcomes in the evidence file.
