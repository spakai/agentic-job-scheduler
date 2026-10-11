# L06: Bound retained work and demonstrate recovery

Status: in progress

## Scope and plan

- Requirement: implement the Spec 002 L06 backpressure/recovery increment,
  acceptance IDs B14–B20.
- Ordered follow-up authorized on 2026-10-10: address the outstanding runtime
  cases in this order: B07 retry concurrency/rate and attempt progression; B13
  worker adaptive limits; B14 worker-under-pressure integration; B15
  out-of-order and independent-partition commits; B19 unconfirmed cancellation;
  B11 rebalance/stale callbacks; B18 sustained broker outage/recovery. Keep
  each acceptance independently reviewable and record test evidence and any
  remaining gap before starting the next.
- Current slice plan — B19: clarify the handler cancellation contract before
  implementing timeout cancellation. The current handler Future has no cancel
  acknowledgment; preserve the tested shutdown/no-premature-commit behavior and
  record what additional contract is required to close B19.
- Base commit and branch: PR #9 merge commit `bffaf8a`; current continuation
  branch `feat/l06-rebalance-stale-callbacks`.
- Current continuation slice (2026-10-10): complete Spec 002 B14–B20 runtime
  acceptance by wiring the existing library components into runnable main and
  retry workers, adding explicit topic provisioning, a sample producer and
  replay-safe demo handler, and Compose-based broker-backed recovery tests.
  Previous window/controller work remains in the merged baseline.
- Authorized scope and unresolved decisions: implement on the current Kafka-first
  architecture. The merged-main baseline includes the B14 controller and
  component suite; worker wiring and integration fixtures are authorized to
  cover B14–B20. Preserve independent main/retry groups and the no-store
  architecture. Docker availability is checked again before container commands.
- Expected behavior and invariant for the bounded-window slice: one
  `ProcessingWindow` can be shared across intake paths; reservations track record
  count and key/value byte count until the corresponding source commit is
  acknowledged. It pauses at either adjusted high watermark, allows at most one
  configured fetch overshoot, and resumes only below 50% of both watermarks.
- Expected behavior: honor the Spec 002 topic/group/payload contracts; preserve
  per-topic same-job serialization, main/retry independence, bounded retention,
  pause-while-polling, and explicit completed-prefix commits. Retry/DLQ source
  records complete only after acknowledged publication; failed sends retain the
  original result and do not rerun handlers. Malformed/oversized/wrong-partition
  records are routed to bounded DLQ records. Rebalance and shutdown invalidate
  local ownership and cannot manufacture completion for unconfirmed handler
  cancellation. Restart begins at Kafka's committed offsets and replay remains
  possible. Compose has separately scalable main/retry roles and persistent
  Kafka storage, without a database or state store.
- Files/components expected to change: runtime/config/bootstrap sources,
  publisher/consumer adapters, sample producer and handler, Kafka topic
  provisioning, unit and Testcontainers integration tests, Dockerfile/Compose,
  README/architecture/spec acceptance status, and this task/evidence record.
- Validation approach: preserve/test component contracts; build runnable JAR;
  use uniquely named Testcontainers topics/groups to exercise retry/DLQ
  acknowledgment, invalid and wrong-partition input, commit/replay boundaries,
  cancellation outcomes, and rebalance/restart behavior; verify Compose roles
  and persistent Kafka data when Docker is available; run clean `mvn verify`,
  `git diff --check`, and a resolved-dependency CVE scan.
- Boundaries: do not introduce PostgreSQL, Flyway, REST, delayed scheduling,
  scheduling state topics, RocksDB, global ordering, durable deduplication, or
  external-effect fencing. Do not close L06 based on adapter-only tests; report
  each unsupported or untested acceptance explicitly.
- Non-goals: adding cross-topic exclusion, durable deduplication, global
  ordering, exactly-once effects, or external-effect fencing.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: Maven, Git,
  Docker/Compose, official documentation, and the Maven ecosystem CVE assessment,
  used only for this repository and uniquely named L06 test resources. Docker is
  shared; existing containers are not to be stopped or modified.
- Read/write operations needed: modify source/build/test/docs and add container
  artifacts; run project builds/tests and short-lived project-owned containers.
- Environment constraints and recovery path: Java 21/Maven and Docker Desktop
  available. No existing Kafka application runtime exists. Keep external runtime
  failures separate from unit test outcomes; stop only containers created for
  this task.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  shell/Git/Docker are unrestricted local capabilities; project scope is an
  instruction, not a technically enforced sandbox.

## Progress and decisions

- 2026-10-10: verified PR #6 merged as `8476630`; fast-forwarded local `main`,
  created `feat/l06-backpressure-recovery`, and passed `mvn -B -ntp clean test`
  on the merged baseline (115 tests, no failures/errors/skips).
- 2026-10-10: after Docker initially was unavailable in the WSL distro, the user
  reported Docker was starting. `docker version`, `docker compose version`, and
  `docker ps` now succeed. Existing `spec007-worker` containers were observed and
  will not be modified.
- 2026-10-10: confirmed official Vert.x KafkaConsumer documentation exposes
  per-partition pause/resume and assignment/revocation callbacks; confirmed
  Testcontainers official Kafka module documentation recommends its newer
  `org.testcontainers.kafka.KafkaContainer` API over the deprecated class.
- 2026-10-10: implemented `ProcessingWindow`, a process-shareable record/byte
  reservation ledger. The high watermark reserves headroom for one configured
  fetch; resume requires both retained count and bytes below half their adjusted
  high watermarks. Reservations remain held after a failed commit and are released
  only after the tracker commit succeeds.
- 2026-10-10: initially retained `ProcessingWindow` as a library component
  rather than wiring unimplemented Kafka consumers. Docker was available, but
  broker-backed evidence was still pending at that point; the following
  integration test supersedes that evidence state.
- 2026-10-10: added `ProcessingWindowKafkaIT`, a Testcontainers-backed B14
  integration case. It publishes eight records, reserves through the count
  watermark, pauses the real Kafka partition, continues polling without receiving
  paused records, acknowledges offset 4, releases the reservations, resumes, and
  consumes the remaining records.
- 2026-10-10: the full resolved Maven graph scan found eight advisories across
  Commons Compress 1.24.0 and LZ4 Java 1.10.1; a follow-up found one advisory in
  Commons Lang 3.14.0 pulled by the first patch. Overrode the resolved versions
  to patched releases, added Commons Codec needed by Commons Compress at
  Testcontainers startup, and rescanned the whole graph with no findings.
- 2026-10-10: added `ProcessingWindowKafkaController` to apply the shared window
  signal to assigned Vert.x Kafka partitions. A Testcontainers integration test
  drives the pinned Vert.x consumer through assignment, pause, continued polling
  without delivery, acknowledged source commit, reservation release, resume, and
  consumption of the remaining offsets. Pause/resume futures remain visible to
  callers; rebalance integration is still pending.
- 2026-10-10: the first broker run caught that the controller had been bound to
  the test's initial context rather than the consumer's operation context.
  Constructing it from the context of the completed assignment future fixed the
  ownership mismatch; the focused broker test then passed.
- 2026-10-10: `appmod-cve-assessment` checked the newly added resolved
  `io.vertx:vertx-kafka-client:5.2.1` coordinate and reported no known findings.
  The prior full resolved-graph scan remains clean for dependencies already
  present before this client was added.
- 2026-10-10: PR #7 merged as `3b8cdc3`; its Java 21 CI check passed. Started
  `feat/l06-worker-runtime` from the refreshed `origin/main` to continue the
  still-open L06 acceptance, without mixing the separate Spec 03 draft.
- 2026-10-10: scoped this continuation to runnable main/retry roles, Kafka
  publish/commit wiring, invalid-input DLQ, bounded window retention, and
  recovery/Compose evidence. Numerical performance targets and chaos campaigns
  are not L06 deliverables; Spec 002 B18 outage/replay remains in L06.
- 2026-10-10: added the runnable Kafka main/retry worker, idempotent topic
  provisioning, retry/DLQ encoding, sample producer, and Compose services. The
  focused and full clean verification passed with 123 unit tests and two
  Testcontainers integration tests.
- 2026-10-10: clean verification exposed an adaptive-limiter timer callback on
  Vert.x's global timer context. Marshaling it to the worker owner context removed
  the dispatcher context exception; a rebuilt Compose sample was processed with
  no context exception in fresh worker logs.
- 2026-10-10: Compose was run under project `l06-worker-runtime`. Scaled
  `retry-worker` to two replicas; with `main-worker` scaled to zero, published a
  job and confirmed its main-topic offset remained queued. Restarted the
  persistent Kafka container, restored one main worker, and observed that job
  processed. Published malformed JSON and read its bounded `INVALID_JSON` record
  from the DLQ. The latest sample job was also processed by the rebuilt image.
  The Compose stack and named volume remain running/preserved.
- 2026-10-10: extended `KafkaWorkerIT` to lower the retry topic message-size
  limit, forcing retry handoff send rejection. The test confirms the main
  handler runs once, the source offset does not advance, the retry handler does
  not run before acknowledgment, and restoring the limit allows the identical
  handoff to complete and the source to commit. The run also exposed unhandled
  producer event errors; registering the producer exception handler made the
  rejection visible as a warning while preserving bounded retry behavior.

- 2026-10-10 (after PC restart): working tree still based on `3b8cdc3`. Docker
  was initially unavailable in WSL (Testcontainers failed with "Could not find a
  valid Docker environment"; environment failure, not a code regression). After
  Docker Desktop WSL integration returned (server 29.3.1), `mvn -B -ntp clean
  verify` passed: 123 unit tests, `KafkaWorkerIT` and `ProcessingWindowKafkaIT`
  (2 integration tests), 0 failures/errors/skips; `git diff --check` clean. The
  Compose project `l06-worker-runtime` came back up (kafka healthy, main worker,
  two retry workers) with no exceptions in the last 10 minutes of worker logs.
  This adds no new acceptance coverage; the gaps listed below remain open.
- 2026-10-10 (health check on pushed snapshot `7e9e7d0`): ran
  `mvn -B -ntp clean verify` from the feature branch after recovering the branch
  onto healthy `origin/main` history. A clean build compiled 22 production and
  eight test sources; all 123 unit tests and four Testcontainers integration
  tests passed (0 failures, errors, or skips), and Maven built the JAR. The four
  integrations were two `KafkaWorkerIT` cases, one `KafkaWorkerCrashIT` case,
  and one `ProcessingWindowKafkaIT` case. This verifies the code on the pushed
  snapshot but does not close L06: the worker-level B15/B19, B14 under-pressure
  integration, sustained broker-outage B18, and worker-level B11/B13 cases
  remain unverified. The run emitted expected `RecordTooLargeException`
  warnings from the injected failed-handoff test; that test passed after
  verifying the source offset stayed put and recovery succeeded.
- 2026-10-10 (B07): the first worker-level retry test exposed that the retry
  start rate was hard-coded to one start per second despite Spec 002 requiring
  both retry concurrency and rate to be configurable. Added
  `WORKER_RETRY_STARTS_PER_SECOND` (default 1), wired it through Compose, and
  added a broker-backed controlled-handler test. With rate 4/s and concurrency
  2, it verifies spacing between handler starts, no third active handler while
  both slots are held, a retryable attempt-2 failure publishing attempt 3, and
  successful consumption/commit of that next attempt. `WorkerConfigTest` checks
  the default, override, and invalid zero rate. Focused command
  `mvn -B -ntp -Dtest=WorkerConfigTest -Dit.test=KafkaWorkerIT verify` passed:
  two unit tests and three Kafka worker integration tests, no failures/errors/
  skips. B07 is now covered at worker level; B13 remains a separate open case.
- 2026-10-11 (B13): a focused unit assertion exposed a limiter defect: healthy
  increases were delayed until 20 qualifying windows rather than one window
  with at least 20 completed attempts. Removed the cumulative-window gate while
  preserving the per-window sample threshold. Added a broker-backed main-worker
  test proving the limit rises from 4 to 5 under healthy backlog and later
  reduces to 2 after a slow window with sufficient completions. The focused
  limiter unit test passed (5 tests); the full `KafkaWorkerIT` class plus
  `WorkerConfigTest` and limiter tests passed (4 integration, 7 unit tests; no
  failures/errors/skips). The first integration attempt did not allow a full
  slow sampling interval before checking the reduced limit; the test was
  adjusted to observe through two intervals and passed. Evidence is recorded
  for the working tree based on `94f5550`; B14 worker-pressure testing is next.
- 2026-10-11 (B14): added package-private processing-window injection so the
  worker integration can use a small hard bound without changing production
  defaults. The broker test held the first same-job handler, observed the
  worker pause at its count watermark with no committed source offset and no
  further retained batch, then released it and verified all 20 records
  completed, committed, and released the window. Combined with
  `ProcessingWindowKafkaIT`, which confirms polling continues while the real
  Vert.x partition is paused and resumes after commit acknowledgment, this
  covers the worker pressure interaction. Focused combined run passed: 10 unit
  tests and 6 Testcontainers integration tests, 0 failures/errors/skips. B15 is
  now next.
- 2026-10-11 (B15): added a worker-level test with offset 0 held on one
  partition, offset 1 completed behind it, and offset 0 completed on a second
  partition. The second partition committed to 1 while the first stayed at
  `-1`; after releasing offset 0, the first partition committed to 2. Combined
  validation passed: 29 selected unit tests, including offset-gap tracker
  cases, and 7 Testcontainers integration tests, 0 failures/errors/skips. B19
  unconfirmed cancellation is next.
- 2026-10-11 (B19 partial): a worker shutdown test confirms an unresolved
  handler keeps its same-job successor gated, leaves the source offset
  uncommitted, and keeps shutdown pending. Confirming the handler allows both
  records to complete and commit. The focused check passed (2 unit tests and 1
  Testcontainers worker integration); a clean full verify then passed 125 unit
  tests and 9 integrations with no failures/errors/skips. This does not test a
  timeout cancellation acknowledgment: `JobHandler` has no cancellation API,
  and the worker currently waits until its 30-second close timeout before
  closing clients with unfinished records. B11 rebalance/stale-callback testing
  is next; B19 remains partially open pending the cancellation contract.
- 2026-10-11 (B11): a two-member Testcontainers rebalance test now proves the
  replacement owner replays the revoked partition, the old handler callback
  neither starts its queued same-job successor nor commits, and the new owner
  serially processes both records before committing offset 2. It passed twice
  focused. The first full run caught a forced-close timer callback executing
  outside the worker owner context; marshaling it onto the owner context fixed
  cleanup. Full clean verification passed with 125 unit tests and 10
  Testcontainers integrations, 0 failures/errors/skips. B18 is next.
- 2026-10-11 (B18): a Testcontainers test paused the task-owned Kafka broker for
  three seconds while a worker handler was active, completed that handler during
  the outage, then resumed the broker. The worker committed the retained source
  record, consumed a later record, and committed through offset 2 without
  rerunning the first handler. The focused case passed twice. Combined with the
  existing Compose evidence of persistent-volume broker restart and queued-work
  replay, B18 is covered. Full clean verification passed with 125 unit tests and
  11 Testcontainers integrations, 0 failures/errors/skips. B19 is next.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Broker outage and worker recovery (B18) | `ddc8ecd` | `mvn -B -ntp -Dtest=WorkerConfigTest -Dit.test=KafkaWorkerIT#workerRecoversCommitsAndPollingAfterKafkaBrokerOutage verify` | passed twice; 2 unit tests and 1 Testcontainers worker integration per run; full suite also passed | [L06 validation evidence](evidence/L06-validation.txt) |
| Rebalance revocation and stale handler callback (B11) | `ddc8ecd` | `mvn -B -ntp -Dtest=WorkerConfigTest -Dit.test=KafkaWorkerIT#workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner verify` | passed twice; 2 unit tests and 1 Testcontainers worker integration per run | [L06 validation evidence](evidence/L06-validation.txt) |
| Unconfirmed handler during worker shutdown (partial B19) | `6ad2d2e` | `mvn -B -ntp -Dtest=WorkerConfigTest -Dit.test=KafkaWorkerIT#workerShutdownWaitsForUnconfirmedHandlerBeforeCommittingSource verify` | passed: 2 unit tests and 1 Testcontainers worker integration; no early commit; cancellation API remains unimplemented | [L06 validation evidence](evidence/L06-validation.txt) |
| Out-of-order prefix and independent partition commits (B15) | `6ad2d2e` | `mvn -B -ntp -Dtest=WorkerConfigTest,L04RetryAndLimiterTest,OffsetCommitTrackerTest,ProcessingWindowTest -Dit.test=KafkaWorkerIT,ProcessingWindowKafkaIT verify` | passed: 29 unit tests and 7 Testcontainers integration tests; 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Worker bounded-window pressure and pause/resume (B14) | `6ad2d2e` | `mvn -B -ntp -Dtest=ProcessingWindowTest,L04RetryAndLimiterTest -Dit.test=KafkaWorkerIT,ProcessingWindowKafkaIT verify` | passed: 10 unit tests and 6 Testcontainers integration tests; 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Main adaptive limit (B13) | `6ad2d2e` | `mvn -B -ntp -Dtest=WorkerConfigTest,L04RetryAndLimiterTest -Dit.test=KafkaWorkerIT verify` | passed: 7 unit tests and 4 Testcontainers Kafka worker integration tests; 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Retry rate/concurrency and attempt progression (B07) | working tree | `mvn -B -ntp -Dtest=WorkerConfigTest -Dit.test=KafkaWorkerIT verify` | passed: 2 unit tests and 3 Testcontainers Kafka worker integration tests; 0 failures/errors/skips | current session output, 2026-10-10 |
| Fresh health check of pushed feature snapshot | `7e9e7d0` | `mvn -B -ntp clean verify` | passed: 123 unit tests and 4 Testcontainers integration tests; 0 failures/errors/skips; JAR built | current session output; [L06 validation evidence](evidence/L06-validation.txt) |
| Merged L05 baseline | `8476630` | `mvn -B -ntp clean test` | passed: 115 tests, 0 failures/errors/skips | session output; retain final L06 evidence |
| Processing-window component (partial B14) | working tree | `mvn -B -ntp -Dtest=ProcessingWindowTest test` | passed: 5 tests, 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Vert.x Kafka pause/poll/commit/resume (partial B14) | working tree | `mvn -B -ntp -Dit.test=ProcessingWindowKafkaIT verify` | passed: 120 unit tests and 1 Testcontainers Vert.x Kafka integration test, 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Maven dependency CVE scan and patched rescan | working tree | `appmod-cve-assessment` against the resolved Maven test graph | final rescan clean; initial and intermediate findings fixed | session artifact `files/l06-cve/` |
| Safe-prefix and handoff boundaries (B15/B16) | L06 working tree | `mvn -B -ntp clean verify` | passed in tracker unit tests and worker-level B15 integration; B16 handoff acknowledgment remains covered by KafkaWorkerIT | [L06 validation evidence](evidence/L06-validation.txt) |
| Worker retry/DLQ handoff, bounded invalid-record DLQ, failed retry-send retention/recovery (B06/B16/B17) | L06 working tree | `mvn -B -ntp clean -Dit.test=KafkaWorkerIT verify` | passed: 123 unit tests and 1 Testcontainers worker integration test; rejected sends logged and recovered, no repeated handler invocation | [L06 validation evidence](evidence/L06-validation.txt) |
| Compose roles, two retry replicas, bounded DLQ, persistent broker restart and queued main replay (partial B18/B20) | L06 working tree | `docker compose -p l06-worker-runtime up --build --scale retry-worker=2 --detach`; publish/scale/restart/inspect commands recorded in evidence | passed manually; services remain running with named volume preserved | [L06 validation evidence](evidence/L06-validation.txt) |
| Full B14–B20 runtime acceptance | L06 working tree | `mvn -B -ntp clean verify` plus manual Compose checks | partial: see gaps below; not complete | [L06 validation evidence](evidence/L06-validation.txt) |
| Full clean build, unit suite, Kafka integration, and package | `ddc8ecd` | `mvn -B -ntp clean verify` | passed: 125 unit tests and 11 Testcontainers integration tests; JAR built | [L06 validation evidence](evidence/L06-validation.txt) |
| Whitespace | L06 working tree | `git diff --check` | passed after source, test, and documentation updates | [L06 validation evidence](evidence/L06-validation.txt) |
| Editor diagnostics | L06 working tree | VS Code diagnostics for the touched Java source and test files | no errors | current session output |
| Docker/Testcontainers broker | working tree | `mvn -B -ntp verify` | passed; L06 Kafka container started and cleaned up; pre-existing `spec007-worker` containers untouched | [L06 validation evidence](evidence/L06-validation.txt) |
| CI/security/restricted-agent exercises | n/a | workflows/scans not yet selected | not run | no claim |

Negative window tests reject fetch-bound violations, prevent new reservations
while paused, prevent foreign/double release, and verify that a failed commit
retains the window reservation. The worker integration now covers failed retry
publication and recovery. B11 revocation and stale-handler behavior are covered
by a real consumer-group rebalance test. B18 is covered by the broker
pause/recovery test plus the Compose persistent-broker restart and replay
evidence. Still unverified is timeout cancellation acknowledgment (B19). B19's
shutdown wait and no-premature-commit boundary is covered, but the handler API
cannot confirm cancellation. B15 out-of-order prefix and independent-partition
commits are covered at worker level; B13 adaptive-limit behavior is covered by
controlled healthy and slow windows; B14 pressure behavior is covered by the
worker test together with the pause-while-polling controller test. B09/B20 replay
after SIGKILL of a real worker JVM is covered
by `KafkaWorkerCrashIT`. Defect found and fixed: completions arriving while a
commit was in flight were not re-batched after commit success, leaving offsets
under-committed (surfaced as intermittent `KafkaWorkerIT` failures, ~1 in 3).
Manual
B08 is now covered by `KafkaWorkerIT` (permanent and exhausted failures reach
the DLQ; source offsets commit after DLQ acknowledgment). Manual
broker restart tests persistence/replay of queued main work, not client behavior
during a sustained broker outage.

## Failure analysis and correction

- Observed failure and reproduction: the first window test run failed because
  tests passed batches larger than their configured one-fetch limits. The next
  run exposed two incorrect assertions: a test expected resume before both
  watermarks were below half, and an overshoot test expected a rejected batch to
  be counted. The first clean verify after the dependency CVE overrides then
  failed at Testcontainers startup with
  `NoClassDefFoundError: org/apache/commons/codec/Charsets`.
- Classification: test design/reasoning.
- Root cause supported by evidence: test fixtures contradicted the window's
  configured maximum fetch values and low-water hysteresis; after pause, a
  refused reservation intentionally leaves counters unchanged.
- Code, instruction, or tool change: corrected test batches and assertions to
  stay within the contract, assert hysteresis against both adjusted watermarks,
  and prove an outstanding fetch may cross the high threshold without exceeding
  the configured hard bound. Added test-scoped Commons Codec because Commons
  Compress declares it optional but Testcontainers uses the dependent API.
- Rerun and remaining uncertainty: after the timer-context fix, full clean
  verification passed (123 unit, 2 broker integration tests). Compose processed
  jobs, routed malformed data to DLQ, and replayed queued data after persistent
  broker restart. The injected B16 send failure/recovery integration passed.
  Full B14–B20 acceptance remains open for the specific cases listed above.

- B11 follow-up failure: the first broker test combined initial records with
  same-job successors and only observed two running handler heads before its
  timeout. Separating startup delivery from successor delivery made ownership
  and queued work deterministic. A later cleanup timed out because the shutdown
  timer called dispatcher owner-context APIs from another context; marshaling
  forced close through the captured owner context corrected it. Focused test
  passed twice and clean verification passed.

## Handoff and review

- Merged baseline: PR #9 merged to `main` at `bffaf8a`; its Java 21 test/package
  CI check passed. Commit `ddc8ecd` on `feat/l06-rebalance-stale-callbacks` adds
  B11/B18 and is pushed. [PR #10](https://github.com/spakai/agentic-job-scheduler/pull/10)
  targets `main`; its Java 21 check is in progress. The original local history,
  which contained missing Git objects and could not be pushed, is preserved
  locally as `backup/feat-l06-worker-runtime-corrupt-history`. Unrelated untracked
  `.vscode/settings.json` remains untouched.
- Completed steps: implemented executable main/retry workers and Compose demo;
  B07, B11–B15, and B18 worker behavior are covered by broker tests; B19 has
  partial shutdown/no-premature-commit evidence. A fresh
  `mvn -B -ntp clean verify` passed on the current working tree with 125 unit
  tests and 11 Testcontainers
  integrations; prior evidence records broker
  retry/DLQ, crash/replay, failed-send recovery, and Compose scaling/DLQ/
  persistent broker restart.
- Next concrete step: clarify the B19 cancellation contract before implementing
  timeout cancellation, updating
  evidence after each actual check, and only close L06 when the acceptance table
  is adequately covered.
- Open risks/decisions and owner: B19 cancellation semantics remain unverified.
  No exactly-once, global ordering,
  durable deduplication, or external-effect fencing claim is made.
- Reviewer findings and disposition: pending.
- PR / final outcome: L06 remains in progress; PR #9 is merged and PR #10 is
  open. B11/B18 are covered; B19 cancellation remains open.

## Learning check

- What invariant did this task establish? Source completion waits for successful
  handler completion or Kafka acknowledgment of retry/DLQ publication; failed
  retry sends retain the source offset and do not rerun the handler.
- What failure would invalidate the claim? A committed source offset before
  acknowledged handoff, or a second handler invocation while the first
  handoff-send remains unacknowledged.
- Which artifact proves what actually happened? The broker-backed
  `KafkaWorkerIT` result and command log in
  [L06 validation evidence](evidence/L06-validation.txt).
