# L06: Bound retained work and demonstrate recovery

Status: in progress

## Scope and plan

- Requirement: implement the Spec 002 L06 backpressure/recovery increment,
  acceptance IDs B14–B20.
- Base commit and branch: L06 B14 controller slice merged in PR #7 at
  `3b8cdc3`; runtime continuation branch `feat/l06-worker-runtime`.
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
  integration, sustained broker-outage B18, and worker-level B07/B11/B13 cases
  remain unverified. The run emitted expected `RecordTooLargeException`
  warnings from the injected failed-handoff test; that test passed after
  verifying the source offset stayed put and recovery succeeded.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Fresh health check of pushed feature snapshot | `7e9e7d0` | `mvn -B -ntp clean verify` | passed: 123 unit tests and 4 Testcontainers integration tests; 0 failures/errors/skips; JAR built | current session output; [L06 validation evidence](evidence/L06-validation.txt) |
| Merged L05 baseline | `8476630` | `mvn -B -ntp clean test` | passed: 115 tests, 0 failures/errors/skips | session output; retain final L06 evidence |
| Processing-window component (partial B14) | working tree | `mvn -B -ntp -Dtest=ProcessingWindowTest test` | passed: 5 tests, 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Vert.x Kafka pause/poll/commit/resume (partial B14) | working tree | `mvn -B -ntp -Dit.test=ProcessingWindowKafkaIT verify` | passed: 120 unit tests and 1 Testcontainers Vert.x Kafka integration test, 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Maven dependency CVE scan and patched rescan | working tree | `appmod-cve-assessment` against the resolved Maven test graph | final rescan clean; initial and intermediate findings fixed | session artifact `files/l06-cve/` |
| Safe-prefix and handoff boundaries (B15/B16 library/component coverage) | merged baseline + L06 tree | `mvn -B -ntp clean verify` | passed in unit suite; worker-level out-of-order/independent-partition acceptance not yet covered | [L06 validation evidence](evidence/L06-validation.txt) |
| Worker retry/DLQ handoff, bounded invalid-record DLQ, failed retry-send retention/recovery (B06/B16/B17) | L06 working tree | `mvn -B -ntp clean -Dit.test=KafkaWorkerIT verify` | passed: 123 unit tests and 1 Testcontainers worker integration test; rejected sends logged and recovered, no repeated handler invocation | [L06 validation evidence](evidence/L06-validation.txt) |
| Compose roles, two retry replicas, bounded DLQ, persistent broker restart and queued main replay (partial B18/B20) | L06 working tree | `docker compose -p l06-worker-runtime up --build --scale retry-worker=2 --detach`; publish/scale/restart/inspect commands recorded in evidence | passed manually; services remain running with named volume preserved | [L06 validation evidence](evidence/L06-validation.txt) |
| Full B14–B20 runtime acceptance | L06 working tree | `mvn -B -ntp clean verify` plus manual Compose checks | partial: see gaps below; not complete | [L06 validation evidence](evidence/L06-validation.txt) |
| Full clean build, unit suite, Kafka integration, and package | L06 working tree | `mvn -B -ntp clean verify` | passed: 123 unit tests and 2 Testcontainers integration tests; JAR built | [L06 validation evidence](evidence/L06-validation.txt) |
| Whitespace | L06 working tree | `git diff --check` | to rerun after documentation updates | [L06 validation evidence](evidence/L06-validation.txt) |
| Editor diagnostics | L06 working tree | Problems panel for `ProcessingWindow` and its test | no errors | [L06 validation evidence](evidence/L06-validation.txt) |
| Docker/Testcontainers broker | working tree | `mvn -B -ntp verify` | passed; L06 Kafka container started and cleaned up; pre-existing `spec007-worker` containers untouched | [L06 validation evidence](evidence/L06-validation.txt) |
| CI/security/restricted-agent exercises | n/a | workflows/scans not yet selected | not run | no claim |

Negative window tests reject fetch-bound violations, prevent new reservations
while paused, prevent foreign/double release, and verify that a failed commit
retains the window reservation. The worker integration now covers failed retry
publication and recovery. Still unverified are rebalance ownership/stale callback
behavior in the worker, B15 with out-of-order completion across independent Kafka
partitions, controlled runtime limiter/rate behavior (B07/B13), confirmed/unconfirmed worker
cancellation (B19). B09/B20 replay after SIGKILL of a real worker JVM is covered
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

## Handoff and review

- Latest verified commit: `7e9e7d0` on `feat/l06-worker-runtime`; the feature
  snapshot is pushed to `origin/feat/l06-worker-runtime`. Its committed tree
  matches the original feature snapshot. The original local history, which
  contained missing Git objects and could not be pushed, is preserved locally
  as `backup/feat-l06-worker-runtime-corrupt-history`. The current health-check
  documentation changes are pending commit. Unrelated untracked
  `.vscode/settings.json` remains untouched.
- Completed steps: implemented executable main/retry workers and Compose demo;
  fresh `mvn -B -ntp clean verify` passed on `7e9e7d0` with 123 unit tests and
  four Testcontainers integration tests; prior evidence records broker
  retry/DLQ, crash/replay, failed-send recovery, and Compose scaling/DLQ/
  persistent broker restart.
- Next concrete step: address the remaining B14–B20 gaps listed above, update
  evidence after each actual check, and only close L06 when the acceptance table
  is adequately covered.
- Open risks/decisions and owner: worker-level rebalance/commit-frontier and
  cancellation semantics remain unverified. No exactly-once, global ordering,
  durable deduplication, or external-effect fencing claim is made.
- Reviewer findings and disposition: pending.
- PR / final outcome: L06 remains in progress; no commit or PR created.

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
