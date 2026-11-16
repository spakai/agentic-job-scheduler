# Task: Separate consumer and execution verticles

Status: ready for review

## Scope and plan

- Requirement: one Kafka consumer verticle per role process/pod, assigned zero
  or more partitions; pass jobs to separate execution verticles.
- Spec 002 acceptance IDs: B03-B05, B11-B12, B14-B15, B19; Spec 005 C12.
- Base commit and branch: `1a06fc1`, `feature/consumer-worker-verticles`.
- Expected behavior: Kafka ownership, jobId gates, handoffs, bounded work and
  completed-prefix commits remain with the consumer. Execution verticles run
  nonblocking handlers and return settlement without owning Kafka partitions.
- Steps: deploy an execution pool and consumer verticle, preserve lifecycle and
  assignment checks, exercise unit and Kafka tests, and update Spec 002.
- Non-goals: global ordering, durable deduplication, fencing, new retry policy,
  blocking handler support, or Kubernetes deployment.

## Tools and boundaries

- Local Git branch, Java/Maven build, and the already-running Docker daemon.
- Maven and Java 21 were installed in session-local storage; GitHub writes and
  branch protection were not checked. Integration tests create test containers.
- The web documentation host was unavailable; the pinned Vert.x 5.2.1 source
  artifact from Maven Central was inspected for deployment and context APIs.

## Progress and decisions

- 2026-11-13: Pool size is the smaller of configured initial concurrency and
  four. Verticles handle asynchronous invocations; the existing adaptive and
  retry gates remain the actual execution limits.
- Assignment activity is checked before invoking the handler. A start racing
  revocation remains subject to Spec 002's lack of external fencing.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Initial Kafka topology | `1a06fc1` + working tree | `mvn -q -Dtest=ExecutionVerticlePoolTest -Dit.test='KafkaWorkerIT#consumerVerticleOwnsMultiplePartitionsAndDispatchesSafely' verify` | Failed on shutdown; see failure analysis | `target/failsafe-reports/` at that run |
| Pool, B03-B05, B11, B15, B19, C12 | `1a06fc1` + working tree | `mvn -q -Dtest=ExecutionVerticlePoolTest -Dit.test='KafkaWorkerCooperativeIT,KafkaWorkerIT#consumerVerticleOwnsMultiplePartitionsAndDispatchesSafely+timedOutHandlerRetriesAndReleasesGateOnlyAfterStopConfirmation' verify` | Passed: 2 unit and 4 Kafka cases | `target/surefire-reports/`, `target/failsafe-reports/` at that run |
| Earlier full unit, package, integration | `1a06fc1` + working tree | `mvn -q test && mvn -q package && mvn -q verify` | Unit and package passed; integration failed 1 of 23 cases (retry-rate fourth start) | Earlier local reports; overwritten by focused reruns |
| Final units | `1a06fc1` + final code diff | `mvn -q test` | Passed | `target/surefire-reports/` at final run |
| Final focused integrations and artifact | `1a06fc1` + final code diff | `mvn -q -Dtest=ExecutionVerticlePoolTest -Dit.test='KafkaWorkerCooperativeIT,KafkaWorkerIT#consumerVerticleOwnsMultiplePartitionsAndDispatchesSafely+timedOutHandlerRetriesAndReleasesGateOnlyAfterStopConfirmation+handlerTimeoutDoesNotRunWhileInvocationWaitsForWorkerContext+retryWorkerEnforcesConfiguredRateAndConcurrencyAndAdvancesAttempts' verify` | Passed: 3 pool units and 6 Kafka cases; artifact built | `target/surefire-reports/`, `target/failsafe-reports/` at final run |
| Full branch verification | `b8b93ad` | `mvn -B -ntp verify` | Passed: 135 unit and 24 Kafka integration tests; artifact built | `target/surefire-reports/`, `target/failsafe-reports/` |

## Failure analysis and correction

- The initial Kafka test failed with `Unknown deployment` during shutdown:
  Vert.x undeployed the consumer's child execution pool before its stop hook.
- The execution pool is now a separate root deployment. The runtime first
  undeploys and drains the consumer, then undeploys the pool; the Kafka test
  explicitly exercises shutdown with a blocked handler and queued successor.
- The corrected targeted run passed on Java 21.
- The earlier retry-rate failure also passed in the full 24-case integration run
  on `b8b93ad`; the cause of the initial failure remains unestablished.
- A follow-up timeout test initially failed because the start callback installed
  the timeout on the worker context. It now returns to the consumer context
  before installing timers; the final focused run passed.
- The full local integration suite passed. The GitHub CI result is still pending;
  CI now runs `mvn verify` and uploads both test report directories.

## Handoff and review

- Verified code diff against base `1a06fc1`; publishing as a draft PR.
- Spec 002, README and `arch.md` describe the new topology and lifecycle.
- Full local verification passed; final CI results and human review remain pending.

## Learning check

- Invariant: each partition's consumer owns its completion frontier even when
  different execution verticles finish jobs out of order.
- Failure: committing a later completion past a blocked earlier record could
  permanently skip unfinished work.
- Evidence: the Kafka topology test blocks one record and checks that partition
  cannot commit, while another partition commits independently.
