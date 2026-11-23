# Task API01: HTTP admission implementation plan

Status: planned

## Scope and plan

- Requirement / issue: Implement [Spec 006](../../specs/006-http-admission-api.md)
  so HTTP clients can submit canonical jobs without operating a Kafka producer.
- Spec 006 acceptance IDs: API01-API16.
- Base commit and branch: record the implementation branch and current `HEAD`
  when execution starts. Use a feature branch; do not merge the existing
  `demo.md` and demo-graph working-tree changes into this task.
- Authorized scope: HTTP admission only. Keep Spec 002 worker ownership,
  handler execution, retry, DLQ, and offset behavior unchanged.
- Expected behavior and invariant: `202 Accepted` is returned only after
  `jobs.main.v1` acknowledges a valid, keyed Spec 002 envelope. The API is a
  stateless admission adapter, not a status store or scheduler.
- Files/components expected to change: `pom.xml`, `Dockerfile`, `compose.yaml`,
  `src/main/java/com/example/agenticjobscheduler/admission/**`,
  focused admission tests, `README.md`, and `arch.md`.
- Steps and validation approach: deliver the phases below in order; run focused
  tests after each slice, `mvn test` and `mvn package` for the complete unit/build
  gate, and broker-backed `mvn verify` coverage when Docker is available.
- Non-goals: PostgreSQL, Flyway, REST queries/status, delayed jobs, durable
  idempotency, HTTP-triggered execution, Kafka state topics, outbox storage,
  batch admission, authentication implementation, TLS provisioning, and
  Kubernetes.

## Planning context

### Current implementation facts

- [EnvelopeValidator](/home/spakai/agentic-job-scheduler/src/main/java/com/example/agenticjobscheduler/messaging/EnvelopeValidator.java)
  already enforces canonical main/retry envelopes, bounded bytes, strict JSON,
  duplicate-key rejection, key identity, registered job types, and
  `remainingAttempts`.
- [JobRecordEncoder](/home/spakai/agentic-job-scheduler/src/main/java/com/example/agenticjobscheduler/runtime/JobRecordEncoder.java)
  already serializes canonical main keys and values.
- [WorkerMain](/home/spakai/agentic-job-scheduler/src/main/java/com/example/agenticjobscheduler/runtime/WorkerMain.java)
  and the Docker image currently launch the worker directly. The admission role
  needs an independent `HttpApiMain` entry point.
- [TopicProvisioner](/home/spakai/agentic-job-scheduler/src/main/java/com/example/agenticjobscheduler/runtime/TopicProvisioner.java)
  creates all application topics. The HTTP role must not call it; its readiness
  check must only describe existing topics and fail without mutation.
- [WorkerConfig](/home/spakai/agentic-job-scheduler/src/main/java/com/example/agenticjobscheduler/runtime/WorkerConfig.java)
  is worker-specific. Do not overload it with HTTP concerns.
- The project has Vert.x core and Kafka support but no Vert.x Web or metrics
  dependency.

### Constitution and architecture gate

No `constitution.md`, knowledge graph, project topology, or upstream feature
design artifacts exist in this repository. This plan therefore derives
constraints directly from Spec 002, Spec 006, the current source, and repository
instructions. There are no `[NEEDS CLARIFICATION]` markers in Spec 006.

The following decision must be closed before observability implementation:

- **D1: metrics format/export** — select an existing Vert.x-compatible metrics
  backend and exposition mechanism. The default plan is counters/histograms
  with bounded labels exposed in Prometheus text format over `/metrics`, but
  dependency and operational compatibility must be confirmed before Phase 5.

Authentication, authorization, and TLS remain explicitly outside this task;
the implementation must not expose the Compose service as a production security
claim.

## Applied guidelines

- Preserve the Spec 002 Kafka-only backlog: no database, state topic, inbox, or
  durable idempotency store.
- Commit behavior belongs to workers only. HTTP admission ends at Kafka
  acknowledgment and must not consume worker topics or offsets.
- Treat `503` after an attempted send as an ambiguous outcome, not proof of
  absence.
- Keep event loops nonblocking. Use asynchronous Vert.x HTTP/Kafka futures.
- Bound request bytes, active requests, producer buffers, and all deadlines.
- Reuse `JobRecordEncoder` and `EnvelopeValidator` as the final wire boundary.
- Never label duplicate-tolerant admission as exactly-once.
- Tests must exercise observable Kafka and HTTP behavior, including ambiguous
  publication and overload, not only private mappers or counters.

## Implementation steps

### Step 1: Establish a clean, isolated baseline

**Plan 1.1 — Branch and acceptance baseline.** Create a feature branch from
the selected base commit, record current `HEAD`, and verify the unrelated demo
changes are not included in implementation evidence.

**Plan 1.2 — Confirm the clean build.** Run the unit suite before edits and
record the result separately from later admission tests.

Tasks:

- [ ] T001 [Plan:1.1] Record base commit/branch and create the implementation
  branch without moving or reverting unrelated user edits.
- [ ] T002 [Plan:1.2] Run `mvn -B -ntp test`; record the exact result and baseline
  evidence in this task record.
- [ ] T003 [P] [Plan:1.2] Inspect existing Vert.x and Kafka dependency versions
  in `pom.xml`; select a Vert.x Web version from the existing BOM/version
  strategy rather than adding an independent framework.

### Step 2: Add the strict HTTP request-to-envelope boundary

**Plan 2.1 — Admission configuration.** Define immutable, environment-backed
HTTP configuration with positive numeric bounds, a nonempty job-type registry,
and timeout relationships validated at construction.

**Plan 2.2 — HTTP request decoding.** Strictly decode the HTTP-only request shape.
It is different from the Kafka envelope because `schemaVersion` is server-owned
and both identifiers may be generated.

**Plan 2.3 — Canonical mapping.** Construct `JobEnvelope`, serialize it through
`JobRecordEncoder.main`, and revalidate key/value using `EnvelopeValidator`
before any Kafka publish path receives it.

Tasks:

- [ ] T004 [Plan:2.1] Add `src/main/java/com/example/agenticjobscheduler/admission/HttpApiConfig.java`
  with validated `HTTP_PORT`, `HTTP_MAX_BODY_BYTES`, `HTTP_MAX_IN_FLIGHT`,
  `HTTP_ACK_DEADLINE_MILLIS`, `HTTP_GRACEFUL_SHUTDOWN_MILLIS`,
  `KAFKA_BOOTSTRAP_SERVERS`, `KAFKA_TOPIC_PARTITIONS`,
  `KAFKA_DELIVERY_TIMEOUT_MILLIS`, and `REGISTERED_JOB_TYPES`.
- [ ] T005 [P] [Plan:2.1] Add `src/test/java/com/example/agenticjobscheduler/admission/HttpApiConfigTest.java`
  for defaults, accepted overrides, missing values, contradictory deadlines,
  zero/negative bounds, and invalid job registries.
- [ ] T006 [Plan:2.2] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionRequest.java`
  as an internal decoded value carrying optional identity fields, required
  job type/payload, and optional attempt budget.
- [ ] T007 [Plan:2.2] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionRequestDecoder.java`
  with bounded raw UTF-8 decoding, strict duplicate/trailing-content checks,
  exact field allowlist, canonical UUID pairing, and object payload validation.
- [ ] T008 [Plan:2.3] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionEnvelopeMapper.java`
  to generate paired UUIDs only when omitted, default attempts to 3, build a
  `JobEnvelope`, and produce a typed `AdmissionEnvelope` containing key/value
  bytes plus normalized identities.
- [ ] T009 [P] [Plan:2.2,2.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionRequestDecoderTest.java`
  covering malformed JSON/UTF-8, duplicates, trailing content, unknown fields,
  content typing-independent request decoding, paired IDs, payload type, and
  attempt bounds.
- [ ] T010 [P] [Plan:2.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionEnvelopeMapperTest.java`
  proving generated ID pairs, supplied identity preservation, default attempts,
  canonical `JobRecordEncoder` output, and `EnvelopeValidator` acceptance.
- [ ] T011 [Plan:2.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionEnvelopeMapperNegativeTest.java`
  proving invalid and oversized mapped records never cross the publication
  boundary.

### Step 3: Build bounded Kafka publication

**Plan 3.1 — Publisher boundary.** Introduce a narrow asynchronous publisher
interface so HTTP routes can be tested without a broker and production uses a
single bounded idempotent Kafka producer.

**Plan 3.2 — Ambiguous outcomes.** Map successful acknowledgment separately
from conclusive local rejection and unresolved/conclusive producer failures.
Do not manufacture an application retry or status claim after send ambiguity.

**Plan 3.3 — Admission capacity.** Bound concurrent publication requests before
encoding or sending work when the configured in-flight capacity is exhausted.
Release a permit only when the underlying future settles or the publication
deadline is conclusively handled.

Tasks:

- [ ] T012 [Plan:3.1] Add `src/main/java/com/example/agenticjobscheduler/admission/JobPublisher.java`
  with `publish(AdmissionEnvelope)` and explicit accepted/failure/ambiguous
  result categories, expressed without leaking Kafka records into HTTP code.
- [ ] T013 [Plan:3.1] Add `src/main/java/com/example/agenticjobscheduler/admission/KafkaJobPublisher.java`
  using one Vert.x `KafkaProducer`, byte-array serializers, `acks=all`,
  idempotence, finite delivery timeout, and `jobs.main.v1` only.
- [ ] T014 [Plan:3.2] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionOutcome.java`
  and sanitize producer failures before exposing them to route/observation
  layers.
- [ ] T015 [Plan:3.3] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionLimiter.java`
  with an atomic bounded permit reservation, no request queue, and deterministic
  release semantics.
- [ ] T016 [P] [Plan:3.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionLimiterTest.java`
  covering saturation, concurrent reservation races, release, double-release,
  and close/reject behavior.
- [ ] T017 [P] [Plan:3.2] Add `src/test/java/com/example/agenticjobscheduler/admission/JobPublisherContractTest.java`
  proving accepted, conclusive failure, ambiguous timeout, and no second
  application-level send.
- [ ] T018 [Plan:3.1,3.2] Add the focused broker test
  `src/test/java/com/example/agenticjobscheduler/admission/KafkaJobPublisherIT.java`
  to assert exact topic/key/value and producer acknowledgment (API01, API02,
  API04, API10, API11).

### Step 4: Expose the HTTP contract and independent role

**Plan 4.1 — Routes and error mapping.** Add `POST /v1/jobs`, deterministic
status/error mapping, body/media limits, and JSON responses using a thin route
layer.

**Plan 4.2 — API lifecycle.** Start and close the HTTP server and producer with a
bounded shutdown that stops intake before draining in-flight work.

**Plan 4.3 — Launchable role.** Add `HttpApiMain` without changing the worker
entry point or worker role parser.

Tasks:

- [ ] T019 [Plan:4.1] Add Vert.x Web at the same Vert.x version in `pom.xml`.
- [ ] T020 [Plan:4.1] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionException.java`
  and `AdmissionError.java` with stable HTTP/code/message mappings for
  malformed input, unsupported content, overload, Kafka unavailable, and
  unexpected internal failures.
- [ ] T021 [Plan:4.1] Add `src/main/java/com/example/agenticjobscheduler/admission/HttpJobApi.java`
  for `POST /v1/jobs`; enforce effective media type and body limits before
  publication, reserve admission capacity, decode/map, await the bounded
  publication result, and always release the permit.
- [ ] T022 [Plan:4.2] Add `src/main/java/com/example/agenticjobscheduler/admission/HttpApiRuntime.java`
  to own HTTP server, publisher, health/readiness handlers, admission stop,
  bounded drain, and resource closure.
- [ ] T023 [Plan:4.3] Add `src/main/java/com/example/agenticjobscheduler/admission/HttpApiMain.java`
  with an independently selectable main class, startup failure cleanup, and
  shutdown hook.
- [ ] T024 [P] [Plan:4.1] Add `src/test/java/com/example/agenticjobscheduler/admission/HttpJobApiContractTest.java`
  using a controlled publisher to assert request/response JSON, every 4xx,
  `429`, `503`, and `202` contract (API03, API05-API10).
- [ ] T025 [P] [Plan:4.2] Add `src/test/java/com/example/agenticjobscheduler/admission/HttpApiRuntimeLifecycleTest.java`
  for startup cleanup, stop-intake ordering, drain completion, bounded stalled
  futures, and double-close idempotence (API12).

### Step 5: Add readiness and bounded observation

**Plan 5.1 — Read-only Kafka readiness.** Verify connectivity, topic existence,
and the expected main partition count through `AdminClient`. Never create,
expand, or alter topics from the API role.

**Plan 5.2 — Health endpoints.** Expose liveness based on runtime health and
readiness based on configuration/topic state and recent producer failure state.

**Plan 5.3 — Metrics and sanitized logs.** Resolve D1, then instrument outcome
counts, active requests, bytes, and latencies with only bounded labels.

Tasks:

- [ ] T026 [Plan:5.1] Add `src/main/java/com/example/agenticjobscheduler/admission/HttpApiReadiness.java`
  for asynchronous `AdminClient`-based main-topic description and exact
  partition-count validation, with no topic mutation.
- [ ] T027 [P] [Plan:5.1] Add `src/test/java/com/example/agenticjobscheduler/admission/HttpApiReadinessTest.java`
  using an injectable metadata boundary for missing topic, broker outage,
  partition mismatch, and valid readiness results.
- [ ] T028 [Plan:5.2] Add `/health/live` and `/health/ready` to `HttpJobApi`
  or a dedicated route factory; keep status routes distinct from job submission.
- [ ] T029 [Plan:5.3] Resolve D1 in this task record, including dependency,
  exposition path, label set, histogram strategy, and failure behavior.
- [ ] T030 [Plan:5.3] Add `src/main/java/com/example/agenticjobscheduler/admission/AdmissionMetrics.java`
  and instrument request outcomes, in-flight/queued capacity, bytes, and
  validation/acknowledgment duration without high-cardinality job labels.
- [ ] T031 [P] [Plan:5.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionMetricsTest.java`
  to prove labels are bounded and lifecycle/outcome counters transition as
  documented.
- [ ] T032 [Plan:5.3] Add `src/test/java/com/example/agenticjobscheduler/admission/AdmissionLogSanitizationTest.java`
  using captured test logs to prove payloads, raw request bodies, and credentials
  are absent (API15).
- [ ] T033 [Plan:5.1] Add broker-backed readiness cases to
  `src/test/java/com/example/agenticjobscheduler/admission/HttpApiReadinessKafkaIT.java`
  for missing and mismatched topics and a negative assertion that no topic was
  created or expanded (API13).

### Step 6: Wire Compose and validate the full boundary

**Plan 6.1 — Container role.** Preserve the default worker image entry point and
override it for a separately deployed `http-api` Compose service.

**Plan 6.2 — End-to-end boundary.** Submit through HTTP, inspect the Kafka record,
and allow an existing main worker to process it. This validates transport
composition without changing worker acceptance semantics.

**Plan 6.3 — Operational documentation.** Document the API, ambiguity/retry
contract, local security limitation, and exact validation commands.

Tasks:

- [ ] T034 [Plan:6.1] Keep the worker default in `Dockerfile`; ensure the
  `http-api` Compose service overrides `entrypoint` to `HttpApiMain`.
- [ ] T035 [Plan:6.1] Add `http-api` to `compose.yaml` with explicit environment
  defaults, Kafka readiness dependency, bounded resource configuration, and a
  localhost-only published port for the local demo.
- [ ] T036 [Plan:6.2] Add `scripts/smoke-http-admission.sh` or an equivalent
  JUnit-based Compose acceptance helper to submit a job, inspect Kafka, and
  verify worker processing without introducing a state store (API16).
- [ ] T037 [Plan:6.2] Add
  `src/test/java/com/example/agenticjobscheduler/admission/HttpToWorkerKafkaIT.java`
  or extend the focused broker test to prove an HTTP-produced record follows
  existing worker validation, per-topic identity gating, safe commits, and replay
  behavior (API14).
- [ ] T038 [P] [Plan:6.3] Update `README.md` with the `POST /v1/jobs` contract,
  broker-acknowledgment meaning, stable-ID retry recommendation, ambiguous
  `503`, health endpoints, and Compose command.
- [ ] T039 [P] [Plan:6.3] Update `arch.md` to remove the implemented-increment
  claim that no HTTP API exists only after the role and tests land; add the
  admission boundary and explicitly preserve non-guarantees.
- [ ] T040 [Plan:6.2,6.3] Run `mvn -B -ntp test`, `mvn -B -ntp package`, and, with
  Docker available, focused/full `mvn -B -ntp verify`; record each separately.
- [ ] T041 [Plan:6.2] Run the Compose HTTP smoke check, capture exact commands,
  response, Kafka evidence, worker evidence, and any environment skips.
- [ ] T042 [Plan:6.3] Review the final diff against Spec 006 and close every
  acceptance row with test or explicit remaining-gap evidence.

## Final phase: Cross-cutting review

- [ ] T043 [Plan:6.3] Check errors and logs contain no payloads, credentials, raw
  input, stack traces in client responses, or high-cardinality labels.
- [ ] T044 [Plan:6.3] Check the HTTP role never calls topic creation, consumes a
  worker topic, opens a worker consumer, or depends on PostgreSQL/JDBC/Flyway.
- [ ] T045 [Plan:6.3] Confirm 256 KiB request/envelope boundaries, 1,000
  in-flight defaults, 10-second acknowledgment deadline, 9-second producer
  delivery timeout, and 30-second shutdown deadline remain mutually consistent.
- [ ] T046 [Plan:6.3] Confirm docs distinguish `202`, broker outage, ambiguous
  send, worker replay, and duplicate HTTP admission.
- [ ] T047 [Plan:6.3] Run `git diff --check` and inspect the staged/working-tree
  diff for unrelated demo or repository changes.

## Dependency order

```text
Phase 1 baseline
    -> Phase 2 configuration + request/envelope boundary
    -> Phase 3 bounded publisher
    -> Phase 4 HTTP route/lifecycle
    -> Phase 5 readiness/observation
    -> Phase 6 Compose/end-to-end/docs
```

T019 should precede route code, not the request/envelope layer. T013 and T015 are
prerequisites for the real `HttpApiRuntime`. T026 may be developed in parallel
with the route after the configuration contract is stable, but API readiness
must not cause topic mutation. Documentation tasks must follow verified runtime
behavior.

## Expected project structure

```text
src/main/java/com/example/agenticjobscheduler/
  admission/
    AdmissionEnvelope.java
    AdmissionError.java
    AdmissionException.java
    AdmissionLimiter.java
    AdmissionMetrics.java
    AdmissionOutcome.java
    AdmissionRequest.java
    AdmissionRequestDecoder.java
    AdmissionEnvelopeMapper.java
    HttpApiConfig.java
    HttpApiMain.java
    HttpApiReadiness.java
    HttpApiRuntime.java
    HttpJobApi.java
    JobPublisher.java
    KafkaJobPublisher.java
  messaging/
    (unchanged canonical Kafka validators/models)
  runtime/
    (worker runtime unchanged; encoder reused)
src/test/java/com/example/agenticjobscheduler/
  admission/
    focused unit/contract tests
    KafkaJobPublisherIT.java
    HttpApiReadinessKafkaIT.java
    HttpToWorkerKafkaIT.java
```

## Requirement mapping

| Requirement | Description | Plan items | Planned implementation evidence |
| --- | --- | --- | --- |
| API01 | Broker-acknowledged valid canonical submission | 2.3, 3.1, 4.1 | `KafkaJobPublisherIT`, `HttpJobApiContractTest`, `HttpJobApi` |
| API02 | Generate and return a paired identity | 2.3, 4.1 | `AdmissionEnvelopeMapperTest`, HTTP response assertions |
| API03 | Reject a partially supplied identity pair | 2.2, 4.1 | `AdmissionRequestDecoderTest`, HTTP contract test |
| API04 | Repeated stable identity may duplicate physically | 3.2, 4.1 | `KafkaJobPublisherIT`, no-deduplication tests |
| API05 | Omitted identities create a new pair per request | 2.3, 4.1 | mapper and HTTP contract tests |
| API06 | Reject malformed/unknown/wrong-type input | 2.2, 4.1 | decoder and HTTP negative cases |
| API07 | Enforce raw and encoded size limits | 2.2, 2.3, 4.1 | boundary tests and `HttpJobApi` |
| API08 | Correct media/body error responses | 4.1 | `HttpJobApiContractTest` |
| API09 | Prompt bounded overload rejection | 3.3, 4.1 | `AdmissionLimiterTest`, saturated-route test |
| API10 | Map refusal/timeout without false rejection | 3.2, 4.1 | controlled failure contract and broker test |
| API11 | No application retry of an ambiguous send | 3.1, 3.2 | `JobPublisherContractTest`, producer integration test |
| API12 | Bounded shutdown and producer close | 4.2 | `HttpApiRuntimeLifecycleTest` |
| API13 | Read-only, exact readiness checks | 5.1 | `HttpApiReadinessTest` and Kafka IT |
| API14 | Preserve existing worker invariants | 2.3, 6.2 | `HttpToWorkerKafkaIT` and existing worker tests |
| API15 | Sanitized logs, responses, and metrics | 4.1, 5.3 | sanitization and bounded-label tests |
| API16 | Compose HTTP-to-worker path without state store | 6.1, 6.2 | Compose smoke evidence and service inspection |

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions:
  standard repository read/write tools, Maven, Git, Docker/Compose, and
  Testcontainers for focused broker tests.
- Read/write operations needed: create admission package/tests and documentation;
  surgical edits to Maven/build and Compose configuration.
- Environment constraints and recovery path: broker-backed checks require
  Docker. If unavailable, record the exact failed/unavailable result and do not
  call integration coverage passing. The unit/contract suite must still run.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  branch and commit evidence will be recorded at execution time; Docker
  availability and any repository protection controls remain unverified until
  the implementation run.

## Evaluation plan

| Check | Commit | Command or workflow | Expected result | Evidence |
| --- | --- | --- | --- | --- |
| Baseline unit suite | to record | `mvn -B -ntp test` | existing suite passes before edits | task record |
| Admission unit/contract suite | to record | `mvn -B -ntp -Dtest='...admission...' test` | all focused admission cases pass | Surefire report |
| HTTP/Kafka publication | to record | `mvn -B -ntp -Dit.test=KafkaJobPublisherIT verify` | exact acknowledged main record; failure boundaries pass | Failsafe report |
| Readiness and worker boundary | to record | focused `mvn ... verify` selectors | no topic mutation; existing worker invariants preserved | Failsafe report |
| Full unit/build gate | to record | `mvn -B -ntp test` and `mvn -B -ntp package` | 0 failures and application artifacts build | console/reports |
| Full broker gate | to record | `mvn -B -ntp verify` | admission and existing Kafka tests pass when Docker exists | Failsafe report |
| Compose smoke | to record | documented Compose/HTTP command | request accepted and worker processes it | command log/Kafka evidence |
| Diff hygiene | to record | `git diff --check` and focused diff review | no whitespace/unrelated edits | task record |

At implementation time, every row must record passing, failed, skipped, planned,
or unavailable separately. A unit-only success is not evidence for a
broker-backed acceptance criterion.

## Failure analysis and correction

- Expected failure classes: requirement mismatch, request/protocol mapping
  defect, timeout/ambiguity bug, admission permit leak, Kafka startup failure,
  nonblocking lifecycle error, stale worker assumptions, or unavailable Docker.
- On failure, reproduce with the smallest focused test, classify it, cite the
  exact assertion/output, fix the root cause, and rerun that selector before any
  broader suite.
- Never convert an ambiguous publication into `accepted`, retry it at the HTTP
  layer, or fabricate a status response to make tests pass.

## Handoff and review

- Next concrete step: establish Phase 1 on a feature branch and record the base
  test result before editing.
- Open decision: D1 metrics format/export.
- Review focus: whether every `202` follows broker acknowledgment, whether all
  permits release on every route branch, whether readiness mutates nothing, and
  whether the plan preserves worker invariants and avoids Spec 001's state
  model.
- Final handoff must include latest commit, actual commands, evidence locations,
  remaining gaps, and exact unrelated working-tree changes.

## Learning check

- What invariant does the task establish? HTTP success means broker acceptance
  of a valid canonical main-topic record, not execution completion.
- What failure invalidates the claim? A route returning `202` before acknowledgment,
  an ambiguous send causing an extra application append, or a local overload path
  growing unbounded.
- Which artifacts prove it? Contract tests, controlled timeout tests, broker-backed
  key/value assertions, lifecycle tests, safe-diff review, and actual command
  evidence.
