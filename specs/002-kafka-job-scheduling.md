# Spec 002: Immediate Kafka job processing with slower retries

Status: **Draft — simplified architecture agreed; L01 validation implemented for review, worker runtime pending**

Project: `agentic-job-scheduler`
Date: 2026-10-08
Supersedes: the PostgreSQL architecture in [Spec 001](001-job-lifecycle.md)
and the earlier delayed-scheduling design of Spec 002.

## 1. Scope and decisions

Clients publish jobs to Kafka. Vert.x consumes them as soon as records, worker
capacity, and the adaptive limiter allow. A failed first attempt goes to a retry
topic; separate retry pods consume at a lower rate and concurrency. Permanent or
exhausted failures go to a dead-letter queue (DLQ).

There are no scheduled times, delayed jobs, timer indexes, jobs.state topic,
RocksDB store, or state restoration service. Kafka topic records and consumer
offsets provide the durable work backlog. PostgreSQL, JDBC, Flyway, and EDR are
outside this increment. Spec 001 remains historical, not a prerequisite.

Same-job execution is sequential within each topic under its current owner.
Main and retry consumers are independent: a new main-topic execution can overlap
an older retry for the same jobId. This relaxed ordering is an explicit decision
to keep Spec 002 simple. Cross-topic exclusion and durable deduplication are
future work. Handlers must tolerate replay and concurrent duplicate requests.

Retain Java 21, Vert.x, Maven, JUnit 5, Testcontainers, and Docker Compose. Deliver
one application with main/retry worker roles, a sample producer, and a controlled
demo handler. Pin compatible versions during implementation. No HTTP API,
production business handler, or Kubernetes deployment is required.

## 2. Flow and topic ownership

```text
Client -> jobs.main.v1 -> main Vert.x workers + adaptive limiter
                               | success: complete record
                               | retryable failure
                               v
                         jobs.retry.v1 -> separate slower retry workers
                               ^                 | success: complete record
                               |_________________| retryable, attempts remain
                                                 |
                               permanent/exhausted failure -> jobs.dlq.v1
```

Permanent failure on the main topic also goes directly to DLQ. Retry workers
execute jobs directly; they do not send them back to the main topic.

Use consumer group `job-main-v1` for main workers and `job-retry-v1` for retry
workers. A consumer belongs to one verticle and may own multiple partitions.
Within that verticle, maintain FIFO queues by jobId and run only one handler per
jobId at a time. Different job IDs, including those in one partition, may run
concurrently. Worker threads can vary; logical ownership stays with the verticle.
Serialize queue, completion, and offset mutations on its context. Never block
the event loop or share a native Kafka consumer across worker threads.

## 3. Topic configuration

| Topic | Key | Default partitions | Retention |
| --- | --- | --- | --- |
| jobs.main.v1 | canonical jobId | 12 | delete, 7 days |
| jobs.retry.v1 | same jobId | 12 | delete, 7 days |
| jobs.dlq.v1 | jobId, or source identity for invalid input | 3 | delete, 30 days |

Provision topics explicitly. Producers must use consistent UTF-8 key serialization
and Kafka's Java default keyed partition calculation; validate key/partition
agreement. Keep partition counts fixed for this increment. Matching partition
numbers across topics do not imply shared ownership or ordering.

Use idempotent producers with `acks=all` and finite delivery timeouts. Local
Compose uses one KRaft broker, replication factor 1, min ISR 1, and a persistent
Kafka volume. Document a deployment baseline of three brokers, replication factor
3, min ISR 2. Use `enable.auto.commit=false` and `auto.offset.reset=earliest` for
new groups. Fail visibly on an established group's out-of-range offsets instead
of silently skipping lost work. Consumers may use `read_committed` to support
transactional clients, but this increment does not require Kafka transactions.

Retention can expire unfinished records during a sufficiently long outage or
backlog. Monitor oldest-unprocessed age and lag before retention is reached.
Kafka offsets do not protect records against retention deletion.

## 4. Job envelope

Clients write UTF-8 JSON to jobs.main.v1, keyed by jobId:

```json
{
  "schemaVersion": 1,
  "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
  "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
  "jobType": "demo.echo",
  "payload": { "message": "hello" },
  "maxAttempts": 3
}
```

Require UUID identifiers, version 1, a registered jobType of 1–128 characters,
and an object payload. maxAttempts is optional, defaults to 3, and accepts 1–100.
Reject unknown fields, including scheduledAt, notBefore, and client-supplied
attempt values. Limit client records to 256 KiB. Read raw bytes before decoding
so malformed JSON can be routed to DLQ rather than trapping deserialization.

jobId defines serialization within a topic. executionId identifies a logical
invocation. A client retry reuses both identifiers and identical content; a new
invocation uses a new executionId. The service passes `(jobId, executionId)` as
the stable handler idempotency key, across attempts and deliveries. It does not
store a durable deduplication table or detect conflicting payloads across restarts.
The client is responsible for not reusing identities with changed content.

Internal retry records wrap the original normalized job with `attempt` (the next
attempt to execute), a stable handoffId derived from source topic/partition/offset
and next attempt, original source identity, and a bounded error code. Validate
retry attempt in 2..maxAttempts. Preserve the original job and execution IDs.
Do not recursively nest retry envelopes. Reserve at least 512 KiB for internal
records and align producer/broker/consumer limits accordingly.

L01 makes the retry wire fields concrete: top-level `schemaVersion`, `job`,
`attempt`, `handoffId`, `originalSource`, `failedSource`, and `errorCode` are
required, with no unknown fields. `job` is the normalized main envelope, including
explicit maxAttempts. Each source has `topic` (Kafka topic name), `partition`
(nonnegative 32-bit integer), and `offset` (nonnegative 64-bit integer).
originalSource stays unchanged; failedSource identifies the record that just
failed. They must match for attempt 2. handoffId is lowercase SHA-256 hex over
UTF-8 `failedSource.topic + "\n" + partition + "\n" + offset + "\n" + attempt`,
using newline separators and no final newline. errorCode matches
`[A-Z][A-Z0-9_]{0,63}`. Later integration checks configured topic/partition routing.

All envelope UUIDs use canonical lowercase form. Validation rejects duplicate
JSON properties, trailing content, malformed UTF-8, and type coercions. Parsing
is bounded to 100 main nesting levels (101 for the retry wrapper) and 1,000
characters per numeric token; size
limits apply to raw UTF-8 bytes. Error diagnostics contain codes, not raw input.

## 5. Immediate execution and completion

A valid fetched record is runnable as soon as it is its per-job queue head and
has an execution permit. There is no eligibility timestamp. Waiting for a permit
or an earlier same-topic execution is backpressure, not delayed scheduling.

Keep the jobId gate until the handler succeeds or a retry/DLQ handoff is
acknowledged. On release, its next record in that topic can start. A handoff to
retry does not keep the main-topic gate locked. A1 may therefore retry while A2
runs on the main topic. Retry records follow retry-topic append order; repeated
handoffs do not promise original main-topic order across attempts.

On success mark the source record complete in memory. On retryable failure,
publish its retry envelope and mark complete only after broker acknowledgment.
On permanent/exhausted failure, do the same with DLQ. If the handoff publication
fails, keep the source incomplete and retry the send with the same envelope;
do not rerun the handler simply because its handoff failed.

No durable RUNNING/SUCCESS status table is introduced. Use structured logs and
metrics for observation. Producer acknowledgment of a submitted job means Kafka
accepted it, not that a handler has completed it.

## 6. Offset commits and delivery guarantees

Maintain a bounded completion tracker per partition. Commit only the next offset
after the completed prefix of records delivered in that partition; never jump
past a fetched but unfinished record. Serialize commit requests to avoid an older
asynchronous commit overwriting a newer position. Account for gaps in Kafka offset
numbers; a missing integer is not necessarily an unfinished record.

Example: offsets 10, 11, and 12 belong to different job IDs. If 11 and 12 finish
first, they remain tracked while 10 runs. Once 10 completes, the next offset can
advance to 13. Other partitions can commit independently.

Use acknowledgment-before-commit, not a consume/produce transaction, for the
retry and DLQ handoffs. A crash after publication but before source commit can
produce duplicates. Likewise, a successful handler can run again after a crash
before offset commit. Delivery is at least once; there is no exactly-once promise.
Kafka documents [manual offsets and replay behavior](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).

Restart reads from committed offsets. In-memory queues and completion trackers
are discarded and rebuilt from replay. There is no state-topic recovery step.
Handler idempotency must protect external effects, including the case where a
later same-job execution finished before an earlier partition offset committed.

## 7. Main adaptive limiter and backpressure

Use a shared process-wide main-worker concurrency limit: initial 16, minimum 1,
maximum 128. Each running handler needs one permit and its jobId gate. This is a
per-pod limit, not a cluster-wide budget. A permit is released when the handler
has actually stopped; keep the jobId gate while a required handoff is pending.

Evaluate 5-second windows with at least 20 completed attempts. Halve the limit
(round down, clamp to minimum) on p95 handler latency above a configurable
1-second target, at least 10% transient failures, throttling, or timeouts.
Otherwise add one permit when backlog exists and p95 is below target, up to the
maximum. Insufficient samples hold the limit; an explicit throttle signal can
trigger one decrease per window. Exclude validation/permanent errors. Measure
handler latency separately from queue wait using monotonic time.

A decrease never cancels existing work; it stops new starts until usage allows.
Round-robin runnable job queues across assigned partitions. Bound the total
retained processing window, including fetched, running, handoff-pending, and
completed-but-uncommitted records, to 1,000 records or 16 MiB per process.
Pause intake at either watermark and resume below 50%. Account for one bounded
fetch overshoot and Kafka client buffers; configure fetch limits accordingly.
Continue processing retained records, polling, and heartbeats while intake is
paused. An early slow offset must not cause unbounded completed-record tracking.

Use [Vert.x partition pause/resume](https://vertx.io/docs/apidocs/io/vertx/kafka/client/consumer/KafkaConsumer.html)
and verify behavior against the selected client version. Bound executor queues
and producer buffers as well as application queues.

## 8. Slower retries and DLQ

The main worker makes attempt 1 only. A retryable failure with maxAttempts > 1
publishes attempt 2 to jobs.retry.v1. Retry workers consume immediately when their
own capacity and rate budget allow. They default to concurrency 2 and a token
bucket of 1 handler start per second per pod, burst 1. Both limits are configurable;
there is no per-record delay, exponential backoff, or notBefore timestamp.

A retry failure with remaining attempts publishes attempt N+1 to the same retry
topic, then completes its source record after acknowledgment. A retry success
completes the source. A permanent failure or failure at maxAttempts goes to DLQ.
Only handler failures advance the attempt number; Kafka send failures do not.

The attempt limit bounds a logical retry chain, not physical invocations under
crash/replay: the same numbered attempt may execute more than once. Duplicate
handoffs can fork duplicate chains. Preserve identities and require duplicate-safe
handlers. Rate limits are per pod; scaling retry pods increases aggregate retry
traffic, which must be documented and observed. Slow retry throughput deliberately
allows backlog to accumulate, subject to topic retention.

DLQ records include a stable handoffId, identity where valid, logical attempt,
sanitized reason, timestamp, original source and failing source coordinates,
and the original valid job. Invalid records use a bounded byte sample and hash
instead of copying arbitrary input. Never log payloads or credentials. Automated
DLQ redrive is deferred; any manual redrive needs an explicit identity decision.

## 9. Rebalance, shutdown, and handler contract

On revocation stop dispatch for the revoked partitions, invalidate their local
assignment generation, cancel/drain work within a bounded deadline, and discard
late callbacks from that generation. Commit only completed prefixes while still
allowed to commit for the assignment. Do not block the poll thread during drain.
New owners replay uncommitted records. On shutdown, pause intake, drain up to
30 seconds, commit safe prefixes, and close clients.

The simple architecture guarantees per-job exclusion within a stable topic
owner. It cannot prevent an old worker's external operation from continuing after
forced ownership loss. It also deliberately permits main/retry overlap. Durable
fencing and global per-job exclusion are deferred. Do not claim idempotency alone
prevents concurrent computation.

Only replay-safe, concurrency-safe handlers are supported in this increment;
the demo handler must be cancellable and have no irreversible external effects.
Production integrations must supply appropriate target-side idempotency rather
than rely on an in-memory cache as proof of safety.

Default handler timeout is 30 seconds. Cancellation must be confirmed before
releasing a local gate or retrying a timed-out handler. If cancellation cannot be
confirmed, keep the gate, leave the record incomplete, and report the stuck task;
do not manufacture completion. Forced pod loss can still cause replay elsewhere.

On broker/commit failure, retain bounded results and pause intake as necessary.
Do not drop records to make space. Readiness requires valid configuration, known
topics, and Kafka connectivity. Document recovery and unsupported handler risks.

## 10. Artifacts and observation

Deliver Maven build, Java sources/tests, Dockerfile, Compose, explicit topic
provisioning, sample producer, demo handler, and README. Compose demonstrates
separate main and retry services, corresponding to separately scalable pods.
Organize code around messaging, execution/queues, limiter, and configuration.

Report main/retry lag and oldest record age, active handlers, queue/window size,
current main limit, retry start rate, handler latency, handoff failures, DLQ count,
commit failures, and rebalances. Logs identify jobId, executionId, topic, partition,
offset, logical attempt, and outcome. Avoid high-cardinality metric labels.

README demonstrates immediate work, same-topic ordering, cross-topic overlap,
slower retries, DLQ, backpressure, restart/replay, and Kafka volume persistence.
There is no PostgreSQL service or local persistent scheduling volume.

## 11. Acceptance tests

These B01–B20 definitions replace the earlier scheduling/state-store cases.
Use real Kafka via Testcontainers, controlled handler barriers, bounded waits,
and injected failure boundaries. Planned commands are `mvn test` and `mvn verify`;
integration tests require Docker and must fail clearly if unavailable.

| ID | Scenario and assertion |
| --- | --- |
| B01 | Valid main envelope is accepted; invalid fields/identity are rejected |
| B02 | Available records start when permits allow, with no time eligibility check |
| B03 | Same jobId is sequential within each topic under stable ownership |
| B04 | Different job IDs run concurrently, including within one partition |
| B05 | Offset commit never passes an incomplete delivered record |
| B06 | First retryable failure hands attempt 2 to retry before source completion |
| B07 | Retry pods obey concurrency/rate limits and increment logical attempt on failure |
| B08 | Permanent/exhausted failures reach DLQ before source completion |
| B09 | Crash after handler success or handoff acknowledgment demonstrates permitted replay |
| B10 | Repeated delivery preserves handler identity; no service deduplication claim is made |
| B11 | Revocation stops new dispatch and ignores stale callbacks/commits |
| B12 | Controlled main/retry overlap for the same jobId is allowed and documented |
| B13 | Main adaptive limit increases/decreases according to measured signals and bounds |
| B14 | Window watermarks bound retained work and pause/resume while polling continues |
| B15 | Out-of-order completions, offset gaps, and independent partitions commit safely |
| B16 | Failed retry/DLQ publication leaves source incomplete without rerunning its handler |
| B17 | Malformed/oversized/wrong-partition input produces bounded DLQ records |
| B18 | Broker outage and restart preserve replayable work within retention |
| B19 | Unconfirmed cancellation keeps the local gate and record incomplete |
| B20 | Compose demonstrates main/retry workers, DLQ, and replay without database/state store |

Unit tests cover validation, queue exclusion, completion frontiers, limiter windows,
retry rate/attempt calculations, and envelope bounds. Integration tests establish
Kafka handoff and replay behavior. L01 has unit validation tests; Kafka handoff,
DLQ publication, partition routing, and worker-runtime tests remain pending.

## 12. GH-600 learning workflow

This project is a practical exercise in supervising coding agents. The Kafka
service supplies concrete engineering tasks; its workers, retries, and consumer
offsets do not themselves demonstrate AI-agent coordination or memory.
Use the [learning guide](../docs/gh-600-learning-guide.md) alongside implementation.
The mapping is based on the [official GH-600 study guide](https://learn.microsoft.com/en-us/credentials/certifications/resources/study-guides/gh-600),
checked on 2026-10-08. It is not a claim of complete exam preparation.

For each small implementation task, retain the requirement, bounded plan,
acceptance IDs, diff, actual test/scan results, review findings, and final outcome.
Start with the [task record template](../docs/templates/task-record.md). Proposed
checks must remain distinguishable from executed checks. Use branches and PRs
for implementation exercises; GitHub checks and review controls must be configured
and verified before claiming they enforce a gate.

| Exercise | Implementation focus | Learning evidence |
| --- | --- | --- |
| L01 | Envelope validation; B01/B17 | Reviewed plan, scoped patch, positive/negative cases |
| L02 | Completion and offset commits; B05/B09 | Tool/permission inventory, crash/replay test |
| L03 | Immediate processing and per-topic ordering; B02–B04 | Durable handoff, fresh-session resumption, concurrency trace |
| L04 | Retry/DLQ and limiter; B06–B08/B13 | Deliberately failing case, root cause, revised instruction, rerun |
| L05 | Replay and rebalances; B09–B12 | Isolated implementer/reviewer exercise with handoff and conflict resolution |
| L06 | Backpressure and recovery; B14–B20 | CI/scanning artifacts, restricted agent-in-CI exercise, PR review evidence |

These are learning increments, not a claim that each row independently delivers
the whole runtime. Establish build/test scaffolding first; introduce dependencies
as needed without pretending incomplete components are production-ready.

Exercise completion requires the learner to explain the invariant and failure
case, inspect the diff, and locate evidence for the result. A useful first
question is: why are two executions of job A sequential within one topic,
yet allowed to overlap across the main and retry topics?

Keep project direction and reusable instructions in AGENTS.md, task progress in
task records, and transient exploration out of durable guidance. Revalidate
handoffs against the current commit. Preserve failed experiments and their
corrections as evidence; never invent a successful run to fill a learning row.

Current learning status: architecture/specification artifacts and L01's Maven
foundation and unit validation exist. Worker runtime, integration evaluations,
MCP configuration verification, multi-agent exercises, and GitHub enforcement
controls have not yet been demonstrated. See the [L01 task record](../docs/tasks/L01-envelope-validation.md).
