# Spec 002: Kafka-native job scheduling and execution

Status: **Draft — architecture direction agreed; implementation pending**

Project: `agentic-job-scheduler`
Date: 2026-10-08
Supersedes: the PostgreSQL scheduling architecture in [Spec 001](001-job-lifecycle.md)

## 1. Scope and decisions

Clients publish jobs directly to Kafka. Vert.x Kafka consumers own partitions,
maintain durable scheduling state backed by Kafka, and execute different job IDs
concurrently under an adaptive limiter. Records with the same jobId execute
sequentially through the same owning verticle, including retries. Failed jobs
use a retry topic and ultimately a dead-letter queue (DLQ).

No PostgreSQL, JDBC, Flyway, database outbox, or database-backed API is required.
PostgreSQL may be considered later for EDR; it has no scheduling responsibility
in this increment. Spec 001 remains historical context, not an implementation
prerequisite. Its REST contract, SQL schema, and acceptance tests do not apply.

Retain Java 21, Vert.x, Maven, JUnit 5, Testcontainers, and Docker Compose. Deliver
an executable scheduler, a sample Kafka producer, and a deterministic demo
handler. Pin compatible dependency/container versions during implementation.
Cron, cancellation, rescheduling, an HTTP query API, production business handlers,
and automated DLQ redrive are deferred.

## 2. Architecture and ownership

```text
Client producer -> jobs.commands.v1 -> Vert.x partition owners
                                           |
                                 Kafka-backed scheduling state
                                           |
                                 Per-job sequential queues
                                           |
                                Adaptive concurrency limiter
                                           |
                                        Handlers
                                       /   |    \
                                success  retry   permanent/exhausted
                                   |       |             |
                             jobs.events  jobs.retry    jobs.dlq
                                           |
                                      Retry forwarder
                                           |
                                    jobs.commands.v1
```

The commands topic is the single ownership boundary. Only its owning scheduler
consumer may admit or execute that partition's jobs. Retry consumers forward
commands; they never invoke handlers. Independently consuming main and retry
topics for execution would not establish one owner for a job across those topics.

Use consumer group `job-scheduler-v1`. Each consumer belongs to one verticle and
may own several partitions. Each assigned partition has a serialized state
coordinator; handlers run asynchronously through bounded executors. Logical
ownership remains in the verticle even when worker threads vary. All callbacks
return to the coordinator before mutating state. Never block the event loop.

## 3. Topics, routing, and retention

| Topic | Key | Purpose | Cleanup |
| --- | --- | --- | --- |
| jobs.commands.v1 | jobId | SUBMIT and internal RETRY_READY commands | delete, 7 days |
| jobs.state.v1 | typed state key | Authoritative execution records, lane metadata, and owner epoch | compact only |
| jobs.retry.v1 | jobId | Durable retry handoff to the command stream | delete, 7 days |
| jobs.events.v1 | jobId | Lifecycle events for inspection and future projections | delete, 30 days |
| jobs.dlq.v1 | jobId, or source identity for invalid records | Permanent/exhausted failures and invalid input | delete, 30 days |

Use 12 partitions by default for the command, state, retry, and event topics.
Explicitly route state records to their originating command partition, regardless
of their typed key. Pin the routing algorithm for UTF-8 jobId keys to Kafka's
Java keyed default partition calculation and test producer compatibility. Reject
valid-looking commands whose key or partition does not match the contract.
Do not increase partition counts in place; repartitioning requires a separately
specified migration. Provision topics explicitly, with auto-creation disabled.

Local Compose uses one KRaft broker, replication factor 1, and min ISR 1.
Document a deployment baseline of replication factor 3 and min ISR 2 on at least
three brokers. Enable idempotent production and `acks=all`. Consumers of internal
transactional output use `isolation.level=read_committed`.

Retain active state indefinitely. Retain terminal deduplication state for 30 days,
then delete it with tombstones through the owning coordinator. This defines a
30-day deduplication horizon, not permanent duplicate suppression. Preserve lane
sequence counters and fencing epochs. Bound cleanup batches. Restore local state
from an empty cache on each assignment so expired tombstones cannot leave stale
entries in an old cache.

Command/retry retention bounds how long an unconsumed record survives. Alert
before consumer lag approaches retention; Kafka is not unlimited storage. Jobs
already admitted into the compacted state topic survive command expiry. Document
capacity, broker disk monitoring, ACLs, and development-only plaintext settings.

## 4. Client contract and identity

Publish UTF-8 JSON to `jobs.commands.v1`, keyed by canonical job UUID:

```json
{
  "schemaVersion": 1,
  "type": "SUBMIT",
  "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
  "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
  "jobType": "demo.echo",
  "payload": { "message": "hello" },
  "scheduledAt": "2026-10-09T01:00:00Z",
  "maxAttempts": 3
}
```

jobId is the serialization lane. executionId identifies one logical invocation.
A client retry must reuse both identifiers and the same immutable content.
A new invocation uses a new executionId, even when jobId is unchanged. Identity
and deduplication are scoped to `(jobId, executionId)`.

Require UUID identifiers, schema version 1, a registered jobType of 1–128
characters, object payload, explicit RFC 3339 scheduledAt, and integer
maxAttempts from 1–100. Limit the entire serialized record to 256 KiB and configure
Kafka limits to accommodate internal envelopes. Reject unsupported versions,
unknown SUBMIT fields, invalid timestamps, and mismatched keys/partitions to DLQ.

Same identity plus identical normalized content is a duplicate: do not enqueue
another execution. Different content under the same identity produces a conflict
DLQ record without changing the original. Use a documented deterministic JSON
canonicalization and SHA-256 digest for immutable-content comparison.

Producer acknowledgment means Kafka durably accepted the command under its
replication configuration. It does not mean scheduler validation or execution
succeeded. The sample client also shows how to inspect lifecycle events.

## 5. Kafka-backed state and admission transactions

Use an embedded disk-backed materialized cache, such as RocksDB, for local indexes.
Kafka is authoritative; local files can be deleted and rebuilt. This design uses
Vert.x consumers and explicit Kafka transactions, not a hidden relational store.

State records include:

- Execution identity, immutable command and digest, lane sequence, status,
  scheduledAt, attempt, nextAttemptAt, retry token, start/completion times,
  bounded error code, and monotonic state version.
- Per-job next sequence and current head identity, stored separately so a hot
  job never creates an unbounded single Kafka record.
- Per-partition owner epoch for fencing-capable handlers.

Build local indexes by `(jobId, sequence)` and by `(headDueAt, jobId)`. Statuses
are SCHEDULED, READY, RUNNING, RETRY_WAIT, SUCCESS, and FAILED. Admission order is
committed SUBMIT offset order within the job's command partition. Across producer
processes this is broker append order, not client wall-clock order.

Each partition uses one transactional producer with a stable ID derived from
scheduler deployment namespace and partition. Serialize its short transactions;
never hold a transaction open while running a handler or waiting for a due time.
Initialize it on assignment to fence previous producers for that partition.

Admission atomically writes execution/lane state, lifecycle output, and command
consumer offsets using the current group metadata. Apply cache changes only
after transaction commit. Duplicate commands may advance offsets without creating
state. Invalid commands atomically write DLQ plus their consumed offsets.

Commit only a contiguous prefix of admitted command records per partition.
Admission may commit before execution because the full pending work is now
recoverable from Kafka state. This intentionally separates command intake from
handler completion, allowing future jobs and slow handlers to avoid holding the
command offset open. Never commit merely because a record reached an in-memory
queue. Other partitions may progress independently.

Kafka transactions atomically cover Kafka output and input offsets; external
handler effects are outside that transaction. See [Kafka transaction semantics](https://kafka.apache.org/design/).

## 6. Scheduling and per-job sequential execution

Every 100 ms, scan at most 100 due lane heads, using the local due-time index.
Compare scheduledAt/nextAttemptAt against synchronized UTC wall-clock time.
Recheck immediately before dispatch. Clock drift and pauses affect precision;
monitor clock skew and do not promise exact execution times. Use monotonic time
for durations, handler timeouts, and limiter windows.

A Kafka timestamp is metadata, not delayed delivery. Pending deadlines live in
Kafka-backed state and are reconstructed on recovery; an idle partition still
gets timer ticks. No sleeping consumer threads, one-timer-per-record requirement,
or unbounded in-memory timer queues.

For each lane, execute only its head and only while holding a lane permit plus
an adaptive global-per-process permit. Persist RUNNING and its attempt before
invoking the handler. Initial attempt is 1. A retry increments it only when that
new attempt is dispatched. Recovery of an uncertain RUNNING attempt preserves
its attempt identity and uses the same handler idempotency key.

Keep a lane blocked while its head is scheduled, running, retrying, or awaiting
a durable outcome. Later executions cannot overtake a failed head during retry
backoff, even if they are already due. Other lanes continue. On SUCCESS or FAILED,
commit the terminal state and event (plus DLQ for FAILED), advance the lane head,
and only then allow its successor to start.

One execution at a time per jobId is enforced by the owning coordinator, not by
partition-wide serial execution. Different job IDs in the same partition may
run concurrently. Head-of-line blocking within one jobId is intentional.

## 7. Adaptive limiter and backpressure

Use one process-wide limiter shared by consumer verticles, initially 16 permits,
minimum 1 and maximum 128. Configuration may lower these values for tests or
resource limits. A lane consumes at most one permit. This is a per-process
limit; adding instances increases cluster capacity, not a global quota.

Evaluate non-overlapping 5-second windows with at least 20 completed attempts:

- On downstream throttling, timeout, at least 10% transient failure rate, or p95
  handler latency above a configurable 1-second target, halve the limit, rounded
  down and clamped to the minimum.
- Otherwise, if the ready backlog is nonempty and p95 is below target, add one
  permit up to the maximum.
- With insufficient samples, hold the limit; an explicit throttle signal may
  still trigger one decrease per window. Exclude validation/permanent job errors.

Measure handler time separately from queue wait. Decreasing the limit does not
cancel running work; it stops new starts until usage falls below the limit.
Round-robin eligible lanes/partitions to prevent one hot partition dominating.

Bound fetched-but-unadmitted records to 1,000 and 16 MiB per process, and pause
assigned partitions at either high watermark; resume below 50%. Keep Kafka polling
and group membership active while paused. Allow bounded overshoot for one fetch
and account for it in configuration. Bound executor queues and producer buffers.
Pending admitted jobs live on disk, with a configurable byte budget; at 80% stop
admission, at 60% resume. Execution/recovery may continue to free space. At hard
capacity, fail closed and alert without discarding authoritative state.

Vert.x exposes [partition pause/resume](https://vertx.io/docs/apidocs/io/vertx/kafka/client/consumer/KafkaConsumer.html);
verify polling, callback, and transaction behavior against the pinned version.

## 8. Retries and DLQ

Classify outcomes as success, retryable failure, permanent failure, or uncertain
completion. Retry transient handler failures, throttling, and confirmed cancelled
timeouts. Default maximum attempts is 3 including the initial attempt.
Use exponential delay with equal jitter: base 1 second, cap 5 minutes, choose a
delay in `[capForAttempt/2, capForAttempt]`. Persist the chosen deadline once.

On a retryable outcome with attempts remaining, atomically persist RETRY_WAIT,
its nextAttemptAt and unique retry token, and publish a retry envelope. It carries
identity, expected completed attempt, retry token, and notBefore, not a new
execution identity. A stateless retry forwarder transactionally publishes
RETRY_READY to the command topic and commits its retry offsets. Forwarding is
immediate: the scheduling owner provides the delay.

Only a matching active retry token can activate a pending retry. Ignore duplicate,
stale, terminal, or wrong-attempt retry commands. Future retries enter the same
due-time index and lane gate; retry forwarders cannot bypass ordering or permits.

Because retry transport has finite retention, every 60 seconds scan a bounded
batch of RETRY_WAIT records still missing their return command. Re-emit the same
retry token when its last emission is older than 5 minutes. Persist that emission
time transactionally. Duplicate handoffs remain harmless.

On permanent failure or exhausted attempts, atomically persist FAILED, emit its
lifecycle event, and write one logical DLQ event. Include stable dlqId, identity,
attempt count, sanitized reason, timestamps, original command for valid jobs,
and source topic/partition/offset. For invalid input include a bounded byte sample
and hash instead of copying arbitrary oversized data. Do not log payloads.
Size internal envelopes and enforce limits before committing offsets.

No automatic DLQ replay. Document manual resubmission with a new executionId;
reusing a terminal identity within the deduplication horizon is a no-op. Kafka
publication failures do not count as handler attempts; retry or recover the short
transaction without rerunning a handler whose outcome is still held by the owner.
An unknown commit outcome requires fencing/recovery before further dispatch.

## 9. Rebalances, crashes, and external effects

On assignment, pause intake and dispatch for that partition. Initialize its stable
transactional producer, rebuild state with a read-committed reader through the
captured last stable offset, then transactionally increment its persisted owner
epoch. Rebuild lane/due indexes and resume only after this barrier. Abort/resolve
old transactions before capturing the restore boundary. Never start from an
unrestored cache or skip to the latest command offset.

On revocation, stop new work, invalidate the local assignment generation, cancel
or drain handlers within a bounded deadline, and stop timers. Do not block the
consumer poll thread during drain. Discard late callbacks from that generation.
Fenced producers stop permanently. A new owner restores RUNNING attempts as
uncertain; replay is allowed only under the handler recovery contract below.

Kafka fencing protects Kafka state writes. It cannot stop an old process from
continuing an HTTP request or other external side effect. Therefore:

- Stable ownership guarantees one local in-flight execution per jobId.
- Graceful handoff drains or confirms cancellation before the old owner releases
  its execution gate.
- Strict cross-failure exclusion requires the execution target to enforce an
  atomic per-job gate and monotonically increasing owner fencing epoch, including
  rejection of stale completion writes. Pass `(partition, ownerEpoch)` and
  `(jobId, executionId)` to capable handlers.
- Idempotency keys suppress duplicate effects but alone do not guarantee absence
  of concurrent handler computation. Do not claim universal exactly-once execution.

The demo handler is cancellable and replay-safe. A production handler must declare
whether it supports confirmed cancellation, replay idempotency, and target-side
fencing. Without sufficient support, uncertain timeout/crash outcomes block that
lane for operator resolution; do not automatically retry or advance its successor.
Record `uncertain=true` in RUNNING state and emit an operational event. Resolution
of arbitrary external effects is outside this increment.

Default handler deadline is 30 seconds and shutdown drain is 30 seconds; validate
these against polling/session settings. A timeout is not proof that work stopped.
On Kafka unavailability, stop new dispatches requiring durable RUNNING state and
retain bounded completion results while attempting recovery. Readiness remains
false until Kafka connectivity, topic validation, and state restoration succeed.

## 10. Observability and local artifacts

Deliver `pom.xml`, Java sources/tests, Dockerfile, Compose, topic provisioning,
sample producer, README, and AGENTS.md. No PostgreSQL container or migration files.
Separate messaging, scheduling/state, execution, limiter, retry, and configuration
packages. Use a supported transactional Kafka adapter; any native client calls
that may block run on dedicated threads with explicit client ownership.

Expose/log consumer lag, restore progress, active lanes, due backlog, oldest due
age, permits/current usage, handler latency, retries, DLQ counts, state disk use,
fencing events, and uncertain executions. Structured lifecycle events include
jobId, executionId, attempt, state version, owner epoch, and timestamp. Metrics
must not use unbounded job IDs as labels. Event history may feed EDR later.

README demonstrates direct production, a future job, concurrent distinct IDs,
sequential same-ID jobs, retry backoff, DLQ, broker outage, and worker restart.
Document development credentials, persistent Kafka volumes, destructive volume
removal, and the limitations of a single-broker demo. Broker acknowledgment,
scheduler admission, handler start, and successful completion are distinct events.

## 11. Acceptance tests

Use real Kafka through Testcontainers with deterministic handlers, controlled
barriers, bounded waits, and injected crashes at transaction boundaries. Do not
substitute an in-memory broker. `mvn test` runs unit tests; `mvn verify` runs
integration tests and fails clearly when Docker is unavailable.

| ID | Scenario and required assertion |
| --- | --- |
| B01 | Direct SUBMIT admission persists state and offsets atomically; abort exposes neither |
| B02 | Future head never starts before its deadline; idle-topic timers still dispatch it |
| B03 | Same jobId, distinct executionIds execute in admitted order with maximum concurrency 1 |
| B04 | Different IDs execute concurrently, including within a single partition |
| B05 | Exact duplicates do not re-execute; conflicting content reaches DLQ without replacing state |
| B06 | A failed head blocks later same-ID jobs across backoff; unrelated lanes continue |
| B07 | Retry forwarding, restart, and duplicate tokens preserve identity, attempts, and ordering |
| B08 | Permanent/exhausted failures atomically create FAILED and DLQ, then release the next job |
| B09 | Delete all local state and restart: pending, delayed, terminal, and retry state restore correctly |
| B10 | Crash before/after admission, RUNNING, and terminal commits loses no admitted work; uncertain effects follow handler contract |
| B11 | Rebalance during execution fences old Kafka writes and ignores stale callbacks |
| B12 | Fencing-capable test target rejects old epochs and enforces per-job exclusion; unsupported uncertain handler stays blocked |
| B13 | Throttle/latency signals reduce permits; healthy load increases them; configured bounds hold |
| B14 | Queue/disk watermarks pause/resume intake while membership and unrelated execution remain live |
| B15 | Out-of-order handler completion never loses durable pending work or advances non-admitted command offsets |
| B16 | Retry record expiry is recovered by state reconciliation with the same token |
| B17 | Invalid keys, partition routing, versions, fields, and oversized input reach bounded DLQ records |
| B18 | Broker outage starts no undurable execution and recovery restores consistent ownership/state |
| B19 | Terminal state deduplicates within 30 days; controlled expiry documents replay behavior |
| B20 | Compose smoke test runs producer, scheduling, parallelism, retries, DLQ, and restart with no PostgreSQL |

Unit tests cover canonical identity, lane ordering, due-time comparisons, limiter
windows, jitter bounds, retry classification, state-version guards, and bounded
DLQ encoding. Integration tests establish transaction and recovery guarantees.
Record checks actually run during implementation; this document does not claim
an implementation or passing runtime tests.
