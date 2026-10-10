# Architecture documentation (arc42)

Snapshot: 2026-10-10, L01–L06 worker-runtime work. [Spec 002](specs/002-kafka-job-scheduling.md)
is the requirements and architecture authority; this document follows the
[arc42](https://arc42.org) template and records decisions as ADRs (section 9).
[Spec 001](specs/001-job-lifecycle.md) is historical.

---

## 1. Introduction and goals

### 1.1 Requirements overview

Clients publish jobs to Kafka. Vert.x workers consume them as soon as records,
worker capacity, and the adaptive limiter allow. A retryable first failure goes
to a retry topic consumed by separate, slower retry workers. Permanent or
exhausted failures go to a dead-letter queue (DLQ). Kafka records and consumer
offsets are the only durable backlog.

There is no PostgreSQL, Flyway, REST API, delayed-job timer, scheduling state
topic, or RocksDB index. EDR is deferred.

### 1.2 Quality goals

| Priority | Goal | Meaning here |
| --- | --- | --- |
| 1 | No lost work | Commit only completed record prefixes; retry/DLQ handoff acknowledged before the source completes |
| 2 | Per-job exclusion | One execution per jobId at a time within a topic under stable ownership; different IDs run concurrently |
| 3 | Bounded resources | Retained records/bytes and handler concurrency are capped; intake pauses instead of growing memory |
| 4 | Operational independence | Main and retry workers scale, fail, and are rate-limited independently |
| 5 | Explainable behavior | Invariants are tested at crash/commit boundaries with recorded evidence |

### 1.3 Stakeholders

| Role | Expectation |
| --- | --- |
| Job clients | Submit jobs by publishing to `jobs.main.v1`; replay-tolerant outcome |
| Handler authors | Implement replay-safe, concurrency-tolerant handlers using `(jobId, executionId)` as idempotency key |
| Operators | Scale roles, monitor lag/oldest-unprocessed age, inspect the DLQ |
| Maintainers/reviewers | Traceable acceptance IDs (B01–B20) and evidence per increment |

---

## 2. Constraints

| Constraint | Source |
| --- | --- |
| Java 21, Maven, Vert.x, JUnit 5, Testcontainers, Docker Compose | Spec 002 §1 |
| Kafka provides durability; no database or state store | Spec 002 §1 |
| Fixed topic partition counts (default 12/12/3) | Spec 002 §3 |
| Client records ≤ 256 KiB; internal records reserve ≥ 512 KiB | Spec 002 §4 |
| No HTTP API, production business handler, or Kubernetes deployment in this increment | Spec 002 §1 |
| Never block the event loop; never share a native Kafka consumer across threads | Spec 002 §2 |

---

## 3. Context and scope

### 3.1 Business context

```text
 Job client ──publish (key = jobId)──> jobs.main.v1 ──> Main workers ──> Job handler
                                                            │ retryable failure
                                                            ▼
                                       jobs.retry.v1 ──> Retry workers ──> Job handler
                                                            │ permanent / exhausted
                                                            ▼
                                       jobs.dlq.v1  <── (also: invalid input)  ──> Operator
```

### 3.2 Technical context

| Neighbor | Interface | Notes |
| --- | --- | --- |
| Kafka broker | Kafka protocol; idempotent producer (`acks=all`), manual-commit consumers | Local Compose: single KRaft broker, RF 1, persistent volume. Deployment baseline: 3 brokers, RF 3, min ISR 2 |
| Job handler | `JobHandler` future-returning SPI | Demo handler only; completes only after work has stopped |
| Docker/Compose | `compose.yaml`, `Dockerfile` | Roles `main-worker`, `retry-worker`, tool profile `sample-producer` |

Out of scope: HTTP clients, databases, external-effect fencing.

---

## 4. Solution strategy

| Goal | Approach | ADR |
| --- | --- | --- |
| No lost work | Kafka as backlog; explicit safe-prefix commits; acknowledged handoffs | 1, 4, 5 |
| Immediate start | Run queue heads as soon as capacity exists; no eligibility timestamps | 2 |
| Per-job exclusion | Keyed partitioning plus per-jobId FIFO gates in the owning verticle | 3 |
| Retry isolation | Separate topic, group, and worker deployment | 6, 7 |
| Bounded resources | Processing window with pause/resume; adaptive main limiter; retry token bucket | 8, 9 |
| Safe input | Validate raw bytes before dispatch; bounded DLQ samples | 10 |

---

## 5. Building block view

### 5.1 Level 1: components

| Increment | Component | Responsibility | Executed test cases |
| --- | --- | --- | ---: |
| L01 | `EnvelopeValidator` | Validate raw main/retry envelopes and preserve invocation identity | 77 |
| L02 | `OffsetCommitTracker` | Propose commits only for completed delivered prefixes | 17 |
| L03 | `ImmediateJobDispatcher` | Start eligible queue heads; serialize each jobId within its topic | 14 |
| L04 | `FailureClassifier` + `AdaptiveLimiter` | Classify retryable vs exhausted/permanent failures; main-worker backpressure policy | focused unit coverage |
| L05 | `ImmediateJobDispatcher.revokeOwnership()` | Stop admission/dispatch and ignore stale local callbacks for a revoked owner | 3 |
| L06 | `ProcessingWindow`, `ProcessingWindowKafkaController`, `KafkaWorker` runtime | Bound intake, run main/retry roles, publish acknowledged handoffs, commit safe offsets | 5 window unit + 19 dispatcher + 5 limiter + 2 Kafka integration |

These are executed cases, including parameterized inputs. L03 tests run real
Vert.x event loops with controlled handler futures; crash/replay tests use a
simulated broker store. The two L06 integration tests use a real Kafka broker
and do not constitute the full B14–B20 suite. Evidence:
[L01](docs/tasks/L01-envelope-validation.md),
[L02](docs/tasks/L02-completion-offsets.md),
[L03](docs/tasks/L03-immediate-processing.md),
[L04](docs/tasks/L04-retry-dlq-limiter.md),
[L05](docs/tasks/L05-replay-rebalances.md), and
[L06](docs/tasks/L06-backpressure-recovery.md).

Runtime classes (`runtime/`): `WorkerMain` (one role per process), `KafkaWorker`
(consumer group member), `WorkerConfig`/`WorkerRole`, `TopicProvisioner`,
`JobRecordEncoder`, `DemoJobHandler`, `SampleProducerMain`.

### 5.2 Level 2: worker verticle and partition ownership

Four partitions are shown for clarity; Spec 002 defaults main and retry to
twelve. Assignment is broker-managed and changes during rebalances.

```text
Producer: key = canonical jobId
              |
              v
       Kafka topic: jobs.main.v1
       +---------------------------+
       | P0 | P1 | P2 | P3 | ...   |
       +---------------------------+
          |    |    |    |
          +--+-+    +--+-+
             |         |
             v         v
       Verticle A   Verticle B
       owns P0,P1   owns P2,P3

Consumer group: job-main-v1

+----------------------------+  +----------------------------+
| Main worker verticle A     |  | Main worker verticle B     |
|                            |  |                            |
| Own Kafka consumer         |  | Own Kafka consumer         |
| Assigned P0, P1            |  | Assigned P2, P3            |
|          |                 |  |          |                 |
|          v                 |  |          v                 |
| EnvelopeValidator     L01  |  | EnvelopeValidator     L01  |
|          | valid job       |  |          | valid job       |
|          v                 |  |          v                 |
| ImmediateJobDispatcher L03 |  | ImmediateJobDispatcher L03 |
|   per-job queues + gates   |  |   per-job queues + gates   |
|   async JobHandler calls   |  |   async JobHandler calls   |
|          |                 |  |          |                 |
| OffsetCommitTracker   L02  |  | OffsetCommitTracker   L02  |
|   safe next offsets        |  |   safe next offsets        |
+----------------------------+  +----------------------------+
             |                              |
             +----- explicit commits -------+
                            |
                            v
                 Kafka group offsets
```

Within a consumer group each partition has one owner at a time; one consumer may
own several partitions. A worker owns its consumer and serializes queue,
completion, and commit-state mutations on its Vert.x context. Verticles are
logical components, not necessarily separate pods or dedicated threads.

With fixed partition counts and consistent keyed production, a jobId routes to
one partition per topic; different IDs can share a partition. The worker
validates the expected keyed partition before dispatch. Changing partition
counts can remap keys and is an operational migration.

An event loop starts nonblocking operations and processes callbacks; several
handlers can be in flight while state changes stay serialized. A blocking
handler needs a bounded executor. L03 marshals handler completions back to the
owner context and rejects direct API calls from the wrong context.

---

## 6. Runtime view

### 6.1 Validate before dispatch (L01)

```text
Raw Kafka key bytes + value bytes       (Kafka worker)
                |
                v
       EnvelopeValidator
          /             \
       valid           invalid
         |                |
         v                v
 JobEnvelope or      sanitized error code
 RetryEnvelope            |
         |                v
         v          bounded DLQ publication
 JobExecution       + acknowledgment
         |
         v
 L03 dispatcher
```

The 77 L01 cases cover accepted identities/payloads/defaults and rejection of
missing/unknown fields, wrong types, invalid UUIDs or keys, malformed JSON/UTF-8,
size/nesting violations, invalid retry attempts, source coordinates, and handoff
identities. `KafkaWorkerIT` verifies malformed, oversized, and wrong-partition
records reach the DLQ with bounded samples and preserved identity where valid.

`jobId` identifies a serialization lane; `executionId` identifies an invocation.
Replay keeps both IDs; there is no durable deduplication table.

### 6.2 One lane per jobId, concurrency between lanes (L03)

```text
Partition P0, delivered in this order:

 offset 10        offset 11        offset 12
    A1               A2               B1

 Dispatcher queues:

 Job A: [ A1 RUNNING ] --> [ A2 WAITING ]
 Job B: [ B1 RUNNING ]

        A1 and B1 can overlap.
        A2 must wait for A1's gate to release.
```

A queue head starts immediately when handler capacity exists. The runtime
creates one dispatcher per assigned partition, preserves per-job FIFO within it,
and allocates the process limit across busy partitions. Main capacity adapts from
five-second handler-latency/failure windows; retry workers have independent
concurrency and a per-pod one-start-per-second token bucket.

### 6.3 Completion and safe-prefix commits (L02)

Kafka's committed position is a next offset per partition, not a per-record
completion flag. Committing 13 resumes at 13; doing so while 10 is unfinished
would skip work.

```text
Step                      Offset 10   Offset 11   Offset 12   Safe progress
                          A1          A2          B1

Start A1 and B1            RUNNING     WAITING     RUNNING     none
B1 finishes               RUNNING     WAITING     COMPLETE    none
A1 finishes; A2 starts     COMPLETE    RUNNING     COMPLETE    commit 11
A2 finishes               COMPLETE    COMPLETE    COMPLETE    commit 13
Broker acknowledges 13    <---------- release retained entries ---------->
```

“None” means no new commit for this partition; it does not erase an earlier
commit. `beginCommit()` scans delivered records in order and stops at the first
incomplete one. Offset gaps are allowed. One commit request may be outstanding;
success releases only the acknowledged snapshot; failure retains results.

The 17 L02 cases cover incomplete prefixes, gaps, independent partitions/topics,
immutable snapshots, serialized requests, failed commits, record bounds, stale or
foreign handles, invalid inputs, simulated crash/replay, and 40 deterministic
shuffled completion schedules. Completed-but-uncommitted records still count
toward the record bound.

### 6.4 Capacity release versus gate release

```text
                          handler actually stops
                                   |
                        +----------+----------+
                        |                     |
                     success                failure
                        |                     |
                release capacity      release capacity
                complete record       keep record incomplete
                release Job A gate    keep Job A gate
                        |                     |
                  A2 may start         HANDOFF_PENDING
                                              |
                              publish retry/DLQ
                                   /                \
                              send fails         broker ACK
                                  |                  |
                          keep gate/record     complete record
                          retry same send      release Job A gate
                          do not rerun A1            |
                                                A2 may start
```

For supported handlers the returned future completes only after work has
actually stopped. While failed A1 awaits publication, job B may use the freed
handler slot but A2 stays blocked. The worker retries the same bounded record
after a failed send and does not invoke the handler again. A never-finishing
handler keeps its gate and capacity; a null future violates the contract and
leaves the submission `STUCK`. Shutdown pauses intake and drains for up to 30
seconds; after the deadline, outstanding records remain uncommitted for replay.
Confirmed cancellation for arbitrary handlers is not provided.

### 6.5 Main and retry overlap

```text
jobs.main.v1                         jobs.retry.v1
Group: job-main-v1                    Group: job-retry-v1
Main worker                          Retry worker (separate process)
      |                                      |
   A1 fails                                  |
      +--- publish retry A1 ---------------->|
      | wait for broker ACK                  v
      v                                 retry A1 RUNNING
 ACK received                                |
 release main Job A gate                     |
      v                                      v
 main A2 RUNNING                        retry A1 RUNNING
          \__________________________________/
                   overlap is allowed
```

Retry workers execute their own topic directly and never forward to main.
Matching partition numbers across topics create no shared lock. Retry envelopes
carry attempt history and source provenance; `failedSource` refers to the record
that previously failed.

### 6.6 Ownership revocation (L05)

`revokeOwnership()` is a context-confined hook that stops admission and dispatch;
late handler completions, handoff acknowledgments, and commit results from that
dispatcher do not advance local state. The caller discards the old dispatcher and
builds a fresh dispatcher/tracker for reassigned partitions.

```text
Old owner: A RUNNING, B QUEUED
                 |  revokeOwnership()
                 v
       no more admission/dispatch; late local callbacks ignored
                 |
New owner: fresh tracker + dispatcher; broker may replay A; handler may run again
```

Revocation does not cancel in-flight handler work or fence external effects, and
an ignored local callback does not prove a sent commit was rejected. Tests cover
this library boundary, not a live consumer-group rebalance.

### 6.7 Bounded retained-work window (L06)

`ProcessingWindow` is a thread-safe, process-scoped ledger. Each fetched batch
reserves its record count and key/value bytes, held until the source offset
commit is acknowledged; handler success or handoff acknowledgment alone does not
release capacity.

```text
              retained work (count OR bytes reaches high watermark)
  intake open -------------------------------------------------> paused
       ^                                                           |
       +---------------- both dimensions below 50% ----------------+

 pauseAt = hard bound - one configured fetch batch
 hard bound = 1,000 records OR 16 MiB (constructor-configurable)
```

`ProcessingWindowKafkaController` pauses/resumes assigned partitions through the
Vert.x Kafka per-partition API, not the global ReadStream. The worker bounds
fetches to two records/2 MiB against the 1 MiB topic record cap and keeps a
fetched record locally if it cannot yet reserve capacity; nothing is dropped or
dispatched outside the window. Five unit tests cover thresholds, hysteresis, one
bounded overshoot, and reservation lifecycle; `ProcessingWindowKafkaIT` covers
pause/poll/commit/resume.

---

## 7. Deployment view

```text
                 +-----------------------------+
                 | Kafka (KRaft, volume)       |
                 | main / retry / dlq topics   |
                 +--------------+--------------+
                                |
        +-----------------------+-----------------------+
        |                                               |
 main-worker (WORKER_ROLE=main)               retry-worker (WORKER_ROLE=retry)
 group job-main-v1, scale N                   group job-retry-v1, scale M
 concurrency adaptive, max 128                concurrency default 2, 1 start/s
        one KafkaWorker, one ProcessingWindow per process
```

`WorkerMain` starts exactly one `KafkaWorker` whose role comes from
`WORKER_ROLE`. [compose.yaml](compose.yaml) defines `kafka`, `main-worker`,
`retry-worker`, and a `sample-producer` tool profile; the broker uses a named
volume. Topics are provisioned idempotently at worker start. The three-broker,
RF 3, min ISR 2 baseline is documented, not provisioned locally.

Evidence: the Compose stack was run with two retry replicas, a persistent-volume
broker restart replayed queued main work, and malformed input reached the DLQ
(manual checks, see the [L06 record](docs/tasks/L06-backpressure-recovery.md)).
Sustained broker outage behavior has not been tested.

---

## 8. Cross-cutting concepts

- **Idempotency and replay.** A crash after handler success or handoff
  acknowledgment, before source commit, replays the record; duplicate handoffs
  are possible. Handlers must tolerate replay and concurrent duplicates using
  target-side idempotency keyed by `(jobId, executionId)`.
- **Ordering.** Per-job exclusion holds within a stable topic owner only. No
  global ordering, cross-topic exclusion, durable deduplication, exactly-once
  effects, or external-effect fencing is claimed.
- **Retention.** Offsets do not protect records from retention deletion;
  monitor lag and oldest-unprocessed age.
- **Concurrency model.** All queue, completion, and offset mutations are
  confined to the owner Vert.x context; commits are serialized per worker.
- **Observability.** Diagnostics carry codes, not raw payloads; DLQ samples are
  bounded (≤ 256 bytes).
- **Testing.** Observable invariants and crash boundaries; unit, broker
  integration, and manual Compose checks are recorded separately.

---

## 9. Architecture decisions

Status values: Accepted, Superseded. Context for all ADRs is [Spec 002](specs/002-kafka-job-scheduling.md).

### ADR 1: Kafka is the durable backlog; no database or state store

- **Status:** Accepted (supersedes Spec 001's PostgreSQL design).
- **Context:** A job scheduler normally needs durable pending state.
- **Decision:** Kafka records and consumer-group offsets hold pending work. No PostgreSQL, Flyway, REST API, scheduling-state topic, or RocksDB.
- **Consequences:** Simple operations and replay from offsets. Records can expire under retention during long outages; no durable deduplication or query API.

### ADR 2: Immediate processing; no delayed scheduling

- **Status:** Accepted (supersedes the earlier delayed-scheduling draft).
- **Decision:** A valid record runs as soon as it is its per-job queue head and a permit exists. No `scheduledAt`/`notBefore`; such fields are rejected.
- **Consequences:** No timer indexes. Waiting for a permit is backpressure, not scheduling. Delays happen only via the slower retry path.

### ADR 3: Per-job serialization within a topic via keyed partitioning and per-verticle gates

- **Status:** Accepted.
- **Decision:** Produce with key = jobId using Kafka's default partitioner; the owning verticle keeps FIFO queues per jobId and runs one handler per jobId at a time. Different IDs, even in one partition, run concurrently.
- **Alternatives:** One thread per partition (loses cross-job concurrency); global locks or a lock store (violates ADR 1).
- **Consequences:** Exclusion holds only under stable ownership; rebalance replay can overlap (see section 8). Partition counts are fixed; the worker validates key/partition agreement.

### ADR 4: Commit only completed record prefixes

- **Status:** Accepted.
- **Decision:** `OffsetCommitTracker` proposes a next offset only when all earlier delivered records are complete. Auto-commit is disabled.
- **Consequences:** A slow record holds back its partition's commit and counts against the retained-work bound; crashes replay uncommitted completed work.

### ADR 5: Acknowledged handoff before source completion; hold the gate until then

- **Status:** Accepted.
- **Decision:** Mark the source complete only after success or broker acknowledgment of the retry/DLQ publication. On send failure, retry the identical record without rerunning the handler and keep the jobId gate.
- **Consequences:** No lost work, but duplicate handoffs are possible after crashes. `handoffId` (hash of failed source and attempt) makes duplicates identifiable, not suppressed. Verified by the injected-send-failure case in `KafkaWorkerIT`.

### ADR 6: Independent main and retry topics, groups, and ordering

- **Status:** Accepted.
- **Decision:** Main uses `jobs.main.v1`/`job-main-v1`; retry uses `jobs.retry.v1`/`job-retry-v1`. Retry workers execute jobs directly and never re-enqueue to main. A new main execution may overlap an older retry for the same jobId.
- **Alternatives:** Cross-topic exclusion (needs shared state; future work).
- **Consequences:** Relaxed cross-topic ordering is explicit; handlers must tolerate concurrent duplicates.

### ADR 7: Retry topic is consumed by a separate worker deployment, not a second verticle in the main worker

- **Status:** Accepted. Alternative retained as a documented option.
- **Context:** The retry topic could be consumed either (a) by a separate worker process/deployment started with `WORKER_ROLE=retry`, or (b) by an additional retry verticle deployed inside the main worker process alongside the main verticles.
- **Decision:** Use (a). `WorkerMain` starts exactly one `KafkaWorker` selected by role; Compose runs `main-worker` and `retry-worker` as distinct services scaled independently.
- **Rationale:**
  - Retry traffic is slower and lower-concurrency by design (default concurrency 2, one start per second per pod); a separate process enforces that limit without competing for the main process's event loops, handler capacity, or `ProcessingWindow`.
  - Failure and restart isolation: a retry storm, slow handler, or crash does not stall or restart main intake, and vice versa.
  - Independent scaling and resource limits per role (main up to 128 handler slots with adaptive limiting).
  - Each process has its own `ProcessingWindow`, so retained-work bounds per role are not shared or contended.
- **Alternative (b), not implemented:** Fewer deployables and lower footprint for small installations. It would need a shared per-process capacity budget between the verticles, a decision about whether one `ProcessingWindow` is shared, and a guard against retry work starving main work. Because `KafkaWorker` is role-parameterized and owns its consumer, producer, window, and limiter, hosting both roles in one process would be an additive composition change, not a redesign. It would require a new ADR superseding this one and fresh B14–B20 evidence.
- **Consequences:** More deployables and configuration; two consumer groups to monitor. Matching partition numbers across the topics still imply no shared ownership (ADR 6).

### ADR 8: Bounded retained work with per-partition pause/resume

- **Status:** Accepted.
- **Decision:** `ProcessingWindow` caps retained records (1,000) and key/value bytes (16 MiB), reserving headroom for one fetch (two records/2 MiB). Intake pauses at the high watermark and resumes below 50% of both. Reservations release only after the source commit is acknowledged.
- **Alternatives:** Unbounded buffering; global ReadStream pause (stalls partitions that could progress).
- **Consequences:** Bounded memory; a stuck handler can pause its partition's intake. Rebalance interaction is not yet verified live.

### ADR 9: Adaptive limiter for main; token bucket for retry

- **Status:** Accepted.
- **Decision:** Main concurrency adapts from five-second latency/failure windows (1–128). Retry uses fixed low concurrency and a one-start-per-second per-pod token bucket.
- **Consequences:** Protects downstream targets from retry bursts. Runtime behavior of the limiter and rate gate is covered by unit tests; controlled worker-level evidence (B07/B13) is outstanding.

### ADR 10: Validate raw bytes first; route invalid input to a bounded DLQ

- **Status:** Accepted.
- **Decision:** Read raw bytes, validate with `EnvelopeValidator` (size, nesting, UUID/UTF-8, unknown fields, key/partition), and publish invalid input to the DLQ as a sanitized code plus a bounded sample.
- **Consequences:** Malformed data cannot trap deserialization or leak payloads; raw input is never logged.

---

## 10. Quality requirements

| Scenario | Expected outcome | Evidence |
| --- | --- | --- |
| Newer record finishes before an older one in a partition | Commit stops at the first incomplete record | L02 tests (shuffled schedules) |
| Retry publication fails after handler failure | Handler not rerun; source uncommitted; gate held; recovers when publish succeeds | `KafkaWorkerIT` |
| Retained work reaches the high watermark | Partition pauses, polling continues, resumes below 50% | `ProcessingWindowTest`, `ProcessingWindowKafkaIT` |
| Malformed/oversized/wrong-partition input | Bounded DLQ record | `KafkaWorkerIT` |
| Broker restart with queued main work | Work processed after restart | Manual Compose check |
| Worker crash after success, before commit | Record replays; handler tolerates it | Simulated broker store only |

---

## 11. Risks and technical debt

| Area | Status |
| --- | --- |
| Rebalance ownership and stale-callback behavior in the live worker | Unverified (library boundary only) |
| B15: out-of-order completion across independent Kafka partitions at worker level | Covered by `KafkaWorkerIT` (permanent: `PERMANENT_HANDLER_FAILURE`, one run, source committed after DLQ; exhausted maxAttempts=2: `ATTEMPTS_EXHAUSTED` via retry worker, no third attempt) |
| B07/B13: controlled runtime limiter and retry-rate behavior | Not covered at worker level |
| B08: retry exhaustion and permanent failure through the worker to the DLQ | Not covered |
| B09/B20: replay after real worker process loss | Covered by `KafkaWorkerCrashIT` (child JVM SIGKILLed mid-handler; source uncommitted; replacement replays once and commits). Compose-level kill demo not run |
| B19: confirmed and unconfirmed handler cancellation | Not covered |
| Sustained broker outage (client behavior) | Not tested; only a persistent-volume restart was exercised manually |
| Retention expiry of unprocessed records | Documented risk; monitoring not implemented |
| Fresh-session learning exercise | L03 handoff prepared; resumption pending |

L06 stays open until these are covered; see the
[L06 task record](docs/tasks/L06-backpressure-recovery.md). Basic
[GitHub Actions CI](.github/workflows/maven.yml) runs unit tests/package; required-check
merge enforcement is a separate repository setting.

---

## 12. Glossary

| Term | Meaning |
| --- | --- |
| jobId | Serialization lane and Kafka key |
| executionId | Identifies one logical invocation; stable across retries and replays |
| handoff | Publication of a retry or DLQ record that must be broker-acknowledged before the source completes |
| gate | Per-jobId lock inside a topic's dispatcher |
| safe prefix | Contiguous run of completed delivered records eligible for offset commit |
| processing window | Process-scoped bound on retained records/bytes |
| DLQ | `jobs.dlq.v1`, for permanent, exhausted, or invalid input |

Learning check: if A1 has stopped but its retry publication fails, can A2 start?
**No. Handler capacity is free, but Job A's gate remains held.**
