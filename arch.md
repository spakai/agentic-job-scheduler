# Architecture through L04

Snapshot: 2026-10-10. [Spec 002](specs/002-kafka-job-scheduling.md) is the
architecture authority. This guide explains the components implemented so far
and how they fit into the Kafka worker design.

**Implemented:** envelope validation, completion/offset tracking, a Vert.x
immediate dispatcher with a library-side ownership-revocation hook, failure
classification, and a deterministic adaptive limiter policy. **Planned:**
executable worker verticles, Kafka consumers and publishers, retry/DLQ producer
wiring, broker-integrated rebalance handling, and full backpressure. There is no
PostgreSQL, Flyway, REST API, delayed-job timer, scheduling state topic, or
RocksDB index in this architecture.

## 1. What each increment adds

| Increment | Component | Responsibility | Executed test cases |
| --- | --- | --- | ---: |
| L01 | `EnvelopeValidator` | Validate raw main/retry envelopes and preserve invocation identity | 77 |
| L02 | `OffsetCommitTracker` | Propose commits only for completed delivered prefixes | 17 |
| L03 | `ImmediateJobDispatcher` | Start eligible queue heads; serialize each jobId within its topic | 14 |
| L04 | `FailureClassifier` + `AdaptiveLimiter` | Classify retryable vs exhausted/permanent failures and enforce the main-worker backpressure policy | focused unit coverage |
| L05 | `ImmediateJobDispatcher.revokeOwnership()` | Stop admission/dispatch and ignore stale local callbacks for a revoked owner | 3 |

These are executed cases, including parameterized inputs, not separate test
methods per increment. L03 tests run real Vert.x event loops with controlled
handler futures; L04 adds library-level unit checks for retry/DLQ classification
and adaptive limiter behavior. Crash/replay tests use a simulated broker store;
no real Kafka integration suite exists yet. Evidence: [L01](docs/tasks/L01-envelope-validation.md),
[L02](docs/tasks/L02-completion-offsets.md),
[L03](docs/tasks/L03-immediate-processing.md), and
[L04](docs/tasks/L04-retry-dlq-limiter.md).

## 2. Intended Kafka partition ownership

The following runtime wiring is **planned**. Four partitions are shown for
clarity; Spec 002 defaults main and retry topics to twelve partitions each.
Assignment here is illustrative, not a promised allocation.

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

Within a consumer group, each partition has one consumer owner at a time. One
consumer may own multiple partitions. A worker verticle owns its consumer and
serializes queue, completion, and commit-state mutations on its context.
Verticles are logical components, not necessarily separate pods or dedicated
threads. Never share the native consumer across handler threads.

With fixed partition counts and consistent keyed production, the same jobId
routes to the same partition within a topic. Different job IDs can share a
partition. Kafka key/partition agreement checks remain future adapter work.

An event loop starts nonblocking operations and processes callbacks. Several
handlers can be in flight while their state changes remain serialized. A handler
that performs blocking work needs a bounded executor; it must not block the loop.
L03 marshals handler completions back to the owner context and rejects direct
API calls from the wrong context.

## 3. L01: validate before dispatch

```text
Raw Kafka key bytes + value bytes       (future consumer)
                |
                v
       EnvelopeValidator               (implemented)
          /             \
       valid           invalid
         |                |
         v                v
 JobEnvelope or      sanitized error code
 RetryEnvelope            |
         |                v
         v          bounded DLQ publication
 JobExecution       + acknowledgment       (planned)
         |
         v
 L03 dispatcher
```

The 77 L01 cases cover accepted identities/payloads/defaults and rejection of
missing/unknown fields, wrong types, invalid UUIDs or keys, malformed JSON/UTF-8,
size/nesting violations, invalid retry attempts, source coordinates, and handoff
identities. Invalid-input rejection does not prove DLQ publication.

`jobId` identifies a serialization lane. `executionId` identifies an invocation.
A1 and A2 below share jobId A but have different executionIds. Replay of A1 keeps
both IDs; the service does not maintain a durable deduplication table.

## 4. L03: one lane per jobId, concurrency between lanes

```text
Partition P0, delivered in this order:

 offset 10        offset 11        offset 12
    A1               A2               B1
     \                /                |
      +-------+------+                 |
              |                        |
              v                        v

 Dispatcher queues:

 Job A: [ A1 RUNNING ] --> [ A2 WAITING ]
 Job B: [ B1 RUNNING ]

        A1 and B1 can overlap.
        A2 must wait for A1's gate to release.
```

A queue head starts immediately when handler capacity exists. There is no
scheduled time or eligibility timer. Ready partitions take turns; ready job lanes
within a partition are FIFO. The current capacity is **fixed per dispatcher**,
not the final process-wide adaptive main limit or retry rate budget. The L04
`AdaptiveLimiter` models that policy in memory; the worker runtime will later read
it as a shared process-wide budget once the consumer and producer wiring is
added.

The 14 L03 cases cover immediate starts and event-loop responsiveness, same-job
FIFO exclusion, different-job overlap, independent main/retry dispatchers,
partition fairness, gate retention during handoff, L02 commit integration,
admission bounds, context/input/handle rejection, replay without deduplication,
and completed/failed/null handler futures.

## 5. L02: finishing later work cannot skip unfinished work

Kafka's committed position is a next offset per partition, not a completion flag
for each record. Committing 13 would resume at 13 after restart; doing that while
10 is unfinished would skip work.

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
committed position. Other partitions can advance independently.

`beginCommit()` scans delivered records in order and stops at the first incomplete
record. Gaps in Kafka offset numbers are allowed: an absent integer is not itself
unfinished work. Only one commit request may be outstanding. Success releases
only the acknowledged snapshot; failure retains results for another commit.

The 17 L02 cases cover incomplete prefixes, gaps, independent partitions/topics,
immutable snapshots, serialized requests, failed commits, record bounds, stale
or foreign handles, invalid inputs, and simulated crash/replay boundaries. One
case also exercises 40 deterministic shuffled completion schedules.

Completed-but-uncommitted records still count toward the record bound. A bound
rejection never authorizes dropping fetched work. Process-wide byte accounting,
fetch overshoot, pause/resume, and broker commit I/O remain future work.

## 6. Handler capacity and the job gate release at different times

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
                              publish retry/DLQ (planned)
                                   /                \
                              send fails         broker ACK
                                  |                  |
                          keep gate/record     complete record
                          retry same send      release Job A gate
                          do not rerun A1            |
                                                A2 may start
```

For supported handlers, the returned future must complete only after work has
actually stopped. While a failed A1 awaits publication, job B may use the released
handler slot, but A2 remains blocked. L03 exposes `handoffAcknowledged()`; L04
adds the library-side classification and payload decision rules, but the full
Kafka publisher/consumer wiring remains future runtime work.

A never-finishing handler keeps its gate and capacity. Returning a null future
violates the contract and leaves the submission `STUCK`, also holding both.
Timeout/cancellation, drain, and recovery remain future work; no completion is
manufactured to release a stuck operation.

## 7. Main and retry topics have independent owners

This diagram shows the **planned publication path**. L03 tests the independent
dispatchers and acknowledgment boundary with controlled futures.

```text
jobs.main.v1                         jobs.retry.v1
Group: job-main-v1                    Group: job-retry-v1
Main verticle                        Separate retry verticle
      |                                      |
   A1 fails                                  |
      |                                      |
      +--- publish retry A1 ---------------->|
      |                                      v
      | wait for broker ACK             retry A1 RUNNING
      v                                      |
 ACK received                                |
 release main Job A gate                      |
      |                                      |
      v                                      v
 main A2 RUNNING                        retry A1 RUNNING
          \__________________________________/
                   overlap is allowed
```

Retry workers execute their own topic directly; they do not forward work back to
main. Matching partition numbers across topics do not create a shared lock. L04
models the retry/DLQ decision boundary in code, while full producer wiring,
acknowledgment handling, and retry-rate integration remain future runtime work.

## 8. L05: invalidate a revoked owner

`revokeOwnership()` is a context-confined library lifecycle hook for the
dispatcher’s fixed assignment. It stops admission and dispatch; late handler
completions, handoff acknowledgments, and commit results from that dispatcher do
not advance its local completion or offset state. The caller must discard the
old dispatcher and build a fresh dispatcher/tracker for reassigned partitions.

```text
Old owner: A RUNNING, B QUEUED
                 |
           revokeOwnership()
                 v
       no more admission/dispatch
       late local callbacks ignored
                 |
                 v
New owner: fresh tracker + dispatcher
                 |
          broker may replay A
                 v
       handler may run again
```

Revocation does not cancel in-flight handler work or fence external effects.
Replay can therefore duplicate effects, and a callback ignored locally does not
prove a previously sent broker commit was rejected. The tests exercise this
library boundary, not Kafka consumer-group callbacks or a live rebalance.

## 9. Guarantees, gaps, and the next increments

Per-job exclusion applies within a stable topic owner. It does not guarantee
cross-topic exclusion or prevent an old external operation continuing after
ownership loss. Handlers must tolerate replay and concurrent duplicates using
appropriate target-side idempotency. No global ordering, durable deduplication,
exactly-once effects, or external-effect fencing is claimed.

A crash after handler success or handoff acknowledgment, before source commit,
can replay the record. Duplicate handoffs are possible. Kafka records and group
offsets will retain pending work within topic retention; the in-memory dispatcher
and tracker will be rebuilt from replay.

| Remaining area | Intended increment/boundary |
| --- | --- |
| Failure classification and adaptive limiter policy | L04, library-scoped and validated in unit tests |
| Retry/DLQ publication wiring and runtime rate limits | future worker publisher/consumer integration |
| Kafka consumer rebalance wiring, handler cancellation/drain, and broker-side commit fencing | Future worker-runtime integration |
| Full byte/fetch backpressure, broker recovery and Compose evidence | L06 |
| Real Kafka consumer/producer/commit integration | Runtime work; not established by these unit-level tests |
| Fresh-session learning exercise | L03 handoff prepared; actual resumption still pending |

Basic [GitHub Actions CI](.github/workflows/maven.yml) runs unit tests/package and
retains reports/JARs. A passing CI run is evidence for those checks; required-check
merge enforcement is a separate repository setting.

Learning check: if A1 has stopped but its retry publication fails, can A2 start?
**No. Handler capacity is free, but Job A's gate remains held.**
