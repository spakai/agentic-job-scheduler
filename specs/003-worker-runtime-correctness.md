# Spec 003: Worker Runtime Correctness Hardening

Status: **Draft — requirements and acceptance criteria proposed; implementation not authorized by this spec draft alone**

Project: `agentic-job-scheduler`
Date: 2026-10-11
Depends on: [Spec 002](002-kafka-job-scheduling.md), merged runtime baseline at `c437786`
Motivation: findings from a read-only review of the merged L06 worker runtime

## 1. Purpose

Close correctness and bounded-resource gaps identified after L06 runtime
acceptance. This increment hardens revocation, dispatch admission, Kafka
configuration, retry queues, limiter signals, and compatibility without changing
the selected Kafka-only architecture.

Spec 002 remains authoritative for job behavior. This spec adds implementation
requirements and tests; it does not supersede Spec 002 or introduce database
admission. Performance/scale experiments belong in Spec 004. Broad fault and
chaos campaigns belong in Spec 005.

## 2. Scope and boundaries

### In scope

- Release local processing-window accounting safely as revoked work is retired.
- Prevent queued, rate-delayed handler starts after ownership loss or shutdown.
- Enforce process-wide handler limits, including replacement starts after a
  capacity reduction.
- Allocate available capacity to runnable work fairly across partitions.
- Fail worker startup on existing topic partition-count mismatches.
- Preserve the distinct offset policy for new consumer groups and established
  groups whose committed offsets are out of range.
- Bound latency/metric memory and forward timeout signals to the adaptive limiter.
- Preserve `JobExecution` value semantics or explicitly version the public API.
- Reconcile `arch.md` and tests with implemented behavior.

### Non-goals

- Do not add PostgreSQL, an EDR adapter, a job inbox, durable admission, or a
  scheduling/state topic. ADR 4 Option C remains an unimplemented alternative.
- Do not claim global job ordering, cross-topic exclusion, exactly-once effects,
  durable deduplication, or external-effect fencing.
- Do not change the accepted retry topic protocol, attempt semantics, or L06
  completion/acknowledgment rules.
- Do not perform performance benchmarking, target 20 pods/40 partitions, or run
  broad chaos campaigns; those are candidates for Specs 004 and 005.

## 3. Invariants

1. A revoked owner never commits or dispatches new work for its revoked
   assignment.
2. Local resources reserved for a revoked assignment are released only when
   their local work has been retired: work never started may be discarded for
   replay; work already running remains accounted and holds its process permit
   until its handler future settles. Neither case marks the source complete.
3. A queued retry-rate request is not a handler start. Before invoking a queued
   operation, the worker revalidates its owner generation, shutdown state, and
   process permit. Invalidated requests do not run or create a handoff.
4. Every actual handler start consumes one process-wide permit. A lowered limit
   does not cancel work already running; it prevents replacement starts until
   active usage is below the new limit.
5. Capacity is assigned to runnable job-lane heads, not merely to partitions
   containing any queued record. Independent runnable partitions make progress
   while another partition is blocked on a same-job predecessor or handoff.
6. Startup never changes the partition count of an existing application topic.
   New groups start from the beginning as Spec 002 requires; an established
   group with an invalid/out-of-range committed offset fails visibly rather
   than silently resetting and skipping expired work.
7. Runtime metrics and samples remain bounded for the lifetime of both main and
   retry workers.
8. A timeout signal is visible to the main adaptive limiter. A timeout request
   does not release a handler permit, job gate, processing-window reservation,
   or source offset before the handler future confirms that execution stopped.
9. Reconstructed `JobExecution` values retain documented equality/hash behavior
   for their source, job, and attempt, unless a versioned API change is proposed.

## 4. Requirements and acceptance criteria

| ID | Requirement | Acceptance evidence |
| --- | --- | --- |
| H01 | Retire revoked assignment reservations without completing source records. | Test a window near its high watermark, then revoke partitions containing queued, running, and handoff-pending records. Retire never-started queued work for replay; keep running handlers accounted until their futures settle; stop retrying handoffs for the revoked generation and retain each outstanding send until its future settles or the producer conclusively fails it. Release local reservations only when that work is retired, never complete/commit the revoked source, and permit duplicate handoffs if publication may have succeeded. Repeat rebalances and prove intake resumes after local work is retired. |
| H02 | Invalidate rate-delayed starts on revocation and shutdown. | Hold retry submissions behind the rate gate, revoke their owner or close the worker, then advance the timer. Assert no handler starts from the invalidated assignment, no handoff is manufactured, and a replacement owner can replay. Assert already-running handlers retain permits until their futures settle. |
| H03 | Enforce the process-wide active-handler limit on every actual start. | Under sustained backlog, lower main capacity from 4 to 2 while 4 handlers are active. Existing handlers may finish, but no replacement starts above the new cap. Assert the active count converges to 2 or lower without cancelling running work. Cover retry concurrency separately. |
| H04 | Schedule fairly using runnable job heads. | Keep one partition's head waiting on an unacknowledged handoff while another partition has runnable work. At concurrency 1, prove the runnable partition progresses. Repeated resize/admission cycles must rotate remainder capacity rather than strand a partition. |
| H05 | Enforce fixed topic partition counts. | Startup creates missing topics with configured counts. If an existing main, retry, or DLQ topic has a mismatched count, startup fails with the expected/actual counts and does not call topic expansion. |
| H06 | Distinguish first-use offset initialization from invalid offsets for established groups. | A genuinely new group consumes from earliest. An established group's expired/out-of-range offset fails visibly and never starts processing from a silently reset position. Test both paths against Kafka. |
| H07 | Bound runtime metrics memory. | Sustained retry completions do not grow a per-attempt latency list without bound. Verify bounded storage or that retry workers do not collect main-limiter latency samples; prove main sample windows clear or remain capped. |
| H08 | Feed timeout signals into adaptive-main decisions. | In a qualifying sample window, include a timeout while aggregate p95 and failure rate remain below their thresholds. Assert the limiter decreases once for that window. Also assert an unconfirmed timeout keeps work and permits accounted. |
| H09 | Preserve `JobExecution` value semantics. | Compare independently reconstructed values with equal source/job/attempt: equality and hash codes remain compatible with the pre-cancellation contract. If record-pattern/source compatibility is intentionally changed, document the migration/version boundary instead. |
| H10 | Keep architecture docs evidence-aligned. | `arch.md` describes live rebalance, process permits, cancellation acknowledgment, offset reset, topic partition validation, retry metrics, and limiter timeout coverage accurately. Every stated coverage claim links to a test/manual artifact; remaining guarantees and non-guarantees are explicit. |

## 5. Proposed implementation order

1. **Assignment lifecycle:** H01 and H02 together, because both concern ownership
   generations, delayed starts, stale callbacks, and reservation retirement.
2. **Admission control:** H03 and H04 together, using one authoritative process
   permit/admission boundary and runnable-head fairness.
3. **Kafka startup safety:** H05 and H06; topic layout and consumer-offset policy
   are independent startup checks but share fail-fast configuration behavior.
4. **Limiter and bounded instrumentation:** H07 and H08.
5. **API compatibility and documentation:** H09, then H10 after all behavior and
   tests settle.

Each phase should be independently reviewable and use controlled handler futures
and broker-backed tests. Preserve user changes and do not combine this work with
Spec 004 or Spec 005 experiments.

## 6. Verification

- Focused unit tests for admission scheduling, assignment-generation invalidation,
  topic configuration, offset initialization policy, metric bounds, limiter
  timeout signals, and `JobExecution` equality/hash behavior.
- Testcontainers Kafka cases for real revocation/reassignment, rate-delayed retry
  invalidation, safe-prefix commits, topic mismatch startup failure, and
  established-group out-of-range failure.
- Negative cases must demonstrate that no source completion or external handler
  start is fabricated when ownership/cancellation is unconfirmed.
- Run `mvn -B -ntp clean verify` after the acceptance slices are integrated.
  Record each focused command, full-suite result, and CI result separately.
- Update [arch.md](../arch.md), the [L06 task record](../docs/tasks/L06-backpressure-recovery.md),
  and this spec only after matching tests exist.

## 7. Decisions intentionally deferred

- Request-only database admission and EDR-backed durable execution state remain
  outside this spec. Adopting ADR 4 Option C would require a separate architecture
  decision specifying unique admission keys, EDR claim/repair guarantees,
  concurrency/lease ownership, and recovery after Kafka offsets have committed.
- Cluster-wide concurrency budgets, cross-topic fencing, partition-count
  migrations, performance targets, and chaos fault matrices require separate
  requirements and validation in later increments.
