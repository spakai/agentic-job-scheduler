# Spec 007: Adaptive handling of external-service pressure

Status: **Draft — requirements and acceptance criteria proposed; implementation
not authorized by this spec draft alone**

Project: `agentic-job-scheduler`  
Date: 2026-11-26

Depends on: [Spec 002](002-kafka-job-scheduling.md),
[Spec 003](003-worker-runtime-correctness.md)  
Supersedes for adaptive external-pressure behavior: Spec 002 section 7 and the
limiter-specific parts of Spec 003 H07/H08 where those requirements are
ambiguous

## 1. Purpose

Make the main worker reduce execution pressure when job handlers encounter an
overloaded or unavailable REST endpoint or database. An HTTP `503 Service
Unavailable` and a confirmed REST or database deadline are explicit signs that
starting more work may worsen the affected dependency. They must not wait for a
minimum number of completed attempts and must not depend on exception-message
matching.

This spec updates adaptive limiter inputs and recovery only. It does not change
Kafka ownership, completed-prefix commits, retry publication, attempt budgets,
or the replay and idempotency contract.

## 2. Scope and boundaries

### In scope

- A typed, machine-readable external-pressure signal from job-handler
  integrations to the worker.
- Normalization of REST `503` and REST/DB timeout outcomes into that signal.
- Immediate latching of pressure within an adaptive evaluation window.
- Bounded reduction and conservative recovery of the process-wide main-worker
  concurrency limit.
- Metrics and structured logs that identify dependency and pressure category
  without recording payloads or connection details.
- Unit and controlled worker tests for signal classification, minimum-sample
  behavior, bounds, sustained pressure, recovery, and ambiguous timeout
  accounting.

### Non-goals

- No database admission model, scheduler state, jobs table, inbox/outbox, or
  durable limiter state. A database discussed here is an external service called
  by a job handler, not scheduler persistence.
- No inspection or classification by HTTP text, SQL text, exception message, or
  stack-trace content.
- No automatic treatment of arbitrary `4xx`, `5xx`, connection failures, or
  application exceptions as pressure unless the handler integration maps that
  outcome explicitly.
- No global or cluster-wide limit, durable learning, external-effect fencing, or
  cross-topic exclusion.
- No separate adaptive policy for retry workers. Retry workers retain their
  independent configured concurrency and token-bucket rate. Main and retry
  execution may still overlap for the same `jobId`.
- No change to attempt consumption: a failed handler attempt still decrements
  `remainingAttempts` once and follows the Spec 002 retry/DLQ handoff rules.

## 3. Signal contract

Handler integrations publish a typed pressure event when they observe one of
these outcomes:

| Category | Qualifying outcome |
| --- | --- |
| `UNAVAILABLE` | A REST call receives `503`; a database client explicitly reports overload, server unavailability, or admission rejection through a typed outcome selected by that client adapter |
| `TIMEOUT` | A REST request has a connect, response-header, read, or total-request deadline failure; a DB connection-acquisition, statement, transaction, or driver operation deadline expires |

The signal is attached to the handler invocation and contains only:

- pressure category: `UNAVAILABLE` or `TIMEOUT`;
- dependency class: `REST` or `DATABASE`;
- registered logical dependency name;
- monotonic observation time and invocation identity.

The logical dependency name must be a configured allow-listed label. It must not
contain a URL, host, database name, query, request, response, credentials, or
payload.

The scheduler cannot reliably infer a `503` or timeout from arbitrary
`Throwable` values. Each REST/DB adapter is responsible for mapping its client's
typed outcomes to this contract. Existing generic handler failures continue to
be counted by Spec 002's aggregate failure-rate signal, but are not
automatically external-pressure signals.

An invocation may report multiple lower-level timeout or `503` events. The
limiter counts the invocation's signal once per category and dependency for
accounting, and pressure remains a boolean latch for the adaptive decision.
Duplicate reports must never cause multiple reductions in one window.

## 4. Adaptive policy

The main limiter remains process-wide, with the Spec 002 bounds: initial 16,
minimum 1, maximum 128, unless configured. One permit is consumed by each
actually started handler, and a decrease never cancels work already in flight.

Evaluate non-overlapping five-second monotonic-time windows.

1. An external `UNAVAILABLE` or `TIMEOUT` event latches `externalPressure=true`
   until the next adaptive decision. The first such event is eligible for one
   decrease regardless of `completedAttempts`, aggregate latency, or transient
   failure rate.
2. At most one decrease occurs per window. A decrease halves the current limit,
   rounds down, and clamps at the configured minimum.
3. Another decrease requires a later window. Repeated events in one window do
   not repeatedly halve the limit.
4. Expiry of the worker's handler deadline is also a pressure latch. Recording
   the timeout request does not release a permit, jobId gate, processing-window
   reservation, or source offset. Those remain held until the handler future
   actually settles as required by the handler contract.
5. If external pressure is absent, ordinary transient failure rate at least 10%,
   p95 handler latency above the configured target (default one second), or an
   explicit throttle signal still triggers the Spec 002 decrease. These existing
   triggers retain the same one-decrease-per-window rule.
6. With fewer than 20 completed attempts, no increase occurs and no
   non-explicit signal causes a decrease. Explicit external pressure, explicit
   throttling, or a confirmed worker timeout is sufficient by itself.
7. Growth requires backlog plus p95 latency at or below target and no pressure,
   throttle, timeout, or transient-failure trigger. After a decrease, require two
   consecutive qualifying healthy windows before the limit may increase. Then
   add one permit per healthy window, up to the maximum. The successful-window
   counter resets on any decrease.
8. After a pressure window, an already-decreased limiter may remain below its
   prior value through the two healthy recovery windows. This hysteresis is
   intentional and prevents a saturated dependency from being immediately
   flooded again.

Rest/DB pressure affects the shared main limit, not just the current
partition or handler type. This conservative behavior is deliberate: Spec 002
currently defines one process-wide main budget, and this spec does not invent
dependency-specific concurrency accounting. A later spec may add isolated
dependency pools if workload separation requires them.

## 5. Timeout and ambiguity semantics

An external-service timeout is an overload signal but not proof that the
external operation did not happen. The handler must retain the stable
`jobId`/`executionId` idempotency contract and must not assume an idempotent
rollback merely because a client deadline fired.

For a handler-level deadline:

- the worker may immediately latch adaptive pressure;
- cancellation is a request, not acknowledgment;
- the permit and all local accounting remain held until the handler future
  settles;
- if cancellation is never acknowledged, the record stays incomplete and the
  job gate remains held;
- no retry/DLQ handoff is produced until the handler future confirms the failure
  and execution has stopped.

For an operation-level REST or database deadline that the handler catches and
converts into a normal future failure, the same rules apply to the handler
future. The adapter records the timeout signal before propagating or wrapping
the failure. Wrappers must preserve the signal without requiring downstream
code to inspect the root cause text.

## 6. Observability

Expose bounded counters and gauges sufficient to audit each decision:

| Observation | Required fields |
| --- | --- |
| pressure event | dependency class, logical dependency, category |
| adaptive decision | window start/end, `limitBefore`, `limitAfter`, reason, completed samples, p95 latency, failure rate, backlog, pressure/throttle/timeout flags |
| recovery | consecutive healthy windows and whether growth was eligible |

Counters must include events suppressed as duplicates within an invocation and
reductions skipped because the configured minimum was already reached. Logs and
metrics must not label an ordinary permanent job error as external pressure
without a typed signal.

Metrics cardinality is bounded by registered dependency names and the two
categories. The adaptive implementation must not retain an unbounded invocation
or latency history. Existing rolling-window storage remains bounded.

## 7. Configuration

The following settings are independently bounded and role-local:

- minimum, initial, and maximum main concurrency;
- five-second evaluation period;
- 20-completion sample floor for aggregate statistical signals;
- one-second target p95 latency;
- 10% transient failure threshold;
- two healthy windows required after a reduction.

Reject invalid values at startup. Configuration changes do not retroactively
cancel running work. Dependency names are registered explicitly; unregistered
names are rejected at adapter construction rather than silently added to
metric labels.

## 8. Acceptance criteria

| ID | Requirement | Acceptance evidence |
| --- | --- | --- |
| A01 | REST `503` becomes typed pressure. | Given a controlled REST client outcome with status 503, assert one `UNAVAILABLE`/`REST` event is attached to the invocation and the limiter is eligible to decrease with zero completed samples. A 502 and a 404 do not produce this signal. |
| A02 | REST timeouts become typed pressure. | Cover connect, read, and total-request deadline outcomes. Each maps to `TIMEOUT`/`REST` and remains visible through supported failure wrapping. |
| A03 | Database timeouts become typed pressure. | Controlled driver outcomes for connection-pool acquisition and statement/query deadline map to `TIMEOUT`/`DATABASE`. An unrelated SQL constraint or validation error does not. |
| A04 | Explicit pressure bypasses the sample floor. | With limit 16, zero completed attempts, healthy latency, zero transient failure rate, and a `503` or timeout latch, the first adaptive decision produces 8. No more than one decrease occurs in that window. |
| A05 | Sustained pressure is bounded. | Across separate windows, repeated pressure decreases 16 to 8, 4, 2, and 1, then remains 1. At no point does a single window reduce more than once or the limit fall below the configured minimum. |
| A06 | Recovery has hysteresis. | After a reduction and with backlog plus healthy aggregate signals, the first qualifying healthy window holds the reduced limit, the second increases it by one, and later healthy windows increase one at a time to the configured maximum. A new pressure event resets healthy-window recovery. |
| A07 | Duplicate reports do not amplify pressure. | Multiple `503` and timeout reports from one invocation, including wrapped reports, yield at most the expected bounded event counters and one limiter reduction for that window. |
| A08 | Lowered capacity stops replacement starts. | With four active handlers at limit 4, lower the limit to 2 because of one external `503` or timeout. Existing handlers finish, but no replacement starts while active use is two or more. |
| A09 | Timeout detection does not release accounting. | A worker or external-operation deadline latches pressure, but an unacknowledged handler future retains its permit, job gate, processing-window reservation, and incomplete source record. A confirmed future failure permits exactly one retry/DLQ handoff according to the remaining attempt budget. |
| A10 | Main and retry policies remain independent. | External pressure observed by a retry worker does not resize the main worker's limit, and retry concurrency/rate remain configured independently. Controlled main/retry overlap for a `jobId` remains permitted. |
| A11 | Existing commit and replay guarantees remain unchanged. | A `503` or timeout cannot cause offset completion before handler settlement and acknowledged handoff publication. Replay may produce duplicate external operations, and tests use stable idempotency keys without claiming effect fencing. |
| A12 | Metrics and logs stay bounded and sanitized. | Repeated events prove no growth in retained signal history and no payload, URL, host, query, credential, response body, or exception message in pressure metrics/logs. Registered dependency label cardinality remains bounded. |
| A13 | Ordinary failures do not masquerade as dependency pressure. | A permanent job error, arbitrary `RuntimeException("503")`, HTTP 500 without a reviewed adapter mapping, and a non-timeout failure do not directly latch external pressure. |

## 9. Verification

Run focused unit tests for REST/DB classification and adaptive policy before
worker integration. Use controlled futures, monotonic test time, and real worker
admission barriers to prove A04–A11 without relying on private limiter fields.

Run the repository's `mvn test` for unit coverage and `mvn package` for the
library artifact. A Testcontainers-backed worker suite is required for the
replacement-start, timeout-accounting, retry/DLQ, and offset guarantees. Kafka
integration is not represented as covered by unit tests alone.

Negative cases must prove that generic exception text, unrelated HTTP statuses,
and unrelated database errors cannot lower capacity, and that an unconfirmed
handler timeout does not release any local resource.

## 10. Implementation decisions intentionally deferred

- Whether REST and database adapters expose the signal by a typed exception, an
  `ExecutionOutcome`, or a callback is an API design decision for the
  implementation increment. The contract and test-observable behavior above are
  mandatory.
- Dependency-specific concurrency pools, circuit breakers, request coalescing,
  or per-service budgets require separate capacity and fairness requirements.
- Automatic recovery driven by service-provided `Retry-After` is not defined;
  ignoring or honoring that hint must not be inferred from a `503`.
- Cluster-wide adaptation and sharing pressure state across pods are outside
  this spec.

