# S007: Implement adaptive external-service pressure handling

Status: planned

## Scope and plan

- Requirement / issue: implement [Spec 007](../../specs/007-adaptive-external-service-pressure.md), acceptance IDs A01-A13. Add typed REST/DB `503`, unavailable, and timeout pressure signals to the main-worker adaptive limiter without changing Kafka ownership or completion rules.
- Spec 002 acceptance IDs: B06-B09, B13, B16, B19; Spec 007 acceptance IDs: A01-A13.
- Base commit and branch: implementation starts after this documentation PR is merged, on an implementation feature branch created from the merged default-branch commit.
- Authorized scope and unresolved decisions: implement the handler-adapter signal contract, limiter policy, worker wiring, configuration, bounded observability, and focused tests. Resolve the Spec 007 deferred API choice before dependent runtime work; the recommended shape is a reporter supplied to the handler because asynchronous invocations can report structured signals without changing `JobExecution` equality or relying on exception unwrapping.
- Expected behavior and invariant: explicit typed REST/DB pressure can halve the shared per-process main limit once per window with zero completed samples. A reduction never cancels running work. Permits, jobId gates, retained-window reservations, and offsets remain accounted until handler settlement and the required acknowledged retry/DLQ handoff. Retry capacity remains independent, and recovery requires two healthy windows before increasing one permit at a time.
- Files/components expected to change: `execution.JobExecution`/`JobHandler` contracts only if the selected reporter API requires them, new external-pressure types, `execution.AdaptiveLimiter`, `runtime.WorkerConfig`, `runtime.KafkaWorker`, worker metrics/logging, REST/DB adapter support where an in-repository integration exists, focused execution/runtime tests, and Spec 007/arc42 evidence updates.
- Steps and validation approach: implement in independently reviewable stages below; run focused tests after each stage, then `mvn test`, `mvn package`, and a Docker-backed `mvn verify` for worker/Kafka acceptance.
- Non-goals: scheduler database, global/shared limiter state, dependency-specific pools, durable pressure state, Retry-After interpretation, REST/DB client implementation beyond typed adapter boundaries, retry-worker adaptive resizing, external-effect fencing, or exactly-once claims.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local Java 21/Maven and JUnit; Testcontainers and Docker only for broker-backed worker acceptance; Git/GitHub for branch, commit, and review evidence.
- Read/write operations needed: execution contracts and tests, limiter/configuration/runtime classes, bounded worker metrics, task evidence, and directly affected architecture/spec statements.
- Environment constraints and recovery path: if Docker is unavailable, record that Testcontainers acceptance was not run and do not describe it as passing. Recover from an isolated failure with a focused test command before rerunning the full required command.
- Evidence of enforced restrictions, or explicitly unverified restrictions: no scheduler-state database or external REST/DB deployment will be added. Actual external-client mapping is limited to clients implemented in this repository; adapter behavior for unsupported clients remains an integration requirement.

## Implementation stages

### Stage 1: signal contract and false-positive boundaries

Acceptance: A01-A03, A07, A13.

1. Define a typed pressure category (`UNAVAILABLE`, `TIMEOUT`), dependency class (`REST`, `DATABASE`), and registered logical dependency identifier.
2. Provide a per-invocation reporter with bounded de-duplication by category and dependency. Preserve existing handler compatibility where possible and avoid changing `JobExecution` source/job/attempt equality semantics.
3. Add reviewed adapter mappings:
   - REST status `503` -> `UNAVAILABLE/REST`;
   - typed REST connect, header, read, and total deadline expiry -> `TIMEOUT/REST`;
   - typed database availability/admission rejection -> `UNAVAILABLE/DATABASE`;
   - typed pool-acquisition, statement, transaction, or driver deadline expiry -> `TIMEOUT/DATABASE`.
4. Do not infer pressure from `Throwable.getMessage()`, stack traces, response bodies, arbitrary `5xx`, HTTP `404`, SQL text, or unrelated database errors.
5. Unit-test positive mappings, supported wrapping, per-invocation duplicates, unregistered dependency labels, and negative false-positive cases.

Exit gate: A01-A03/A07/A13 focused unit tests pass, including arbitrary `RuntimeException("503")` and HTTP 500/constraint-error negatives.

### Stage 2: bounded limiter policy

Acceptance: A04-A07, A12.

1. Extend `AdaptiveLimiter` to accept an explicit external-pressure latch and worker-timeout latch without changing the existing latency/failure aggregate inputs.
2. In one non-overlapping five-second monotonic window:
   - halve once on explicit pressure even with zero completed attempts;
   - clamp to the configured minimum;
   - suppress further decreases in the same window;
   - preserve the existing latency, transient-failure, and throttle triggers.
3. Track consecutive qualifying healthy windows after any decrease. Hold for the first, increase once on the second, then add one permit per later healthy window up to the maximum. Reset on another decrease.
4. Reject invalid bounds, thresholds, window lengths, sample floors, and recovery-window values at construction.
5. Keep statistical and pressure state bounded to the active window.

Exit gate: deterministic tests prove 16 -> 8 -> 4 -> 2 -> 1, no duplicate in-window decrease, no below-minimum value, no increase during recovery pressure, and eventual +1 recovery to the maximum.

### Stage 3: worker ownership and timeout wiring

Acceptance: A08-A11.

1. Attach the selected reporter to each invocation and marshal emitted signals to the consumer context.
2. Latch worker handler-deadline expiry immediately when the timeout requests cancellation; do not release execution accounting at that point.
3. Apply a lowered limit through the existing authoritative process-wide capacity/dispatcher boundary. Existing handlers continue, while replacement starts remain blocked until active usage is below the new limit.
4. Keep the permit, jobId gate, processing-window reservation, and source record incomplete until the handler future settles. A confirmed failure follows the existing remaining-attempt retry/DLQ path.
5. Confirm that pressure in a retry process cannot resize a main process and does not alter retry concurrency/rate.

Exit gate: controlled-future worker tests prove A08-A09; broker-backed cases prove no premature offset commit, one failure-budget decrement, acknowledged retry/DLQ handoff ordering, and preserved main/retry overlap.

### Stage 4: configuration and observability

Acceptance: A12.

1. Add validated role-local main-limiter configuration for minimum/initial/maximum concurrency, evaluation window, target p95, sample floor, failure threshold, and healthy recovery windows.
2. Keep default behavior aligned with Spec 002 and 007: bounds 1/16/128, five-second windows, 20 samples, one-second p95, 10% failures, and two healthy recovery windows.
3. Export bounded counters by registered logical dependency and category for emitted and duplicate-suppressed events.
4. Export each adaptive decision's reason, limit before/after, sample count, p95, failure rate, backlog, pressure/throttle/timeout flags, recovery count, and minimum-clamp outcome.
5. Ensure labels and logs omit URLs, hosts, payloads, request/response values, SQL/query text, credentials, exception messages, and stack traces.

Exit gate: repeated-event tests demonstrate bounded retained state and fixed metric-label cardinality; sanitation assertions prove prohibited values cannot appear in metrics/log output.

### Stage 5: documentation and evidence

Acceptance: A01-A13.

1. Record focused unit, worker, Testcontainers, package, and review outcomes separately in this task record and an evidence file.
2. Update Spec 007's status and acceptance table only as matching evidence exists. Do not change proposed items to implemented based on unit tests alone.
3. Update `arch.md` from "proposed" to "implemented" only for verified surfaces; retain unverified external-client, scaling, or deployment boundaries explicitly.
4. Update handler-author and operator documentation with reporter use, timeout ambiguity, retry/DLQ behavior, bounded labels, and main/retry independence.

Exit gate: every A01-A13 row is backed by a named test/evidence artifact or explicitly remains unverified; no architecture claim exceeds the executed scope.

## Progress and decisions

- 2026-11-26: Spec 007 and arc42 updates added on PR #46. No implementation is included in that PR.
- Planned decision: prefer a typed per-invocation reporter as the default signal API; preserve exception wrapping only as a transport concern, never as a text-parsing mechanism.
- Pending implementation decision: exact Java API shape for preserving compatibility with current `JobHandler` lambdas and `ExecutionVerticlePool` dispatch.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| A01-A03/A07/A13 classification and negative cases | pending | focused Maven selector for external-pressure unit tests | not run | pending |
| A04-A07 limiter bounds, duplicate behavior, and hysteresis | pending | `mvn -B -ntp '-Dtest=AdaptiveLimiterTest' test` | not run | pending |
| A08-A09 worker replacement starts and timeout accounting | pending | focused `KafkaWorkerIT` selectors | not run | pending |
| A10-A11 main/retry isolation, handoff, and offset safety | pending | Docker/Testcontainers `mvn verify` | not run | pending |
| A12 bounded, sanitized metrics | pending | focused metrics/log tests | not run | pending |
| Unit regression suite | pending | `mvn test` | not run | pending |
| Library artifact | pending | `mvn package` | not run | pending |
| Broker-backed full verification | pending | `mvn verify` with Docker | not run | pending |

Each planned command remains distinct from an executed result. Absence of Docker,
an unsupported client, a skipped test, or a failed check must be recorded
without conversion into a passing claim.

## Failure analysis and correction

- Observed failure and reproduction: none; planning only.
- Classification: n/a.
- Root cause supported by evidence: n/a.
- Code, instruction, or tool change: if a stage fails, classify it as requirement, API compatibility, worker accounting, broker integration, or environment; correct the smallest responsible boundary and rerun the failing acceptance first.
- Rerun and remaining uncertainty: no runs performed. REST/DB adapter API compatibility and the actual in-repository external-client surface remain unresolved until implementation begins.

## Handoff and review

- Latest verified commit and working-tree changes: `14628da` documents the proposed policy on `docs/spec-007-adaptive-pressure`; implementation is not present.
- Completed steps: Spec 007 architecture requirements and this staged implementation plan.
- Next concrete step: merge the documentation PR, create an implementation branch, then start Stage 1 with the handler reporter and typed classification tests.
- Open risks/decisions and owner: implementation owner must select and review the handler/reporter API before runtime wiring; operator/handler authors must confirm the registered dependency-name vocabulary.
- Reviewer findings and disposition: PR review pending.
- PR / final outcome: Spec 007 documentation PR #46; implementation PR not created.

## Learning check

- What invariant should this task establish? Typed external pressure reduces main-worker capacity without releasing any execution or Kafka accounting before confirmed settlement.
- What failure would invalidate the claim? A generic exception text lowering capacity, repeated signals causing multiple reductions in one window, a replacement start exceeding the reduced limit, or a timeout permitting source completion/handoff before settlement.
- Which artifact should prove what actually happened? A01-A13 focused unit and broker-backed results, recorded command output, and updated architecture evidence.
