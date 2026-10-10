# L04: Retry/DLQ publication and adaptive main limiter

Status: in progress

## Scope and plan

- Requirement / issue: implement the L04 retry/DLQ and limiter boundary from Spec 002, acceptance IDs B06–B08 and B13. Keep the work scoped to typed failure classification, retry/DLQ publication decisions, and the adaptive main-worker limit; do not add a Kafka consumer/runtime or a durable state store.
- Spec 002 acceptance IDs: B06, B07, B08, B13
- Base commit and branch: current `feat/l04-retry-dlq-limiter` branch; repository baseline is the clean L03 worktree validated by `mvn test` and `mvn package`.
- Authorized scope and unresolved decisions: continue the library-only path. The retry/DLQ handoff remains a publication decision, not an execution runner. Adaptive limiter state is configured in memory and will later be wired into the process-wide worker budget.
- Expected behavior and invariant: a retryable failure before the attempt limit publishes a retry handoff and keeps the source incomplete until broker acknowledgment; permanent or exhausted failures publish to DLQ; the main limiter decreases on slow/unstable windows and only increases when backlog is present and signals remain healthy. No handler reruns after a failed handoff publish.
- Files/components expected to change: `src/main/java/com/example/agenticjobscheduler/execution/FailureClassifier.java`, `src/main/java/com/example/agenticjobscheduler/execution/AdaptiveLimiter.java`, `src/test/java/com/example/agenticjobscheduler/execution/L04RetryAndLimiterTest.java`, and task/evidence docs for L04.
- Steps and validation approach: add deterministic failure classification and adaptive-limit logic, add focused unit tests for retry/DLQ decisions and limit adjustment, then run `mvn test` and capture the evidence.
- Non-goals: Kafka broker integration, live retry pod rate limiting, real consumer rebalance handling, or production handler execution.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local Maven/JUnit only; this is a library-spec change and does not deploy or modify Kafka infrastructure.
- Read/write operations needed: read spec and current L03 dispatcher contract; write new execution classes and tests.
- Environment constraints and recovery path: Java 21 and Maven are required; if the build fails, rerun the narrow test command and inspect the exact cause before changing architecture.
- Evidence of enforced restrictions, or explicitly unverified restrictions: no Kafka broker or Docker-backed integration is available in this increment, so only unit-level verification is claimed.

## Progress and decisions

- 2026-10-10: started the L04 increment from the verified L03 baseline; kept the spec boundary to failure classification, retry/DLQ publication semantics, and adaptive limits without reintroducing the historical scheduling/state-store architecture.
- Decision: classify with `FailureClassifier.FailureType` for `RETRYABLE`, `EXHAUSTED`, and `PERMANENT` so the future publisher can decide a retry or DLQ handoff without guessing from exception text.
- Decision: configure a bounded `AdaptiveLimiter` with min 1 / max 128 / initial 16 and deterministic policy signals aligned to the spec's 1-second latency and 10% transient failure thresholds.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| L04 classification and limiter tests | local working tree | `mvn -B -ntp test` | not run yet | pending |
| Library package still passes | local working tree | `mvn -B -ntp package` | not run yet | pending |

## Failure analysis and correction

- Observed failure and reproduction: not applicable yet; the initial L04 work is in progress and intentionally minimal.
- Classification: n/a
- Root cause supported by evidence: n/a
- Code, instruction, or tool change: keep the scope to library-level L04 decisions before any Kafka runtime wiring.
- Rerun and remaining uncertainty: run the focused Maven suite after adding L04 tests and review whether the limit rule matches the spec without inventing production behavior.

## Handoff and review

- Latest verified commit and working-tree changes: this is the start of L04; no commit yet.
- Completed steps: branch creation, spec review, initial L04 work item, and the library-level failure classification/limiter scaffolding.
- Next concrete step: add the focused L04 JUnit tests and run `mvn test` to verify the new behaviour.
- Open risks/decisions and owner: the adaptive limiter remains a deterministic policy utility; full Kafka integration and producer acknowledgment logic are deferred to later tasks.
- Reviewer findings and disposition: pending
- PR / final outcome: none yet

## Learning check

- What invariant did this task establish? Retryable failures lead to retry publication before source completion, exhausted/permanent failures go to DLQ, and the main limit falls when latency or instability indicates overload, then rises only under healthy backlog conditions.
- What failure would invalidate the claim? A limiter that increases while overloaded or a classifier that labels an exhausted job as retryable.
- Which artifact proves what actually happened? The L04 unit tests and the `mvn test` output in the project evidence directory.
