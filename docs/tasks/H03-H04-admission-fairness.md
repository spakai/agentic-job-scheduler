# Task <H03-H04>: enforce process-wide admission and fair runnable-head scheduling

Status: in progress

## Scope and plan

- Requirement / issue: close the remaining Spec 003 runtime-hardening gap by enforcing the process-wide active-handler cap and allocating replacement capacity only to runnable job-lane heads.
- Spec 002 acceptance IDs: S02-A02 (single execution per jobId within each topic under stable ownership), S02-A03 (different jobIds may overlap), S02-A05 (main/retry workers remain independent and may overlap for the same jobId), S02-A07 (commit only successful prefixes and replay/duplicate handoff behavior).
- Base commit and branch: current post-merge worker hardening branch.
- Authorized scope and unresolved decisions: keep the Kafka-only architecture and the existing per-assignment dispatcher/worker boundary; no PostgreSQL/Flyway/EDR or scheduling-state topic reintroduction.
- Expected behavior and invariant: no replacement starts beyond the active limit; capacity is only allocated where a runnable job-lane head exists; blocked same-job predecessors or handoff-pending work do not starve a runnable partition.
- Files/components expected to change: `src/main/java/com/example/agenticjobscheduler/runtime/KafkaWorker.java`, `src/main/java/com/example/agenticjobscheduler/execution/ImmediateJobDispatcher.java`.
- Steps and validation approach: update the global capacity distribution to prefer runnable heads, verify retained ownership semantics remain intact, and run the focused dispatcher unit tests plus package compile.
- Non-goals: broad Kafka integration suite, delayed-job semantics, or reintroducing historical worker architecture.

## Progress and decisions

Record dated facts and links. Identify superseded decisions. Do not copy hidden reasoning or secrets; summarize decisions and observable results.

- 2026-10-20: narrowed the remaining fix to the worker/dispatcher admission boundary and fairness policy, matching Spec 003 H03-H04 requirements.
- 2026-10-20: confirmed the root cause was capacity allocation based on `queuedRecords()` instead of `runnableHeads()`, which allowed blocked same-job lanes to receive replacement capacity.
- 2026-10-20: kept the fix within the existing dispatcher/owner structure and avoided broad runtime or architecture changes.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| dispatcher unit behavior | current worktree | `mvn test -Dtest=com.example.agenticjobscheduler.execution.ImmediateJobDispatcherTest -q` | pending | targeted dispatcher assertions |
| artifact compile | current worktree | `mvn -q -DskipTests package` | pending | Maven package build |

## Failure analysis and correction

- Observed failure and reproduction: blocked same-job work could still receive replacement capacity because worker resize logic looked at queued records rather than runnable job-lane heads.
- Classification: requirement / reasoning
- Root cause supported by evidence: `KafkaWorker.resizeDispatchers()` was selecting recipients via `queuedRecords(context) > 0`, while the contract requires fairness based on `runnableHeads(context)`.
- Code, instruction, or tool change: allocate capacity only to partitions with runnable heads, leaving blocked partitions out of the recipient set unless no runnable work exists.
- Rerun and remaining uncertainty: rerun the focused dispatcher suite and package build to confirm the repaired fairness boundary.

## Handoff and review

- Latest verified commit and working-tree changes: working tree contains the H03/H04 patch in `KafkaWorker.resizeDispatchers()` and `ImmediateJobDispatcher.runnableHeads()`.
- Completed steps: root-cause analysis and focused code fix.
- Next concrete step: run the targeted Maven validation checks and verify there are no regressions in dispatcher semantics.
- Open risks/decisions and owner: none beyond keeping the fix strictly within the existing Kafka-only runtime contract.
- Reviewer findings and disposition: pending.
- PR / final outcome: pending.

## Learning check

- What invariant did this task establish? Capacity is allocated based on runnable job heads, not merely queued records, while the per-owner dispatchers still respect the process-wide active-handler cap.
- What failure would invalidate the claim? Replacement starts are still admitted for blocked same-job lanes or the process-wide active cap is bypassed.
- Which artifact proves what actually happened? The focused dispatcher test suite and Maven package compile provide the evidence.
