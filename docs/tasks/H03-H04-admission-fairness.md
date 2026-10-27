# Task <H03-H04>: enforce process-wide admission and fair runnable-head scheduling

Status: complete

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
- 2026-10-21: validated commit `f4588e6`; the focused dispatcher suite passed all 21 tests, and the package build completed successfully.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| dispatcher unit behavior | `f4588e6` | `mvn test -Dtest=com.example.agenticjobscheduler.execution.ImmediateJobDispatcherTest -q` | passed; 21 tests, 0 failures, 0 errors, 0 skipped | `target/surefire-reports/com.example.agenticjobscheduler.execution.ImmediateJobDispatcherTest.txt` |
| artifact compile | `f4588e6` | `mvn -q -DskipTests package` | passed; built `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` | `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` |
| whitespace | `f4588e6` | `git diff --check` | passed | no whitespace errors |

## Failure analysis and correction

- Observed failure and reproduction: blocked same-job work could still receive replacement capacity because worker resize logic looked at queued records rather than runnable job-lane heads.
- Classification: requirement / reasoning
- Root cause supported by evidence: `KafkaWorker.resizeDispatchers()` was selecting recipients via `queuedRecords(context) > 0`, while the contract requires fairness based on `runnableHeads(context)`.
- Code, instruction, or tool change: allocate capacity only to partitions with runnable heads, leaving blocked partitions out of the recipient set unless no runnable work exists.
- Rerun and remaining uncertainty: focused dispatcher tests and package build passed on `f4588e6`. These checks validate unit behavior and compilation; they do not provide Kafka integration evidence.

## Handoff and review

- Latest verified commit and working-tree changes: H03/H04 implementation was validated on `f4588e6`; its validation record remains uncommitted alongside the separate H05/H06 work now underway.
- Completed steps: root-cause analysis, focused code fix, dispatcher unit validation, package build, and whitespace check.
- Next concrete step: none for H03/H04; Kafka integration scenarios remain outside this task's validation scope.
- Open risks/decisions and owner: Kafka integration behavior was not exercised by these checks.
- Reviewer findings and disposition: pending.
- PR / final outcome: pending.

## Learning check

- What invariant did this task establish? Capacity is allocated based on runnable job heads, not merely queued records, while the per-owner dispatchers still respect the process-wide active-handler cap.
- What failure would invalidate the claim? Replacement starts are still admitted for blocked same-job lanes or the process-wide active cap is bypassed.
- Which artifact proves what actually happened? The focused dispatcher test suite and Maven package compile provide the evidence.
