# Task <H05-H06>: fail safely on Kafka topic and offset configuration errors

Status: complete

## Scope and plan

- Requirement / issue: implement and verify Spec 003 H05-H06 Kafka startup safety.
- Spec 002 acceptance IDs: Spec 002 §3 (fixed topic partition counts and earliest initialization for new groups) and B09 (offset replay behavior); Spec 003 H05 and H06.
- Base commit and branch: `f4588e6`, `docs/h03-h06-arch-hardening`.
- Authorized scope and unresolved decisions: enforce existing topic partition counts without resizing; initialize genuinely new groups from earliest; established groups with invalid offsets and failures to determine group state must fail visibly. No architecture changes.
- Expected behavior and invariant: startup never changes an existing topic's partition count, and uncertainty about consumer-group offset state never causes silent earliest-offset reset.
- Files/components expected to change: `KafkaWorker`, `TopicProvisioner`, Kafka-backed runtime tests, and this task record.
- Steps and validation approach: add H05/H06 broker-backed cases; correct offset-state error handling; execute focused integration tests and Maven verification.
- Non-goals: migration of existing topics, general rebalance/chaos testing, or unrelated worker changes.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: local repository tools and Maven/Testcontainers against the local Docker daemon.
- Read/write operations needed: read and edit scoped Java sources, tests, and this task record; run Maven tests/builds.
- Environment constraints and recovery path: Docker and Java 21 are available; if broker startup or integration checks fail for environmental reasons, record that separately rather than claiming a pass.
- Evidence of enforced restrictions, or explicitly unverified restrictions: no external services or credentials are required.

## Progress and decisions

- 2026-10-21: started H05-H06 after confirming the worker startup code probes group offsets and currently defaults to `earliest` when the AdminClient query fails.
- 2026-10-21: confirmed Docker is available (`docker info` reports server 29.3.1); the task is scoped to the existing Kafka-only architecture.
- 2026-10-21: moved offset inspection to Vert.x worker threads, made inspection failures fail startup, and added preflight validation against each committed partition's available offset range.
- 2026-10-21: focused H05/H06 Kafka integration cases passed; full clean Maven verification passed with all unit and integration tests.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| H05 topic partition mismatch | `f4588e6` + worktree changes | `mvn -B -ntp '-Dit.test=KafkaWorkerIT#topicProvisionerFailsWithoutResizingAnExistingTopic+newConsumerGroupConsumesExistingRecordsFromEarliest+establishedConsumerGroupWithOutOfRangeOffsetFailsStartupWithoutResetting' verify` | passed; 3 selected integration tests, 0 failures/errors/skips | [H05-H06-validation.txt](./evidence/H05-H06-validation.txt) |
| H06 new and established group offset policy | `f4588e6` + worktree changes | same focused integration command | passed; new group consumed pre-existing record; out-of-range established offset failed startup without handler invocation or offset mutation | [H05-H06-validation.txt](./evidence/H05-H06-validation.txt) |
| Java unit and integration suite | `f4588e6` + worktree changes | `mvn -B -ntp clean verify` | passed; 127 unit tests and 18 Kafka/Testcontainers integration tests; 0 failures/errors/skips | [H05-H06-validation.txt](./evidence/H05-H06-validation.txt) |
| whitespace | `f4588e6` + worktree changes | `git diff --check` | passed | no whitespace errors |

Include a negative case that could disprove the invariant. Separate unit,
integration, scanning, and review evidence. Explain skips or environment failures.

## Failure analysis and correction

- Observed failure and reproduction: AdminClient failures in the group-state probe previously selected `auto.offset.reset=earliest`, which could hide uncertainty about an established group.
- Classification: requirement / reasoning
- Root cause supported by evidence: `initialOffsetPolicyUsesEarliest()` catches every exception and returns `true`.
- Code, instruction, or tool change: offset inspection now runs via `executeBlocking`; checked Admin failures and out-of-range committed offsets fail startup. The worker uses `auto.offset.reset=none` when committed offsets exist.
- Rerun and remaining uncertainty: focused H05/H06 tests and the full clean verification passed. Admin connectivity failure was not separately injected in an integration test; the fail-visible path is implemented and exercised by the same startup failure handling for checked probe errors.

## Handoff and review

- Latest verified commit and working-tree changes: base `f4588e6`; H03-H04 task-record edit preserved; H05-H06 implementation, tests, task record, and evidence are uncommitted.
- Completed steps: fail-fast topic count verification, off-event-loop consumer offset preflight, new-group earliest behavior, out-of-range group rejection, focused and full Maven verification.
- Next concrete step: proceed to Spec 003 H07-H08 after review; do not treat the uncommitted changes as merged.
- Open risks/decisions and owner: connectivity failures were not separately injected; committed-offset range behavior was verified against Testcontainers Kafka.
- Reviewer findings and disposition: pending.
- PR / final outcome: pending.

## Learning check

- What invariant did this task establish? Startup does not resize existing topics or silently reset established consumer-group offsets; uncertain offset state fails instead of selecting a reset policy.
- What failure would invalidate the claim? A mismatched topic is expanded, or an established group's invalid offset (or an offset-probe failure) falls back to earliest.
- Which artifact proves what actually happened? The focused Testcontainers cases and recorded Maven verification results.
