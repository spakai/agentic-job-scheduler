# Task R01: Count down remaining attempts in job envelopes

Status: ready for review

## Scope and plan

- Requirement: replace maxAttempts with remainingAttempts; every handler failure decrements once, retries on jobs.retry.v1 while positive, DLQ at zero.
- Spec 002 acceptance IDs: B01, B06, B07, B08, B16; preserve B03/B05/B09.
- Base commit: a6781ab4480aa6a6fb986d82edba63287c94f159; branch feat/remaining-attempts-envelope.
- Authorized scope: envelopes, validation, handoff policy, producer/log output, tests, current documentation. No dependency changes or deployment.
- Expected behavior: 3 -> retry 2 -> retry 1 -> DLQ 0; success does not publish another record. All handler failures use this budget, including PermanentJobFailure. Invalid input/routing still goes directly to DLQ without execution.
- Keep attempt as diagnostic metadata and handoff identity input. Budget includes the current execution. Main defaults to 3; runnable envelopes require 1–100, retry metadata plus remaining budget cannot exceed 100.
- Breaking development wire change: maxAttempts is rejected rather than silently defaulted. No automatic migration or deletion of existing Kafka records; coordinated producer/worker rebuild needed. Existing demo containers remain unchanged during implementation.
- Validation: unit tests and packaging; real-Kafka integration tests for decrement/exhaustion, successful retry and failed publication. Negative cases: zero runnable budget and publication failure must not consume another attempt.
- Non-goals: durable deduplication, global ordering, external-effect fencing, delayed retries.

## Tools and boundaries

- Local shell/Git/Maven for code and checks; Docker for isolated Testcontainers Kafka.
- .git writes and Docker access require sandbox escalation; branch creation approved. No remote writes.
- Repository guidance about absent runtime/integration tests is superseded by actual sources and build configuration; record executed coverage only.

## Progress and decisions

- 2026-10-29: inspected Spec 002, current code and tests. Supersedes permanent-failure direct DLQ and maxAttempts-based routing. Retry sends reuse an immutable, already-decremented envelope.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Units | a6781ab + working diff | `mvn -B -ntp test` | PASS: 130 tests after stale assertion correction | [evidence](evidence/R01-remaining-attempts-validation.txt) |
| Package | a6781ab + working diff | `mvn -B -ntp package` | PASS: 130 units, JAR built | [evidence](evidence/R01-remaining-attempts-validation.txt) |
| Kafka | a6781ab + working diff | `mvn -B -ntp verify` | 19/20 PASS; one topic-creation timeout before worker start | [evidence](evidence/R01-remaining-attempts-validation.txt) |
| Kafka rerun | same working diff | `mvn -B -ntp -Dit.test=KafkaWorkerIT#workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner verify` | PASS: 130 units and the 1 failed integration case | [evidence](evidence/R01-remaining-attempts-validation.txt) |
| Diff hygiene | same working diff | `git diff --check` | PASS | local diff |

## Failure analysis and correction

- Initial branch creation was blocked by read-only .git; approved escalation succeeded.
- Initial `mvn -B -ntp test`: 130 tests, one failure in `ImmediateJobDispatcherTest.validatesConfigurationAndLogicalAttempt`. The test still rejected attempt 4 against a budget of 3 using the superseded maxAttempts contract. Updated the negative case to attempt 99 with remaining 3 (combined original budget exceeds 100). Rerun passed all 130 tests. Full transient logs: `/tmp/remaining-attempts-unit.log`, `/tmp/remaining-attempts-unit-rerun.log`.
- `mvn -B -ntp package` passed (130 unit tests); real-Kafka `mvn -B -ntp verify` ran all 20 integration cases. The countdown, one-attempt exhaustion, successful retry, failed publication and immutable budget checks passed. One unrelated rebalance case timed out at `TopicProvisioner.ensureTopics:37` before worker start; a focused rerun passed without implementation changes. Root cause of the provisioning timeout remains unestablished; do not claim one clean full integration run. No dependency or external API changes were needed.

## Handoff and review

- Latest verified state: base a6781ab4480aa6a6fb986d82edba63287c94f159 plus this working diff on feat/remaining-attempts-envelope.
- Completed: remainingAttempts wire/model, validation, immutable decrement and retry/DLQ routing, demo logs, unit and real-Kafka assertions, Spec 002/README/architecture updates. Historical maxAttempts and permanent-direct-DLQ decisions are superseded by this record.
- Self-review: budget decreases once before encoding; send retries reuse bytes; offsets/gates still await acknowledgment; identities and payload stay stable; zero cannot enter a handler. No external review claimed.
- Next step: user review, then coordinated producer/worker rebuild if desired. Existing Compose containers still run the previous image; no data was deleted or migrated.
- Remaining risk: breaking wire format; old maxAttempts records route to invalid-input DLQ if replayed after upgrade. Transient Kafka topic-creation timeout recorded separately from passing rerun.
- No commit, push, deployment, new dependency or scan performed.

## Learning check

- Invariant: only a handler failure consumes one unit of budget; failed Kafka sends retry identical bytes and keep the source incomplete.
- Counterexample: failed publication reduces 2 to 1 before a retry handler runs.
- Check: after a handler fails with 3 remaining and its handoff send fails twice, the eventual retry must still carry 2.

## Follow-up: requested Compose failure demo (2026-10-29)

User authorized rebuilding/running the local demo to demonstrate main -> retry -> DLQ.
The earlier no-deployment boundary is superseded for this local Compose run only.
B06/B07/B08/B20: add explicit sample-producer --fail and demo-only payload flag,
log source coordinates and incoming budget, rebuild workers/producer, submit one
job and inspect real retry and DLQ records. Normal samples must still succeed.
Preserve existing Kafka volume and two retry replicas. Validate demo handler
success/failure, Maven test/package, and the real Compose countdown. Results pending.

Follow-up outcome: completed. [Compose evidence](evidence/R01-compose-failure-demo.txt)
records 132 passing unit tests, successful package/build, one actual job through
main(3) -> retry(2) -> retry(1) -> DLQ(0), stable identities, and zero source lag.
The first restart exposed an existing sparse-offset startup bug: an established
group with offsets committed on only some partitions uses auto.offset.reset=none,
preventing reads on never-committed partitions. Recovered this inspected main group
by initializing only its missing offsets at earliest (0), preserving partition 1's
committed offset (1), then restarted main. No records/volumes deleted. Source fix
is still open and retry-worker restarts can encounter the same condition.
Earlier statements that containers use the previous image are superseded: main
and both retry replicas now run rebuilt images. All remain running. Next step is
review and a separate scoped startup-policy fix; no commit/push performed.
