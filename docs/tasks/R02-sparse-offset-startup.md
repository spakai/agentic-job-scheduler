# R02: Resume groups with offsets on only some partitions

Status: ready for review

## Scope and plan

- Requirement: resume an established Kafka group when only some assigned partitions have committed offsets.
- Spec 002 acceptance IDs: B09/B18 recovery, preserving B05/B15 safe commits and B11 ownership.
- Base commit and branch: `a6781ab4480aa6a6fb986d82edba63287c94f159`; `fix/sparse-consumer-offset-startup`.
- Authorized scope: preserve inherited R01 remaining-attempts and failure-demo changes while addressing sparse-offset startup.
- Expected behavior: missing offsets start at each partition's earliest retained record; existing committed positions stay unchanged; out-of-range offsets fail visibly. Do not reset the group or synthesize completion.
- Implementation boundary: handle only `NoOffsetForPartitionException`, seek only its currently owned partitions, never pass an empty set to `seekToBeginning` (Kafka interprets it as all assigned partitions), and resume polling only after the asynchronous seek completes. Treat out-of-range offsets as terminal rather than a reset/retry loop.
- Files: `KafkaWorker`, `KafkaWorkerIT`, Spec 002, and this task/evidence record.
- Validation: real Kafka restart tests for both worker roles, including no replay of a completed record, recovery of pending earliest work, later work on an initially empty partition, and a negative out-of-range case.
- Non-goals: manual offset reset, topic recreation, changes to retry budgets or ordering contracts, dependency changes.

## Tools and boundaries

- Local Git, Maven, Testcontainers, Docker.
- No commit or push requested; existing R01 working-tree changes are retained.
- API references checked: [Kafka 4.2 `NoOffsetForPartitionException`](https://archive.apache.org/dist/kafka/4.2.0/javadoc/org/apache/kafka/clients/consumer/NoOffsetForPartitionException.html) and [Vert.x Kafka consumer API 5.2.1](https://vertx.io/docs/apidocs/io/vertx/kafka/client/consumer/KafkaConsumer.html).

## Progress and decisions

2026-10-29: branch `fix/sparse-consumer-offset-startup` is based on the recorded base commit and contains the inherited R01 changes. The R01 Compose run showed that startup selected `auto.offset.reset=none` when a group had any committed offset, after which Kafka reported never-committed partitions and polling retried indefinitely. Its manual offset initialization is historical recovery, not the fix.

The worker now distinguishes absent offsets from out-of-range committed offsets. It maps Kafka's missing-partition set to Vert.x partitions, filters to current ownership, avoids an empty seek set, and resumes polling only after the seek future succeeds. The regression verifies both main and retry groups.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Unit tests | `a6781ab` + worktree | `mvn -B -ntp test` | PASS, 132 tests, 0 failures/errors/skips | [R02 validation evidence](./evidence/R02-sparse-offset-startup-validation.txt) |
| Library package | `a6781ab` + worktree | `mvn -B -ntp package` | PASS, 132 tests; JAR built | [R02 validation evidence](./evidence/R02-sparse-offset-startup-validation.txt) |
| Sparse startup and invalid-offset controls | `a6781ab` + worktree | `mvn -B -ntp '-Dit.test=KafkaWorkerIT#restartedGroupReadsMissingOffsetsWithoutReplayingCommittedRecords+establishedConsumerGroupWithOutOfRangeOffsetFailsStartupWithoutResetting' verify` | PASS, 132 unit tests and 3 integration invocations | [R02 validation evidence](./evidence/R02-sparse-offset-startup-validation.txt) |
| Full suite | `a6781ab` + worktree | `mvn -B -ntp verify` | PASS, 132 unit tests and 22 integration tests; no failures/errors/skips | [R02 validation evidence](./evidence/R02-sparse-offset-startup-validation.txt) |
| Diff whitespace | `a6781ab` + worktree | `git diff --check` | PASS | [R02 validation evidence](./evidence/R02-sparse-offset-startup-validation.txt) |

## Failure analysis and correction

- Observed failure: R01 Compose demo could not start a group with committed progress on only one of its partitions; see [R01 Compose failure demo](./evidence/R01-compose-failure-demo.txt).
- Root cause: established groups use `auto.offset.reset=none`, so missing partition offsets caused `NoOffsetForPartitionException`; the prior poll failure path logged and retried without initializing those positions.
- Correction: seek only the exception's currently owned missing partitions to earliest. Do not modify committed offsets. Keep out-of-range offsets as visible startup/runtime failures.
- Rerun: targeted negative/positive integration cases and the full Kafka integration suite passed. No records were lost or reset by these tests.

## Handoff and review

- Latest verified commit: base `a6781ab`; implementation and inherited R01 changes remain uncommitted in the worktree.
- Completed: sparse-offset recovery, two-role restart coverage, committed-position preservation assertion, empty-partition follow-up, invalid-offset negative control, documentation, and validation evidence.
- Next step: review the combined R01/R02 worktree and commit only when requested.
- Open risks: no new issue identified in the validation runs; no commit, push, or separate review performed.

## Learning check

If partition A has committed offset 7 and partition B has no offset, only B should seek to earliest. Seeking A too can replay already committed work. The regression confirms that the completed record is not invoked again and its committed position remains unchanged.
