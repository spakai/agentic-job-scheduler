# L03: Dispatch immediate work with per-topic job exclusion

Status: ready for review (dispatcher component); fresh-session exercise pending

## Scope and plan

- Requirement: Spec 002 sections 2, 5–7; B02–B04 at the dispatcher boundary,
  with B05/B15 tracker integration and a controlled B12 overlap case.
- Base: `fa5222168a60c809d568776027031bc33583e455` (merged CI PR #3).
- Branch: `feat/l03-immediate-processing`; clean starting worktree.
- Authorization: implement L03. No merge or deployment requested.
- Invariant: dispatch a queue head immediately when capacity exists; one active
  execution per jobId in a topic under stable ownership. Different IDs can overlap.
  Hold the job gate until success or acknowledged handoff; release handler capacity
  only when the handler reports it has actually stopped.
- Implement a Vert.x owner-context dispatcher for one topic/fixed assignment,
  per-job FIFO queues, round-robin partition selection, configurable fixed capacity,
  bounded admission through L02, and explicit handoff/commit adapter methods.
- Add a nonblocking handler contract carrying jobId/executionId, logical attempt,
  and source. Future completion (success or failure) must certify work has stopped.
- Validate with controlled promises/barriers, no timing sleeps: A1/B1 overlap,
  A2 waits, immediate starts, capacity release, handoff gate retention, safe commit
  prefixes, independent main/retry instances, context confinement, and bounds.
- Files: pom.xml, execution source/tests, arch.md, README/spec status/learning ledger,
  task record and retained evidence/concurrency trace.
- Non-goals: Kafka consumer/producer, routing metadata, real broker durability,
  retry policy/rate/adaptive limiter (L04), rebalance/cancellation/shutdown (L05),
  process-wide byte/fetch accounting (L06). Fixed capacity is per dispatcher for
  this slice, not the final process-wide main limit or retry rate implementation.

## Tools and boundaries

- Local shell/Git/Maven and official documentation reads. Unrestricted local
  filesystem/network; repository instructions govern scope, not enforced isolation.
- Pin vertx-core 5.2.1, verified Maven Central artifact and official current
  [Context](https://vertx.io/docs/apidocs/io/vertx/core/Context.html) and
  [Future](https://vertx.io/docs/apidocs/io/vertx/core/Future.html) API documentation.
- Handler completion may originate on another thread; marshal all state mutations
  onto the event-loop context. Reject direct off-context API calls.
- Unit/component tests use real Vert.x event loops but no Kafka. Docker availability
  does not turn these into broker integration tests.

## Progress and decisions

- 2026-10-10: Revalidated current spec, L02 implementation, and CI changes against
  main. PR #3 is merged; its push-to-main run passed:
  https://github.com/spakai/agentic-job-scheduler/actions/runs/38018328025.
- Drift check: Spec 001 calls for PostgreSQL/REST/Flyway. Historical Spec 002 at
  `826acad` includes scheduling state and a retry forwarder. Both are superseded
  by current Spec 002: immediate processing, no state store, retries execute in
  their own topic, and main/retry overlap is allowed. No stale architecture added.
- One dispatcher owns one topic. Per-job gates span its assigned partitions;
  Kafka key/partition validation remains the adapter's responsibility.
- Pending/failed handoff publication does not invoke completion or rerun handlers.
  L03 exposes this boundary; it does not implement the publisher.

- Documentation follow-up: user requested the teaching diagrams in `arch.md`.
  Added the L01–L03 architecture/test summary and linked it from README. Diagrams
  distinguish implemented library components from planned Kafka/verticle wiring.
  Checked local links, Markdown fences, and whitespace; no code changed or test
  rerun was needed for this documentation follow-up.

## Evaluation

| Check | Revision | Command | Result | Evidence |
| --- | --- | --- | --- | --- |
| Dispatcher and regression tests | `fa52221` + source snapshot | `mvn -B -ntp clean test` | passed: 108 tests, 0 failures/errors/skips | [log/hashes/trace](evidence/L03-validation.txt) |
| Library artifact | `fa52221` + same source snapshot | `mvn -B -ntp package` | passed: 108 tests; library JAR built | [evidence](evidence/L03-validation.txt) |
| Current clean package verification | `fa52221` + current L03 worktree | `mvn -B -ntp clean package` | passed: 108 tests, 0 failures/errors/skips; library JAR built at 2026-10-10 11:40 +08:00 | clean build output in session; retained result summary in [evidence](evidence/L03-validation.txt) |
| Dependency alignment | same source snapshot | `mvn -B -ntp dependency:tree -Dincludes=io.vertx:*,com.fasterxml.jackson.core:*` | passed; aligned Jackson family | [evidence](evidence/L03-validation.txt) |
| Incremental build after BOM change | same source snapshot | `mvn -B -ntp test` | failed during discovery, before tests ran | retained failure excerpt below/in evidence |
| Diff and new-file whitespace | base + working tree | `git diff --check`; Python whitespace check | passed | local check |
| Kafka integration/security scan | n/a | n/a | not run | not implemented in this slice |
| Fresh-session resumption | n/a | separate future session | not run | handoff will be prepared; no fresh-session claim |

## Failure analysis and correction

- Dependency inspection after the first passing 108-test run found mixed Jackson
  versions: Vert.x brought core 2.21.7 while L01 pinned databind 2.20.1. No test
  failure occurred, but leaving the family unaligned creates a compatibility risk.
- Align via the official Jackson BOM 2.21.7 (core/databind 2.21.7, annotations 2.21),
  matching Vert.x's core baseline. Verified the
  [release](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.21.7) and
  [BOM](https://github.com/FasterXML/jackson-bom). Final clean tests/package passed
  with the aligned family; dependency tree confirms the selected versions.
- This supersedes L01's dependency pin for the current build; historical L01 evidence
  remains unchanged. No security scan or vulnerability-free claim is made.

- An incremental run after alignment failed before executing tests with
  `NoClassDefFoundError: com/example/agenticjobscheduler/messaging/SourceRecord`,
  despite the compiler reporting classes up to date. The class file existed on
  subsequent inspection; its modification time coincided with the failed run.
  Exact external/incremental-build cause is unconfirmed; no source defect established.
  `mvn clean test` rebuilt all classes and passed, followed by passing `mvn package`.
  If this recurs, investigate concurrent IDE/build writes to target before changing code.

## Handoff and review

- Verified base: `fa5222168a60c809d568776027031bc33583e455` plus L03 changes
  identified by the SHA-256 snapshot in evidence. At the time of that source
  snapshot, no L03 commit/push/PR existed.
- Completed: dispatcher, handler/execution contract, 14 new controlled test cases,
  L02 tracker integration, dependency alignment, API docs, and checked concurrency trace.
- Self-review: handler completion returns to owner context; no same-job dispatch
  occurs during a pending handoff; completed entries still consume tracker capacity;
  round-robin scheduling moves across partitions and then FIFO ready lanes; no timer
  or state store was added. Null handler futures hold capacity/gate as STUCK.
- Next concrete step after review: continue L04 integration with typed failure
  classification/publication, shared adaptive permits, and retry rate limits.
  Fresh-session resumption remains pending. Rerun only when source or environment
  changes or an unresolved finding warrants it.
- L04 integration must add typed failure classification/publication, shared adaptive
  permits and retry rate limits; L05 must invalidate old ownership callbacks and add
  cancellation/drain before a Kafka worker can safely use this component.
- Caller responsibilities: submit every fetched record in partition order, supply
  already-validated envelopes/attempts, marshal API calls to the owner context,
  and signal handoff only after broker acknowledgment. Handler completion must
  certify actual stop. These contracts cannot fence external work on ownership loss.
- Fresh-session resumption has not happened in this session. The drift comparison
  and durable handoff are prepared; do not mark that learning exercise complete.
- `.vscode/settings.json` appeared during work and is unrelated; left unchanged
  and excluded from the task. Durable task documentation is not durable Kafka state.
- No independent review or completed learner assessment claimed.

## Learning check

- Invariant: A1/B1 can overlap; A2 cannot overlap A1 in one topic. Retry A1 may
  overlap main A2 after acknowledged handoff.
- Failure: releasing A1's gate merely because publication failed allows A2 too early.
- Check: what evidence would a new session need to distinguish a tested dispatcher
  from a fully implemented Kafka worker?
