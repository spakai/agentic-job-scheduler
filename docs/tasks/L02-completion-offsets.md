# L02: Commit only completed delivered prefixes

Status: ready for review (library implementation); learning exercise incomplete

## Scope and plan

- Requirement: Spec 002 sections 5–6; B05/B09, with B15 unit cases.
- Base commit: `d677cdd` (merged L01); branch `feat/l02-completion-offsets`.
- Authorization: execute L02; on 2026-10-10 the user additionally authorized
  commit, push, and PR creation. This supersedes the initial local-only scope.
  Merge, deployment, and account configuration changes remain outside scope.
- Invariant: only success or acknowledged retry/DLQ handoff completes a record;
  commit snapshots cannot pass any incomplete delivered record. Offset gaps are
  allowed. One commit request is outstanding at a time; failure retains work.
- Build a bounded, context-confined completion/commit library with opaque delivery
  and commit handles, immutable commit snapshots, and explicitly assigned partitions.
- Files: execution source/tests, README, this task record/evidence, learning ledger.
- Validate out-of-order completion, gaps, independent partitions, failed commits,
  retained-record bounds, and simulated crashes after success/handoff before commit.
  Run `mvn test`, `mvn package`, and `git diff --check`.
- Non-goals: Kafka/Vert.x adapter, producer, handler dispatch, rebalance lifecycle,
  process-wide byte watermarks, live broker crash testing, or durable deduplication.

## Tools and boundaries

- Local shell, Git, patch tools: unrestricted filesystem/network, approval policy
  never. Instructions scope use to this repository; they are not an enforced sandbox.
- Java: OpenJDK 21.0.12.1; Maven 3.6.3. Maven can download dependencies and execute
  build/plugin code. No new dependency is needed for the pure state machine.
- Initial Docker check failed in WSL; superseded after the user started Docker
  Desktop on 2026-10-10. `docker version` now exits 0 and reaches Docker Desktop
  4.68.0 / Engine 29.3.1. No Kafka integration suite exists; no integration result
  is claimed.
- `gh` is installed. Read-only API checks verified repository access and PR #1
  merged on 2026-10-08. Repository API reports push/admin permissions; remote write
  restrictions and branch protection are unverified. Git/gh can mutate remotes.
- Network: successful Git fetch, GitHub API read, and official Kafka documentation
  lookup. No credentials were printed or copied to repository files.
- GitHub connector tools are exposed, including writes; exposure does not prove
  successful access or restrictions. Installed Copilot CLI was not found on PATH.
  The separate Copilot/GitHub remote MCP setup and harmless denied-operation test
  remain unperformed; this session does not establish a read-only credential boundary.
- Recovery: isolated feature branch; discard/revise only task-owned changes.

## Progress and decisions

- 2026-10-10: Clean starting worktree. L01 record's open-PR status is historical,
  superseded by live PR #1 MERGED response. Fetched main and branched at `d677cdd`.
- Verified Kafka offset gaps, manual commit and replay semantics against
  [official Kafka consumer documentation](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
- This library returns conservative last-completed-offset + 1 positions. The later
  adapter must supply explicit offsets, disable auto-commit, serialize all calls on
  the owner context, register every delivered record in partition order before
  dispatch, and translate broker callbacks into commit results. It must preserve
  leader epoch metadata when integrating with the selected Kafka API.
- Retain completed entries until commit acknowledgment so failures cannot silently
  release the tracker bound. Bounds cover metadata records, not payload bytes/client
  buffers. Full process backpressure remains B14 work.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Unit tests: B05/B15, simulated B09 | `d677cdd` + working tree | `mvn -B -ntp test` | passed: 94 tests, 0 failures/errors/skips | [log and source hashes](evidence/L02-validation.txt) |
| Library package | `d677cdd` + same source tree | `mvn -B -ntp package` | passed: 94 tests; library JAR built | [evidence](evidence/L02-validation.txt) |
| Whitespace | `d677cdd` + working tree | `git diff --check`; new-file whitespace check | passed | [evidence](evidence/L02-validation.txt) |
| Docker connectivity recheck | `d677cdd` + working tree | `docker version` | passed: client/server reachable | [evidence](evidence/L02-validation.txt) |
| Live Kafka crash/replay | n/a | runtime/suite not implemented | not run | Docker now reachable; tests still absent |
| Security scan / independent review | n/a | n/a | not run | no claim |

## Failure analysis and correction

- Environment finding: Docker command failed because WSL integration is unavailable.
  Superseded by successful client/server check after the user started Docker
  Desktop. Unit tests do not require Docker; live integration remains an explicit gap.
- Publication check initially caught trailing spaces in captured Maven log lines
  after the evidence file was staged. Stripped log trailing whitespace and reran
  `git diff --cached --check`; no source change or test rerun was needed.
- No implementation test failures occurred. Negative tests reject premature commit,
  invalid admission, stale/foreign handles, and bound overflow as expected.

## Handoff and review

- Verified base: `d677cdd53c99383b7acb0e72a23f9d1da9b2d6f2` plus uncommitted
  source/test changes identified by SHA-256 in the evidence file. Before publication,
  all recorded build-input hashes were rechecked against the files being committed.
- Completed: bounded tracker, explicit serialized commit snapshots, 17 new tests
  (94 total), README API contract, tool inventory, and status documentation.
- Self-review: snapshots cannot grow while in flight; success removes only their
  prefix; failure retains results; gaps do not create phantom unfinished records;
  foreign and stale handles cannot mutate an active tracker. Forty deterministic
  shuffled schedules compare the frontier against a separate delivery-order model.
- B05/B15 are tested at the library boundary. B09 is simulated after handler success,
  retry acknowledgment, DLQ acknowledgment, and broker commit before callback loss.
  Actual publication/handler execution (B06/B08/B16), rebalance handling (B11), and
  process-wide payload/fetch bounds (B14) are not established by these tests.
- Next step: review the published PR and select the installed client/environment
  for the separate Copilot/MCP setup exercise. The user requested the code-level
  learning explanation and then authorized publication; no independent learner
  assessment is claimed. Later runtime integration must connect the documented adapter
  contract to real Kafka before claiming full B05/B09/B15 acceptance.
- Full B09 broker evidence and Copilot/MCP permissions exercise remain open.
- Publication: prepare the implementation commit on `feat/l02-completion-offsets`,
  push that branch, and open a PR against `main`. The GitHub PR records the final
  commit and publication outcome; merging is not part of this task.

## Learning check

- Invariant: later completions never allow a commit to skip an earlier incomplete
  delivered record. A failure would lose replayable work after a restart.
- Evidence: controlled completion order and simulated durable-offset crash tests.
- Check of understanding: if offsets 11 and 12 finish while 10 is still running,
  which offset can safely be committed, and what changes when 10 completes?
