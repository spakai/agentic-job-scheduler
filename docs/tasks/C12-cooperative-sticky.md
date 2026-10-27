# Task C12: Preserve unaffected work during cooperative reassignment

Status: ready for review

## Scope and plan

- Requirement: user selected cooperative sticky assignment, validated by Spec 005
  partial-revocation tests (C12).
- Spec 002 acceptance IDs: B03, B05, B11, B14; Spec 003 H01/H02 are related.
- Base commit: `f4588e6b08ed287cff9da83d9e03ade7938617d2` with pre-existing
  H03–H06 working changes; branch `feat/cooperative-sticky-rebalance`.
- Authorized scope: configure both worker roles with classic protocol and only
  CooperativeStickyAssignor; add broker-backed partial-revocation regression tests;
  align Spec 005 and operational docs. No static membership or full chaos campaign.
- Expected behavior: retained partitions preserve running handlers, queued
  successors, and commit progress across a partial revoke. Moved work can replay;
  late outcomes from moved owners cannot commit unfinished replacement work.
- Files: KafkaWorker configuration; new KafkaWorkerCooperativeIT; README, arch.md,
  Spec 005, this record and validation evidence. Preserve existing source/test edits.
- Plan: inspect pinned client behavior; configure assignor; test both roles through
  two joins/departures with handler barriers and broker-observed commits; run
  focused integration, unit, package, and appropriate integration regressions.
- Non-goals: fixing all C01–C15 gaps, partition expansion, global fencing, cluster
  deployment, static membership, newer consumer group protocol.

## Tools and boundaries

- Local shell/patch tools and Maven; disposable Testcontainers Kafka only.
- Sandbox prevented Git metadata writes and Docker socket access. Approved
  escalations created the feature branch and verified Docker server 29.3.1.
- Existing dirty files remain in place; no commit/push requested.
- Java 21.0.12.1 / Maven 3.6.3 available. Kafka clients 4.2.0, Vert.x 5.2.1 pinned.

## Progress and decisions

- 2026-10-27: checked official Kafka 4.2 consumer configuration documentation:
  https://kafka.apache.org/42/configuration/consumer-configs/ . Specify only the
  cooperative assignor because the default Range-first list selects eager behavior.
- Inspected pinned Vert.x JAR with `javap -c -p` for
  `KafkaReadStreamImpl$1`: revoke/assign callbacks forward the provided collection
  asynchronously using the owner context. The adapter does not override lost
  partitions in that listener. No stronger final-revoke commit guarantee is added.
- Prior Spec 005 decision to retain Range as deployment default is superseded by
  explicit user direction to select cooperative sticky. Static membership stays
  deferred. This implementation does not establish the full Spec 005 lifecycle.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Focused C12 integration | base + working changes | `mvn -B -ntp -Dit.test=KafkaWorkerCooperativeIT verify` | passed: 127 unit, 2 C12 cases; no failures/errors/skips | [validation](evidence/C12-cooperative-sticky-validation.txt) |
| Unit suite | base + working changes | `mvn -B -ntp test` | passed: 127; no failures/errors/skips | [validation](evidence/C12-cooperative-sticky-validation.txt) |
| Artifact | base + working changes | `mvn -B -ntp package` | passed: JAR built and 127 unit tests | [validation](evidence/C12-cooperative-sticky-validation.txt) |
| Integration regression | base + working changes | `mvn -B -ntp clean verify` | passed: 127 unit + 20 integration; no failures/errors/skips | [validation](evidence/C12-cooperative-sticky-validation.txt) |
| Documentation / whitespace | base + working changes | Local link existence checks; `git diff --check` | passed | [validation](evidence/C12-cooperative-sticky-validation.txt) |

Negative case: retained handler completion must advance its original partition
without replay; an eager full revoke or replacement dispatcher should fail this
assertion. Broker assignment observations identify which partitions actually moved.

## Failure analysis and correction

Focused C12, unit, package, and full clean integration regression passed on first
execution. No implementation correction was required. Expected negative-case
warnings in the existing suite include handler timeout, out-of-range startup, and
failed handoff publication. Kubernetes and full fault campaigns are outside this slice.

## Handoff and review

Documentation follow-up, 2026-10-27 (user requested before PR): added
[CHAOS.md](../CHAOS.md) with commands, C01–C15 evidence boundaries, rollout links,
and remaining slices. Reconciled stale README/architecture coverage statements,
linked Specs 002/005 and the scenario atlas, and updated the learning ledger.
Historical task results remain historical; no new runtime test or scan is claimed
for this documentation-only follow-up. Validation passed: all four source hashes
match the existing C12 evidence, local links in all nine touched/new documents
resolve, C01–C15 appear exactly once in the new ledger, and `git diff --check`
reports no whitespace errors. No commit, push, or PR was created in this follow-up.

- Configuration and controlled main/retry C12 tests implemented; full clean
  regression passed. Source hashes, commands, counts, and outcomes are recorded
  in the validation artifact. No new commit or push performed.
- Next step: review the scoped change. Before deploying to an existing group,
  verify assignor compatibility as documented in README. No deployment performed.
- Existing source and test changes are inputs, not newly authored work in this task.
- Remaining Spec 005 lifecycle and Kubernetes/chaos requirements remain pending.

## Learning check

- Invariant: unaffected partitions retain their running handlers and queued lanes.
- Disproof: an unaffected head replays or its original completion cannot commit.
- Evidence: broker-observed cooperative assignment plus barrier-controlled handler
  starts and committed source offsets, across repeated membership changes.
