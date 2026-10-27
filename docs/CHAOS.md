# Reassignment and chaos testing

Last reconciled: 2026-10-27. Requirements live in
[Spec 005](../specs/005-partition-reassignment-chaos.md); this document records
what can be run today, what passed, and what remains to build.

## Current evidence

Both main and retry workers use classic protocol and
`org.apache.kafka.clients.consumer.CooperativeStickyAssignor` as the sole
assignor. Static membership is not enabled. Topic partition counts remain fixed.
Kafka moves whole partitions; this policy cannot pin JobId=A to a pod or wait
for A's handler to finish before moving its partition.

The latest clean local validation passed **127 unit tests and 20 Kafka integration
tests**, with zero failures, errors, or skips. The library package also passed.
These are local working-tree results, not a claim that CI ran this revision.
See the [C12 task](tasks/C12-cooperative-sticky.md) and
[command output summary/source hashes](tasks/evidence/C12-cooperative-sticky-validation.txt).
The recorded base is `f4588e6b08ed287cff9da83d9e03ade7938617d2` plus the source
changes identified by those hashes; a base commit alone does not identify the
tested artifact.

`KafkaWorkerCooperativeIT` runs once per worker role, with two join/departure
cycles in each case. Three fixed partitions start on one worker. Handler barriers
hold each head while a same-job successor queues, then a second member joins.
The test observes the negotiated assignor and assignments through Kafka Admin,
releases original retained heads, and checks that:

- Unaffected heads do not replay; their original queued successors progress and
  commit while the moved replacement heads are still blocked.
- Late completion on a moved old owner cannot commit the replacement's incomplete
  work or dispatch the old queued successor.
- Replacement work replays and completes; processing windows retire to zero.
- After the newcomer leaves, the original member processes another cycle.

These assertions establish continuity for the exercised states. They do not
prove every retirement path, process-wide permits across retired owners, external
effect fencing, or uninterrupted throughput during every rebalance.

## Acceptance ledger

“Related coverage” means an existing test exercises part of a scenario; it does
not close the full Spec 005 case or its required Kubernetes tier.

| Spec 005 case | Current status and boundary |
| --- | --- |
| C01: scale 1 → 2 → 4 → 1 | Related coverage: C12 exercises repeated 1 → 2 → 1. Four members/idle excess member and pod tier pending |
| C02: graceful removal | Related coverage: worker shutdown tests and C12 departures. Full shutdown-budget/no-new-starts and Kubernetes acceptance pending |
| C03: active-handler process kill | Existing `KafkaWorkerCrashIT` proves child-JVM kill and replacement replay; actual pod kill tier pending |
| C04: crash after success/handoff ACK before commit | Deterministic process-boundary scenarios pending; simulated replay is not process-crash evidence |
| C05: isolate live worker beyond session timeout | Pending; old process must remain alive to expose concurrent replay |
| C06: retained fetch batch across reassignment | Pending; mixed partitions and reacquisition need focused reproduction |
| C07: completed record behind unfinished prefix at revoke | Related queued/running retirement tests exist; completed-but-uncommitted and repeated watermark scenario pending |
| C08: revoke during handoff retry/in-flight send | Related failed-send recovery exists; generation-aware retirement scenarios pending |
| C09: queued/in-flight commits across reacquisition | Related stale-handler checks exist; delayed commit callback/queue scenario pending |
| C10: retired handlers counted against process permits | Pending; C12 does not establish this resource invariant |
| C11: retry-rate queue invalidation | Existing `KafkaWorkerIT` shutdown invalidation case passes; full rebalance variant pending |
| C12: cooperative partial revoke and repeated churn | Passed at Kafka integration tier for both roles, two joins/departures each; evidence linked above |
| C13: broker outage at each completion boundary | Existing broker pause/recovery and historical Compose restart evidence; full boundary matrix pending |
| C14: short pod grace/unconfirmed cancellation | Existing handler-cancellation tests pass; Kubernetes forced-grace case pending |
| C15: seeded combined fault campaign | Pending; no campaign runner or results yet |

## Commands available now

Use Java 21+, Maven, and a reachable Docker daemon. Testcontainers starts disposable
Kafka containers; the current tests use `apache/kafka-native:3.8.0`, Kafka clients
4.2.0, and Vert.x 5.2.1. This is single-broker evidence, not replicated-cluster
availability evidence.

```sh
# Focused C12 tests for both roles; also runs unit/package phases.
mvn -B -ntp -Dit.test=KafkaWorkerCooperativeIT verify

# Existing live-worker stale-handler rebalance regression.
mvn -B -ntp '-Dit.test=KafkaWorkerIT#workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner' verify

# Separate process killed while a handler is active.
mvn -B -ntp -Dit.test=KafkaWorkerCrashIT verify

# Full clean unit, artifact, and Kafka integration validation.
mvn -B -ntp clean verify
```

Reports are in `target/surefire-reports/` and `target/failsafe-reports/`. Save the
command, exit status, source identity, and reports before another clean build.
Docker failure is an environment failure, not a skip that establishes acceptance.
The current GitHub Actions workflow runs `package`, not the Kafka integration suite.

There is no `chaos` Maven profile, Kubernetes fixture, network-isolation runner,
or seeded campaign runner delivered by C12. Spec 005 defines the future budgets,
fault matrix, isolation, cleanup, and evidence requirements. Do not interpret the
commands above as running that entire campaign.

## Next implementation slices

1. Reproduce C06–C11 lifecycle gaps with controlled barriers and broker offsets;
   reconcile each with Spec 003 H01–H04 before changing production code.
2. Harden generation checks, reservation retirement, and process permit accounting.
3. Add isolated process/network faults and the remaining deterministic boundaries.
4. Build the disposable Kubernetes fixture and then the seeded C15 campaign.

Keep fault tools scoped to disposable resources. Record timeouts and cleanup
results, retain failed-run evidence, and report unsupported or unavailable tiers
separately. A successful source handoff is not final business completion; eventual
success/DLQ and safe-prefix commits need independent observations.

## Before rollout and review

Check existing group members' assignor compatibility using the
[README rollout notes](../README.md#partition-reassignment). Static membership and
the newer consumer protocol remain separate decisions. No deployment or cluster
fault execution was performed for C12.

Reviewers should match the evidence source hashes to the implementation, inspect
the actual partial-revocation assertions, and retain the pending-case ledger.
The [scenario atlas](runtime-scenario-visuals.md) illustrates expected behavior;
its larger pod/partition examples are not measured chaos results.

Learning check: does keeping a partition on a pod prove an old handler stopped
after another partition moved? No. C12 proves retained-lane continuity; C10 must
separately establish retired-handler permit accounting.
