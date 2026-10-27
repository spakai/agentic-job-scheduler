# Spec 005: Partition reassignment and chaos testing

Status: **Draft — C12 cooperative assignment implemented with focused Kafka evidence;
remaining lifecycle hardening and chaos acceptance pending**

Project: `agentic-job-scheduler`

Date: 2026-10-27

Depends on: [Spec 002](002-kafka-job-scheduling.md) and
[Spec 003](003-worker-runtime-correctness.md)

Execution status and runnable checks: [chaos runbook](../docs/CHAOS.md).

## 1. Purpose and scope

Make consumer partition reassignment safe and observable when worker pods are
added, removed, restarted, killed, or disconnected. Demonstrate recovery using
repeatable faults and assertions at execution, handoff, and commit boundaries.

Here, “repartitioning” means redistributing existing partitions among consumer
group members. Topic partition counts stay fixed. Increasing topic partitions
changes keyed routing and needs a separate migration design; neither startup nor
the chaos harness may expand application topics. Broker replica reassignment is
also outside this increment.

Spec 002 remains authoritative for job semantics. This spec makes its B03, B05,
B09, B10, B11, B14, B15, B16, B18, and B19 requirements testable under membership
churn, and extends evidence for Spec 003 H01–H04. Those requirements are not
considered complete merely because an earlier task record reports partial tests.
Spec 004 remains reserved for performance/scale experiments; this is Spec 005 as
anticipated by Spec 003.

Deliver lifecycle hardening, deterministic fault tests, a reproducible chaos
harness, and a disposable Kubernetes validation fixture/runbook. Production
deployment, throughput targets, databases, EDR, delayed jobs, state topics,
RocksDB, durable deduplication, and external-effect fencing remain out of scope.
Writing this spec does not authorize running faults against existing clusters.

## 2. Preserved guarantees

- One handler per jobId per topic under stable ownership; different job IDs may
  execute concurrently. Main and retry groups remain independent, including
  permitted overlap for the same jobId. Retry workers consume retry directly.
- Commit only the next offset after a completed delivered prefix, accounting for
  offset gaps. Completion requires handler success or acknowledged retry/DLQ
  publication. Do not commit merely because cancellation was requested.
- Delivery remains at least once. Crashes after external success or handoff
  acknowledgment but before commit may cause replay and duplicate retry chains.
- Ownership generations protect local state, not remote side effects. A stalled
  old handler can overlap a replacement after ownership loss. Test handlers must
  be replay-safe and concurrency-safe; idempotency does not stop computation.
- Every actual handler holds a process-wide permit until its future confirms
  settlement, including handlers from retired assignments. A limit reduction
  allows existing work to finish but prevents starts while usage is at the cap.
- Kafka retention bounds recovery. Fault campaigns keep records within retention;
  expired-offset behavior is tested separately under Spec 003 H06.

## 3. Rebalance configuration and membership

Decision updated 2026-10-27 by explicit user direction: both worker roles use
dynamic membership, `group.protocol=classic`, and only
`org.apache.kafka.clients.consumer.CooperativeStickyAssignor` in
`partition.assignment.strategy`. This supersedes the initial draft's decision to
retain Range as the default and test cooperative assignment separately.

The partial-revocation test exercises the actual worker configuration: unaffected
partitions must retain their queues, generations, and progress. Kafka's default
Range-first list does not enable cooperative assignment; verify the negotiated
assignor through the broker. [Kafka consumer configuration](https://kafka.apache.org/42/configuration/consumer-configs/)

Cooperative assignment reduces unnecessary movement; it does not pin a jobId to
a pod or defer reassignment until a handler finishes. Before rolling this change
into an existing group, verify every member supports the cooperative assignor;
Range-only members need a staged compatible rollout or a coordinated restart.
The newer consumer protocol and static membership require separate compatibility
decisions and are not acceptance requirements here.

The harness records group IDs, member IDs, assignments, session/heartbeat/poll
timeouts, fetch bounds, delivery timeout, concurrency/rate limits, drain budget,
and protocol/assignor. Test configuration must be isolated from production defaults.
Main and retry scaling are exercised separately; adding retry pods increases the
aggregate retry budget because limits are per pod.

## 4. Ownership lifecycle

Identify each local assignment with `(process instance, topic, partition,
generation)`. Reacquiring the same partition creates a fresh generation. Every
retained delivery, queued start, timer, handoff, and commit request carries its
owner identity; a partition lookup alone is insufficient.

| State | Permitted behavior |
| --- | --- |
| Active | Admit current-generation deliveries and start work subject to gates and permits |
| Revoking | Stop admission/starts immediately; invalidate delayed starts; snapshot a safe completed prefix while commit rights still exist |
| Lost | Invalidate immediately; do not initiate source commits |
| Retired | Finish local cleanup only; no new handler, handoff retry, or source commit |

Normal revocation offers a commit opportunity; lost partitions may already have
another owner. Kafka distinguishes these callbacks explicitly.
[Kafka rebalance listener contract](https://kafka.apache.org/42/javadoc/org/apache/kafka/clients/consumer/ConsumerRebalanceListener.html)

Verify the pinned Vert.x adapter's actual callback ordering, thread/context, and
commit behavior before implementation. Do not assume an asynchronous callback
extends Kafka's revocation commit window. If a safe final commit cannot be made,
discard that opportunity and allow replay. A lost-ownership path must never use
the graceful final-commit path. If the adapter cannot distinguish them, use the
conservative no-final-commit behavior and document it.

All lifecycle/accounting mutations stay serialized on the owner context. Do not
block the poll thread for handler drainage. Signal cancellation to active revoked
handlers and observe settlement within a bounded reporting interval; expiry
reports stuck work and does not manufacture completion or free its permit.

## 5. Retained work and resource retirement

Revocation must retire every category below exactly once. Releasing a local
reservation is distinct from marking a Kafka source complete.

| Work category at revocation | Required retirement |
| --- | --- |
| Fetched, not admitted (`pendingBatch`) | Discard revoked-generation records for replay; preserve unaffected records and exact count/byte accounting |
| Queued handler or retry-rate request | Remove/invalidate immediately; never start or fabricate a failure/handoff |
| Running handler | Request cancellation; retain permit and reservation until actual settlement; ignore stale outcome for source completion |
| Completed behind an unfinished offset | Discard retired completion tracking and release its reservation once no local operation references it; do not bridge the unfinished gap |
| Handoff awaiting retry timer | Cancel the timer; do not initiate another send for a retired generation |
| Handoff send already in flight | Keep bounded accounting until send settlement; ambiguous success may replay as a duplicate; stale acknowledgment cannot complete the source |
| Commit queued, not submitted | Drop stale request and retire associated bookkeeping; never submit under a replacement generation |
| Commit already submitted | Observe outcome without mutating a new owner; account for possible broker acceptance and release bookkeeping exactly once |

Filter old retained batches before admission, including when the same partition
is reacquired. Mixed-generation batches must not close a healthy worker, overwrite
new reservations, or dispatch old records as new deliveries.

Track retired handlers and outstanding sends outside the active-owner map so
reassignment cannot reset resource usage. Once all controlled operations settle,
retired resources must return to zero and intake must resume below low watermarks.
Unconfirmed operations remain visible and bounded; a stuck handler may prevent
local progress until process replacement. This is an explicit limitation.

## 6. Commit discipline

Keep commit submission serialized. Validate owner generation and commit rights
immediately before submission, not only when enqueuing. Final revocation commits
may include only the already-completed prefix and must finish within the client's
valid ownership window. Late callbacks cannot retry a commit for a retired owner.

A commit already submitted before loss cannot be recalled. Tests distinguish its
possible broker acceptance from an illegal new stale submission. Assert that no
commit passes unfinished work and that old queued requests cannot move a newly
assigned partition's offset backward. Recovery always reconstructs queues from
Kafka's committed position, with no durable in-memory-state assumptions.

## 7. Shutdown and pod failure

On SIGTERM: mark readiness false, stop intake and new handler starts, invalidate
queued retry starts, continue polling paused assignments while draining active
work, commit safe prefixes while permitted, then close clients. Retained queued
work may be discarded for replay. Readiness alone does not remove Kafka ownership.

Use the Spec 002 30-second maximum drain by default. Bound client close and total
shutdown separately. The Kubernetes fixture starts with a 60-second termination
grace and a documented total shutdown budget of at most 50 seconds; measure and
adjust the fixture if its runtime cannot meet that budget. Any preStop duration
must fit inside the same grace period. Kubernetes sends termination signals and
eventually forces termination when grace expires.
[Kubernetes pod termination](https://kubernetes.io/docs/concepts/workloads/pods/pod-lifecycle/#pod-termination)

SIGKILL/OOM/node loss offers no cleanup guarantee. Detection and reassignment take
time; recovery is measured from fault injection and from restored connectivity,
not assumed instantaneous. Include a deliberately shortened grace test to prove
forced termination still leaves unfinished work replayable.

## 8. Test harness and independent assertions

Use synthetic envelopes, unique test groups/topics, deterministic handler barriers,
monotonic timestamps, bounded waits, and a recorded random seed. Barriers expose
fetch-before-admission, handler start/settlement, send-before/after acknowledgment,
and commit enqueue/submission/acknowledgment. Never use a sleep alone as evidence
that a race was exercised. Test hooks must not become a production control API.

An independent observer records broker committed offsets and a bounded test event
ledger: process/generation, source coordinates, logical identity/attempt, starts,
settlements, sends, acknowledgments, revocations, and resource snapshots. Record
publication occurrences as well as stable handoff IDs, so duplicate sends are
visible. Keep identities in test artifacts/logs, not high-cardinality metrics.

The oracle checks completed-prefix safety, permit usage across active and retired
owners, stable-owner same-job exclusion, eventual replay, and eventual resource
retirement. Compare all acknowledged input records with handler successes or
acknowledged terminal DLQ outcomes after faults cease; retry handoff alone is not
final completion. Allow duplicates and cross-topic overlap. Never equate zero lag
alone with completed business work or assert exactly-once effects.

## 9. Acceptance matrix

C12 has focused Kafka evidence in the [C12 task record](../docs/tasks/C12-cooperative-sticky.md):
both main/retry roles, two join/departure cycles each, retained lane continuity,
safe observed offsets, and resource retirement for the exercised states. The
other cases remain planned under this spec. Run applicable lifecycle cases for
both main and retry roles. Use separate processes for crash/isolation cases.

| ID | Fault / controlled boundary | Required evidence | Traceability |
| --- | --- | --- | --- |
| C01 | Scale 1 → 2 → 4 members with 3 fixed partitions, then 4 → 1 | Stable assignments cover all partitions without duplicate group ownership; excess member can be idle; work resumes and offsets remain safe | B03/B11 |
| C02 | Graceful member removal with queued and active work | No starts after shutdown boundary; completed prefix commits where permitted; replacement replays unfinished work; exit meets budget | B05/B11/B19, H02 |
| C03 | Kill worker while handler is active | Incomplete source remains replayable; replacement consumes it; no cleanup callback is required | B09/B19 |
| C04 | Kill after handler success, and separately after retry/DLQ acknowledgment, before source commit | Replay/duplicate handoff is observed; identities and attempt semantics remain stable | B09/B10/B16 |
| C05 | Isolate one worker from Kafka beyond session timeout while its handler stays alive, then reconnect | Replacement starts; old callbacks never complete new-owner state or initiate stale commits; permitted cross-owner overlap is recorded | B11/B18/B19 |
| C06 | Revoke while fetch batch is waiting for window admission; include mixed partitions and same-partition reacquisition | No stale batch admission, missing-owner crash, duplicate reservation overwrite, or lost unaffected records | B11/B14 |
| C07 | Hold offset 10, complete 11, queue 12; revoke near high watermark; repeat | No commit past 10; completed/queued reservations retire, running reservation retires only on settlement; window returns to baseline | B05/B14/B15, H01 |
| C08 | Revoke with handoff retry timer pending and separately send in flight | No new retired-generation sends; in-flight outcome retires resources without source completion; duplicates allowed | B11/B16, H01 |
| C09 | Delay commit callbacks with another commit queued, revoke and reacquire | Stale queued commit is not submitted; in-flight result cannot mutate replacement tracker or cause offset regression | B05/B11/B15 |
| C10 | Rebalance while old handlers remain active; also lower limit 4 → 2 | Retired handlers count toward process usage; no replacement starts at/above cap; settlement releases permits once | B19, H03 |
| C11 | Revoke/close retry owner with starts waiting behind rate gate | No invalidated handler start or fabricated retry/DLQ; surviving work respects rate and concurrency | B11/B19, H02/H03 |
| C12 | Cooperative partial revoke and repeated member churn | Unaffected partitions keep generation/state and make progress; only moved partitions rebuild | B11/B14, H04 |
| C13 | Broker outage during handler completion/handoff/commit, then restart with storage intact | Bounded retained state, no premature commit, eventual retry/commit/poll recovery | B16/B18 |
| C14 | Short termination grace and handler that ignores cancellation | Forced death and replacement recover; no local release before settlement; no global-exclusion claim | B09/B19 |
| C15 | Seeded campaign combining joins, removals, process kills, and temporary isolation | All safety assertions hold throughout; after healing and barrier release, all finite input reaches terminal outcomes and retired resources clear | B03/B05/B09/B11/B14/B18 |

## 10. Execution tiers and recovery budgets

1. **Unit:** deterministic generations, resource retirement, permit admission,
   timer invalidation, and stale commit queue tests. Negative cases intentionally
   deliver callbacks after retirement and must leave new-owner state untouched.
2. **Kafka integration:** real Kafka through Testcontainers plus worker processes
   and a scoped network-fault mechanism. Cover C01–C13 and process-level C14;
   run each deterministic scenario independently before the combined campaign.
   A single broker proves restart recovery, not replicated broker availability.
3. **Kubernetes:** a disposable namespace/cluster verifies actual pod scale-up,
   scale-down, SIGTERM, forced termination, and worker network isolation. Run C01,
   C02, C03, C05, C14, and C15 here before claiming pod-level acceptance. Node
   failure/OOM scenarios are optional extensions and must be reported separately;
   process kill is not proof of either mechanism.

For deterministic tests, publish detection, rebalance, replay, and final-drain
deadlines before execution. Derive them from effective session/poll timeouts,
handler barriers, retry rate, input count, and environment allowance. Record each
phase's elapsed time; deadline expiry fails the case. Do not increase deadlines
after failure without recording why and preserving the failed run.

Initial C15 campaign: 3 fixed partitions per consumed topic, a finite 300-record
synthetic input set, 1–4 main and 1–3 retry workers, at least 30 membership/fault
events over 10 minutes, then a fault-free drain with all barriers released. Run
three recorded seeds per supported assignor. Input includes repeated job IDs,
success, transient failure, and permanent failure. Compute drain deadline using
the maximum possible configured attempts and retry rate; treat this as a
correctness campaign, not a performance benchmark or universal recovery SLO.

## 11. Isolation, observability, and evidence

Fault tools must require an explicit disposable target and run identifier, enforce
namespace/container/topic allowlists, bound each disruption, and restore network
rules/processes in cleanup even on assertion failure. Preserve failed-run artifacts
before teardown. Do not use production credentials or payloads. Missing Docker,
Kubernetes, or fault permissions is an unavailable environment, never a passing test.

Expose bounded metrics for current/retired handlers, pending sends/commits, retained
records/bytes, assignment changes, stale callback rejection, cancellation age,
rebalance duration, and recovery progress. Structured test logs link events to
source coordinates and generation. Watch oldest-unprocessed age as well as lag.

Each run retains commit SHA plus dirty diff identity, versions/images, effective
configuration, seed, fault timeline, observed assignment changes, test ledger,
broker offset snapshots, deadlines, assertions, exit status, and cleanup outcome.
Evidence lives under `docs/tasks/evidence/` with a task record linking each C ID to
its command and result: planned, passed, failed, skipped, or unavailable.

Planned standard commands are `mvn -B -ntp test`, `mvn -B -ntp package`, and
`mvn -B -ntp verify` for unit, artifact, and Failsafe integration validation.
Define explicit chaos-profile and Kubernetes runner commands in the implementation
task before running them; no such runner is claimed to exist in this draft.
Unit/package success does not establish Kafka or Kubernetes behavior.

## 12. Delivery slices and completion

1. Reproduce lifecycle gaps with deterministic tests: C06–C11; reconcile H01–H04
   task claims with actual coverage before editing implementation.
2. Harden owner generations, retirement, process permits, and commit submission;
   validate unit tests and broker-backed regression cases.
3. Add explicit rebalance profiles and membership/crash/network cases C01–C05,
   C12–C13. Verify pinned adapter behavior and document limitations.
4. Add Kubernetes fixture, shutdown budget checks, and C14.
5. Run C15, retain failed and successful evidence, and update architecture/runbook
   claims only to the level demonstrated by each tier.

Each slice uses a feature branch and task record. Completion requires all mandatory
cases passing at their required tiers, no unexplained permit/reservation growth,
no unsafe source commit, and a documented replay/concurrency boundary. An absent
environment leaves acceptance pending. Fixes to Spec 003 requirements remain
traceable there rather than being relabeled as new guarantees.

Learning check: if a replacement owns a partition while the old handler is still
running, which state may be discarded and which permit must remain held? The
answer must be supported by C05/C10 evidence, not inferred from ownership alone.
