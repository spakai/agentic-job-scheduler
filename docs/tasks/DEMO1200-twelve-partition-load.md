# Task DEMO1200: Twelve-partition 1,200-record demo

Status: complete

## Scope and plan

- Requirement / issue: Update the Compose demo to match the Kafka polling and
  partition-offset diagrams.
- Spec 002 acceptance IDs: B05, B14, B15, B20.
- Base commit and branch: feature branch
  `docs/explain-job-key-partitioning`; base commit `367dd08`.
- Authorized scope: demo producer, demo handler logging, Compose defaults,
  focused tests, and documentation.
- Expected behavior and invariant: publish exactly 100 success records to P0
  through P11; all 1,200 should stop successfully, appear in timestamped
  handler logs, and leave the main consumer group at lag zero.
- Files/components expected to change: `SampleProducerMain`,
  `DemoJobHandler`, `compose.yaml`, `README.md`, focused unit test, this record.
- Steps and validation: run unit tests and package; run Docker Compose if Docker
  and daemon access are available; compare handler count and group lag.
- Non-goals: no database, no durable deduplication claim, no global ordering,
  and no external-effect fencing.

## Tools and boundaries

- Tool, purpose, repository/environment scope: local repository and Maven/Docker
  CLI with repository environment.
- Read/write operations needed: source edits, tests, generated evidence.
- Environment constraints: Docker-backed verification requires the Docker daemon
  and broker container availability.
- Evidence of enforced restrictions: no payload or credential logging.

## Progress and decisions

2026-11-18: Added `--load-1200` producer mode, 12-partition Compose defaults,
and ISO/timestamp plus load-sequence/mode handler logging. The first live trial
set the failure flag incorrectly; after correcting `--fail` detection, the
success load was rebuilt and rerun.

2026-11-18: Live concurrency run passed: producer accepted 1,200 records
(100 each on P0 through P11); 1,200 timestamped `loadMode=success` handler
lines and 1,200 unique sequences were observed; Kafka group lag was zero on
P0 through P11. The 100 ms handler hold measured `peakActive=16`.

2026-11-18: Repeated the live load with two main-worker replicas. Both pods
processed 600 success records and observed `peakActive=16`; P0-P11 lag stayed
zero and partition ownership was split across two stable consumer members.

2026-11-18: Added `scripts/run-demo-1200.sh` and
`scripts/generate-demo-concurrency-graph.py`. The runner assigns a unique
`demoRunId`, waits for 1,200 matching handler lines and lag zero, then writes
the SVG and evidence summary automatically.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| 12x100 routing | eca1ee9+working | `mvn test` | passed: 137 tests | `target/surefire-reports/` |
| Library package | eca1ee9+working | `mvn package` | passed | `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` |
| Compose configuration | eca1ee9+working | `docker compose config --quiet` | passed | no artifact |
| Docker daemon | eca1ee9+working | `docker version --format '{{.Server.Version}}'` | available: 29.3.1 | no artifact |
| 1,200 live records | working | Docker Compose load | passed: 1,200 accepted, 100 per partition | [summary](evidence/DEMO1200-concurrency-summary.txt) |
| 1,200 handler logs | working | Compose worker logs | passed: 1,200 success lines, 1,200 unique sequences | [summary](evidence/DEMO1200-concurrency-summary.txt) |
| Concurrent in-flight handlers | working | 100 ms demo handler | passed: peakActive=16, bounded by main concurrency | [summary](evidence/DEMO1200-concurrency-summary.txt) |
| P0-P11 lag zero | working | Kafka consumer-groups describe | passed: zero lag for all 12 partitions | [group offsets](evidence/DEMO1200-concurrency-group-offsets.txt) |

## Failure analysis and correction

The initial live command was classified as failure mode because argument
parsing used "one argument means fail" for `--load-1200`. It was corrected to
enable failure only for `--fail`, and the success run was rebuilt and verified.

## Handoff and review

- Latest verified commit: `eca1ee9`
- Completed steps: 12x100 routing, live producer acceptance, 1,200 handler
  log lines, and P0-P11 lag verification
- Next concrete step: commit evidence and PR updates
- Open risks/decisions: existing data makes offsets start at prior positions;
  the 1,200 success run is distinguishable by `loadMode=success`
- PR / final outcome: pending commit

## Learning check

- What invariant did this task establish: 1,200 records distributed evenly
  across twelve partitions must be processed within retained-work and commit
  boundaries, while only the configured concurrency limit may execute
  handlers simultaneously.
- What failure would invalidate the claim: missing handler logs, any partition
  count not 100, `peakActive` not above 1, or nonzero consumer-group lag.
- Which artifact proves what actually happened: timestamped handler logs,
  producer per-partition offsets, and Kafka group describe output.
