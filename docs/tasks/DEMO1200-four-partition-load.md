# Task DEMO1200: Four-partition 1,200-record demo

Status: in progress

## Scope and plan

- Requirement / issue: Update the Compose demo to match the Kafka polling and
  partition-offset diagrams.
- Spec 002 acceptance IDs: B05, B14, B15, B20.
- Base commit and branch: feature branch
  `docs/explain-job-key-partitioning`; base commit `367dd08`.
- Authorized scope: demo producer, demo handler logging, Compose defaults,
  focused tests, and documentation.
- Expected behavior and invariant: publish exactly 300 success records to P0,
  P1, P2, and P3; all 1,200 should stop successfully, appear in timestamped
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

2026-11-18: Added `--load-1200` producer mode, 4-partition Compose defaults,
and ISO/timestamp plus load-sequence handler logging.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| 4x300 routing | 74622b2+working | `mvn test` | passed: 136 tests, including 1,200-key routing | `target/surefire-reports/` |
| Library package | 74622b2+working | `mvn package` | passed | `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` |
| Compose configuration | 74622b2+working | `docker compose config --quiet` | passed | no artifact |
| Docker daemon | 74622b2+working | `docker version --format '{{.Server.Version}}'` | available: 29.3.1 | no artifact |
| 1,200 live records | pending | Docker Compose load | blocked by existing 12-partition persistent topics | existing Compose state |
| 1,200 handler logs | pending | Compose worker logs | blocked by existing 12-partition persistent topics | existing Compose state |
| P0-P3 lag zero | pending | Kafka consumer-groups describe | blocked for the 4-partition load | existing group reports lag zero on 12 partitions |

## Failure analysis and correction

Pending implementation verification.

## Handoff and review

- Latest verified commit: pending
- Completed steps: source and documentation updates
- Next concrete step: compile/test, then Docker-backed demo
- Open risks/decisions: availability of Docker, verification duration, and
  whether existing Kafka data causes offsets to start beyond the new demo run
- PR / final outcome: pending

## Learning check

- What invariant did this task establish: 1,200 records distributed evenly
  across four partitions must be processed within retained-work and commit
  boundaries.
- What failure would invalidate the claim: missing handler logs, any partition
  count not 300, or nonzero consumer-group lag.
- Which artifact proves what actually happened: timestamped handler logs,
  producer per-partition offsets, and Kafka group describe output.
