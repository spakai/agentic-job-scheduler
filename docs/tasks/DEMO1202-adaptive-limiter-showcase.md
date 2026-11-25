# Task DEMO1202: Demonstrate main-worker adaptive concurrency

Status: ready for review

## Scope and plan

- Requirement / issue: Add a repeatable local demo that makes healthy additive
  recovery and slow-handler multiplicative decrease observable.
- Spec 002 acceptance IDs: B13.
- Base commit and branch: `df88364` on `feature/adaptive-limiter-showcase`.
- Authorized scope and unresolved decisions: Use the existing Kafka worker and
  `demo.echo` handler in an isolated Compose project. Keep the limiter algorithm
  unchanged; add bounded observability and demo tooling only.
- Expected behavior and invariant: With a backlog, healthy windows increase the
  main worker's per-process limit by one; windows with at least 20 slow completed
  attempts halve it. The limit never cancels active work. A final healthy phase
  demonstrates recovery. Evidence distinguishes configured limit from measured
  handler concurrency.
- Files/components expected to change: Adaptive-window logging in `KafkaWorker`,
  bounded load-count support in `SampleProducerMain`, focused tests, an isolated
  demo runner and visualization, README/demo guidance, and this task record.
- Steps and validation approach: Test load-count parsing and job totals; run
  `mvn test`, `mvn package`, and shell/Python syntax checks. If Docker is
  available, run the isolated Kafka demo and verify the recorded windows show
  both decrease and recovery while all phase records commit.
- Non-goals: No limiter-policy changes, worker topology changes, retry/DLQ
  scenarios, HTTP admission, or claims about a cluster-wide limit.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: Git,
  Maven, Docker Compose, and local scripts within this repository.
- Read/write operations needed: The worker's adaptive-window log, demo
  producer/tests, standalone demo tooling, and directly related documentation.
- Environment constraints and recovery path: Live evidence requires Docker
  Compose and enough time for the controlled slow phase. If unavailable, report
  syntax/unit/build results separately and leave live evidence explicitly not
  run.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  No external services or credentials are required.

## Progress and decisions

- 2026-11-26: Started from the updated `main` containing PRs #41 and #42. Chose
  a single main-worker Compose project so measured changes are not obscured by
  per-pod limits from a second worker.
- 2026-11-26: Initial live startup found host port 9092 already allocated by
  another Compose stack. The runner now selects an overridable host port for its
  isolated broker rather than stopping or reusing the existing service.
- 2026-11-26: First live workload completed all records but its healthy windows
  reported no queued backlog, so the limiter correctly held at 16. The warmup
  and recovery handler hold is now 500 ms to keep a backlog under the direct
  producer's observed rate while remaining below the slow-window target.
- 2026-11-26: Final live run completed 1,200 warmup, 160 slow, and 1,200 recovery
  jobs with all unique handlers observed and zero lag after each phase. It
  measured a healthy increase (16 to 17), slow decrease (22 to 11), and healthy
  recovery increase (2 to 3); 34 adaptive windows and 2,560 handler starts were
  retained as evidence.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Load-count validation and regression tests | working tree at `df88364` | `mvn -B -ntp -Dtest=SampleProducerMainTest test` | passed, 10 tests | `target/surefire-reports/com.example.agenticjobscheduler.runtime.SampleProducerMainTest.txt` |
| Unit regression suite and package | working tree at `df88364` | `mvn -B -ntp package` | passed, 165 tests; JAR produced | `target/surefire-reports/` and `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` |
| Demo script syntax | working tree at `df88364` | `bash -n scripts/run-demo-adaptive-limiter.sh` | passed | command exit 0 |
| Graph generator syntax | working tree at `df88364` | `python3 -c 'import ast; ast.parse(open("scripts/generate-adaptive-limiter-graph.py").read())'` | passed | command exit 0 |
| SVG output validity | working tree at `df88364` | `python3 -c 'import xml.etree.ElementTree as ET; ET.parse("docs/diagrams/demo-adaptive-limiter.svg")'` | passed | `docs/diagrams/demo-adaptive-limiter.svg` |
| Graph negative case | working tree at `df88364` | temporary evidence with slow-window decreases removed | passed; generator rejected the evidence | generator emitted “Pressure did not show a >=20-sample slow-window limit decrease” |
| Live adaptive decrease and recovery | working tree at `df88364` | `bash scripts/run-demo-adaptive-limiter.sh` | passed; 1,200/160/1,200 unique handlers, zero lag after each phase; healthy 16→17, slow 22→11, recovery 2→3 | `docs/tasks/evidence/DEMO1202-adaptive-limiter-summary.txt`, window/handler logs, and graph |

Include a negative case: if no observed window has enough completed attempts or
the graph lacks both a decrease and subsequent increase, the demo must fail
rather than claim the adaptive behavior was shown.

## Failure analysis and correction

- Observed failure and reproduction: The first start could not bind host port
  9092. After isolating the port, the initial 100 ms healthy phase drained as
  quickly as the synchronous producer admitted records, leaving no backlog for
  an additive increase. The first 500 ms run showed correct limiter behavior
  but exposed a format-string error during SVG generation.
- Classification: Environment / test design / tool use.
- Root cause supported by evidence: Host port 9092 was already allocated;
  first-run limiter evidence had `backlogPresent=false`; the SVG formatter
  supplied three values for four placeholders.
- Code, instruction, or tool change: Added an overridable Compose host port,
  increased only the demo's healthy handler hold to 500 ms, fixed the SVG
  formatter, and persisted unique-handler/lag results per phase. Limiter policy
  and production defaults are unchanged.
- Rerun and remaining uncertainty: The complete script passed end to end after
  correction. It asserted healthy increase, slow-window decrease with at least
  20 samples, healthy recovery increase, all unique handler counts, and zero lag.

## Handoff and review

- Latest verified commit and working-tree changes: Base `df88364`; implementation
  and live evidence are uncommitted on `feature/adaptive-limiter-showcase`.
- Completed steps: Added window metrics, bounded producer load counts, isolated
  Compose runner, assertions, graph, documentation, tests, and live evidence.
- Next concrete step: Review the branch and open a PR if desired.
- Open risks/decisions and owner: The demonstration uses a controlled
  `demo.echo` timer and is not a production workload profile.
- Reviewer findings and disposition: Pending.
- PR / final outcome: Pending.

## Learning check

- What invariant did this task establish? B13 windows increase the main limit by
  one only with healthy p95, sufficient samples, and backlog; slow eligible
  windows halve it, without cancelling active handlers.
- What failure would invalidate the claim? Missing samples, no backlog-driven
  healthy increase, no slow-window decrease, no recovery increase, missing
  handler sequences, or nonzero final lag.
- Which artifact proves what actually happened? The window log, phase outcome
  TSV, unique handler log, generated graph, and summary under `docs/tasks/evidence/`
  and `docs/diagrams/`.
