# Task DEMO1201: Run the 1,200-record demo through direct Kafka or HTTP admission

Status: ready for review

## Scope and plan

- Requirement / issue: Exercise the existing 1,200-record demo through either
  direct Kafka publication or the Spec 006 HTTP admission boundary.
- Spec 002 acceptance IDs: B01, B04, B14, B15. Spec 006: API01.
- Base commit and branch: `a57993e` on
  `feature/demo-http-transport`.
- Authorized scope and unresolved decisions: Preserve the existing direct
  producer behavior and add a transport selector; no separate duplicate demo is
  created. The user did not request changes to worker semantics.
- Expected behavior and invariant: Both transports create the same canonical
  1,200-record workload. HTTP success means the API returned `202` for a
  broker-acknowledged canonical envelope; it does not mean execution completed.
  The existing handler, partition, concurrency, commit-prefix, and lag checks
  remain transport-independent.
- Files/components expected to change:
  `SampleProducerMain`, its unit tests, Compose tool configuration,
  `run-demo-1200.sh`, and the demo guide.
- Steps and validation approach: Add argument and request-contract tests, implement
  the HTTP publisher and shared workload, validate shell syntax, run `mvn test`
  and `mvn package`, then record any optional live Compose validation separately.
- Non-goals: No HTTP status/query API, durable admission deduplication, automatic
  retry after ambiguous publication, retry/DLQ load, or worker architecture
  changes.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: Git, Java
  21 Maven, Bash, Docker Compose, and local curl within this repository.
- Read/write operations needed: Java demo driver and tests, shell runner,
  Compose configuration, and directly related documentation.
- Environment constraints and recovery path: Live validation requires Docker and
  a running Kafka/HTTP/worker Compose stack. A failed or timed-out HTTP admission
  is not retried because its outcome may be ambiguous.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  No external credentials or remote systems are used; production TLS,
  authentication, and authorization remain outside this demo.

## Progress and decisions

- 2026-11-25: Implemented a branch-local transport selector with
  `direct-kafka` as the default and `http` as the alternative. Both use the same
  seeded partition sequence and canonical job payload.
- 2026-11-25: HTTP mode includes stable job and execution IDs and verifies the
  response identity before counting a request as accepted.
- 2026-11-25: Existing unrelated graph refinements in the working tree were
  preserved and are not part of the transport implementation.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| B01/API01 canonical HTTP request | `a57993e` + working tree | `mvn -B -ntp -Dtest=SampleProducerMainTest test` | passed, 8 tests | `target/surefire-reports/com.example.agenticjobscheduler.runtime.SampleProducerMainTest.txt` |
| CLI mode selection and negative option cases | `a57993e` + working tree | `mvn -B -ntp -Dtest=SampleProducerMainTest test` | passed, 8 tests | `target/surefire-reports/com.example.agenticjobscheduler.runtime.SampleProducerMainTest.txt` |
| Runner syntax | `a57993e` + working tree | `bash -n scripts/run-demo-1200.sh` | passed | command exit 0 |
| Unit regression suite | `a57993e` + working tree | `mvn -B -ntp test` | passed, 162 tests | `target/surefire-reports/` |
| Library artifact | `a57993e` + working tree | `mvn -B -ntp package` | passed, 162 tests; JAR produced | `target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar` |
| Direct live demo | working tree | `./scripts/run-demo-1200.sh --transport direct-kafka` | not run | direct path already covered by earlier recorded demo evidence |
| HTTP live demo | `a57993e` + working tree | `./scripts/run-demo-1200.sh --transport http`, then final evidence assertion | 1,200 admitted and handled; finalized after lag-parser correction | `demo_http.md`, `docs/tasks/evidence/DEMO1200-http-concurrency-summary.txt` |

Include a negative case that disproves the claim: HTTP receives a non-202
response or a 202 response with mismatched identities; publication must fail and
the load must not report 1,200 accepted.

## Failure analysis and correction

- Observed failure and reproduction: The first live attempt admitted and handled
  all 1,200 records but the runner reported one lag row. Kafka output began with
  a blank line, and the parser counted the `LAG` header as a partition row.
- Classification: tool use / stale output parsing.
- Root cause supported by evidence: `NR > 1 && $6 != "0"` counts the Kafka
  table header; every data row had lag zero.
- Code, instruction, or tool change: Added an HTTP adapter and response-identity
  check behind a transport selector, changed lag parsing to
  `$1 == "job-main-v1"`, and changed the startup check to HTTP liveness because
  producer readiness becomes true after the first successful publication.
- Rerun and remaining uncertainty: Focused tests, full unit tests, and package
  build pass. The same HTTP run was finalized from captured logs without
  republishing. Final Kafka state showed zero lag on all 12 partitions.

## Handoff and review

- Latest verified commit and working-tree changes: base `a57993e`, implementation
  remains uncommitted in `feature/demo-http-transport`.
- Completed steps: shared transport selector, HTTP publication, focused tests,
  Compose wiring, demo documentation, unit suite, and package build.
- Next concrete step: review and commit the implementation and HTTP evidence.
- Open risks/decisions and owner: Direct mode was not rerun in this session.
- Reviewer findings and disposition: no findings from local diff and validation review.
- PR / final outcome: pending.

## Learning check

- What invariant did this task establish? Unit validation establishes that the
  selected HTTP transport uses the canonical request shape and fails on
  non-accepted/mismatched responses. The live HTTP run established that the
  shared 1,200-record workflow reaches 1,200 unique successful handlers, one
  permitted replay line, and zero final lag on all 12 partitions.
- What failure would invalidate the claim? Different payload identities,
  partition distribution, handler count, or lag result between modes.
- Which artifact proves what actually happened? Focused unit reports, Maven
  results, the generated demo summary, logs, and graphs from any live runs.
