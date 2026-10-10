# L06: Bound retained work and demonstrate recovery

Status: in progress

## Scope and plan

- Requirement: implement the Spec 002 L06 backpressure/recovery increment,
  acceptance IDs B14–B20.
- Base commit and branch: merged L05 commit `8476630acaeea21bf1cc8538c2224e96b031f0b9`;
  branch `feat/l06-backpressure-recovery`.
- Authorized scope and unresolved decisions: implement on the current Kafka-first
  architecture. A clean merged-main baseline passes. Docker is now available.
  The repository currently contains library components, not an executable
  Kafka worker, so worker wiring and integration fixtures are in scope where
  required by B14–B20; preserve the independent main/retry groups and no-store
  architecture.
- Expected behavior and invariant for the current code slice: one
  `ProcessingWindow` can be shared across intake paths; reservations track record
  count and key/value byte count until the corresponding source commit is
  acknowledged. It pauses at either adjusted high watermark, allows at most one
  configured fetch overshoot, and resumes only below 50% of both watermarks.
  Complete L06 additionally requires a Kafka adapter/runtime for B14–B20; this
  component alone is not claimed to deliver all those cases.
- Files/components changed so far: `ProcessingWindow`, focused unit tests,
  README/architecture documentation, and this task/evidence record.
- Steps and validation approach: establish a clean merged-main baseline; verify
  official Vert.x Kafka pause/resume and Testcontainers documentation; implement
  count/byte watermarks, one-fetch headroom, acknowledgment-bound reservations,
  and negative tests; then exercise the pause/poll/commit/resume boundary against
  a Testcontainers Kafka broker. The Vert.x adapter/runtime, outage integration,
  Dockerfile, Compose/demo artifacts, and remaining B14–B20 coverage are
  outstanding.
- Non-goals: PostgreSQL/Flyway/REST, delayed scheduling/state topics/RocksDB,
  cross-topic exclusion, durable deduplication, global ordering, exactly-once
  effects, or external-effect fencing.

## Tools and boundaries

- Tool, purpose, repository/environment scope, effective permissions: Maven, Git,
  Docker/Compose, official documentation, and the Maven ecosystem CVE assessment,
  used only for this repository and uniquely named L06 test resources. Docker is
  shared; existing containers are not to be stopped or modified.
- Read/write operations needed: modify source/build/test/docs and add container
  artifacts; run project builds/tests and short-lived project-owned containers.
- Environment constraints and recovery path: Java 21/Maven and Docker Desktop
  available. No existing Kafka application runtime exists. Keep external runtime
  failures separate from unit test outcomes; stop only containers created for
  this task.
- Evidence of enforced restrictions, or explicitly unverified restrictions:
  shell/Git/Docker are unrestricted local capabilities; project scope is an
  instruction, not a technically enforced sandbox.

## Progress and decisions

- 2026-10-10: verified PR #6 merged as `8476630`; fast-forwarded local `main`,
  created `feat/l06-backpressure-recovery`, and passed `mvn -B -ntp clean test`
  on the merged baseline (115 tests, no failures/errors/skips).
- 2026-10-10: after Docker initially was unavailable in the WSL distro, the user
  reported Docker was starting. `docker version`, `docker compose version`, and
  `docker ps` now succeed. Existing `spec007-worker` containers were observed and
  will not be modified.
- 2026-10-10: confirmed official Vert.x KafkaConsumer documentation exposes
  per-partition pause/resume and assignment/revocation callbacks; confirmed
  Testcontainers official Kafka module documentation recommends its newer
  `org.testcontainers.kafka.KafkaContainer` API over the deprecated class.
- 2026-10-10: implemented `ProcessingWindow`, a process-shareable record/byte
  reservation ledger. The high watermark reserves headroom for one configured
  fetch; resume requires both retained count and bytes below half their adjusted
  high watermarks. Reservations remain held after a failed commit and are released
  only after the tracker commit succeeds.
- 2026-10-10: initially retained `ProcessingWindow` as a library component
  rather than wiring unimplemented Kafka consumers. Docker was available, but
  broker-backed evidence was still pending at that point; the following
  integration test supersedes that evidence state.
- 2026-10-10: added `ProcessingWindowKafkaIT`, a Testcontainers-backed B14
  integration case. It publishes eight records, reserves through the count
  watermark, pauses the real Kafka partition, continues polling without receiving
  paused records, acknowledges offset 4, releases the reservations, resumes, and
  consumes the remaining records.
- 2026-10-10: the full resolved Maven graph scan found eight advisories across
  Commons Compress 1.24.0 and LZ4 Java 1.10.1; a follow-up found one advisory in
  Commons Lang 3.14.0 pulled by the first patch. Overrode the resolved versions
  to patched releases, added Commons Codec needed by Commons Compress at
  Testcontainers startup, and rescanned the whole graph with no findings.

## Evaluation

| Check | Commit | Command or workflow | Result | Evidence |
| --- | --- | --- | --- | --- |
| Merged L05 baseline | `8476630` | `mvn -B -ntp clean test` | passed: 115 tests, 0 failures/errors/skips | session output; retain final L06 evidence |
| Processing-window component (partial B14) | working tree | `mvn -B -ntp -Dtest=ProcessingWindowTest test` | passed: 5 tests, 0 failures/errors/skips | [L06 validation evidence](evidence/L06-validation.txt) |
| Kafka pause/poll/commit/resume (partial B14) | working tree | `mvn -B -ntp clean verify` | passed after dependency fixes: 1 Testcontainers Kafka integration test; 120 unit tests also passed | [L06 validation evidence](evidence/L06-validation.txt) |
| Maven dependency CVE scan and patched rescan | working tree | `appmod-cve-assessment` against the resolved Maven test graph | final rescan clean; initial and intermediate findings fixed | session artifact `files/l06-cve/` |
| Existing safe-prefix and handoff boundary (B15/B16 library coverage) | `8476630` + L06 tree | `mvn -B -ntp clean package` | passed within 120-test suite; not a Kafka publisher test | [L06 validation evidence](evidence/L06-validation.txt) |
| Existing simulated crash/replay, invalid envelope, and STUCK handler cases (partial B17/B18/B19) | `8476630` + L06 tree | `mvn -B -ntp clean package` | passed within 120-test suite; only test doubles/library behavior | [L06 validation evidence](evidence/L06-validation.txt) |
| Full B14–B20 worker runtime acceptance, including Vert.x adapter, outage/replay and Compose (B20) | n/a | runtime and Docker artifacts not implemented | not run | no full-runtime claim |
| Full clean build, unit suite, broker-backed integration, and package | L06 working tree | `mvn -B -ntp clean verify` | passed: 120 unit tests and 1 Kafka integration test; JAR built | [L06 validation evidence](evidence/L06-validation.txt) |
| Whitespace | L06 working tree | `git diff --check` | passed | [L06 validation evidence](evidence/L06-validation.txt) |
| Editor diagnostics | L06 working tree | Problems panel for `ProcessingWindow` and its test | no errors | [L06 validation evidence](evidence/L06-validation.txt) |
| Docker/Testcontainers broker | working tree | `mvn -B -ntp verify` | passed; L06 Kafka container started and cleaned up; pre-existing `spec007-worker` containers untouched | [L06 validation evidence](evidence/L06-validation.txt) |
| CI/security/restricted-agent exercises | n/a | workflows/scans not yet selected | not run | no claim |

Negative window tests reject fetch-bound violations, prevent new reservations
while paused, prevent foreign/double release, and verify that a failed commit
retains the window reservation. Remaining B14–B20 cases need the actual worker
runtime: the Vert.x adapter applying pause while polling, failed publish/DLQ
behavior, broker restart, confirmed cancellation, and Compose execution.

## Failure analysis and correction

- Observed failure and reproduction: the first window test run failed because
  tests passed batches larger than their configured one-fetch limits. The next
  run exposed two incorrect assertions: a test expected resume before both
  watermarks were below half, and an overshoot test expected a rejected batch to
  be counted. The first clean verify after the dependency CVE overrides then
  failed at Testcontainers startup with
  `NoClassDefFoundError: org/apache/commons/codec/Charsets`.
- Classification: test design/reasoning.
- Root cause supported by evidence: test fixtures contradicted the window's
  configured maximum fetch values and low-water hysteresis; after pause, a
  refused reservation intentionally leaves counters unchanged.
- Code, instruction, or tool change: corrected test batches and assertions to
  stay within the contract, assert hysteresis against both adjusted watermarks,
  and prove an outstanding fetch may cross the high threshold without exceeding
  the configured hard bound. Added test-scoped Commons Codec because Commons
  Compress declares it optional but Testcontainers uses the dependent API.
- Rerun and remaining uncertainty: the focused window tests, full unit/package
  suite, broker-backed pause/poll/commit/resume integration case, and final
  dependency rescan passed. Full worker-runtime integration remains outstanding.

## Handoff and review

- Latest verified commit and working-tree changes: base `8476630` on
  `feat/l06-backpressure-recovery` plus uncommitted library/docs changes;
  implementation input hashes are in
  [L06 validation evidence](evidence/L06-validation.txt).
  unrelated pre-existing `.vscode/` remains untouched.
- Completed steps: confirmed L05 merge, clean test baseline, Docker readiness,
  current spec, official API documentation, bounded window implementation,
  focused tests, and one real-broker pause/poll/commit/resume test.
- Next concrete step: implement the Vert.x Kafka worker adapter that connects
  `ProcessingWindow` to assigned partitions, then add the remaining B16–B20
  runtime/recovery cases before calling L06 complete.
- Open risks/decisions and owner: runtime scope is larger than prior library
  increments; distinguish broker-backed evidence from test doubles and preserve
  at-least-once semantics.
- Reviewer findings and disposition: pending.
- PR / final outcome: L06 remains in progress; no commit or PR created.

## Learning check

- What invariant did this task establish?
- What failure would invalidate the claim?
- Which artifact proves what actually happened?
