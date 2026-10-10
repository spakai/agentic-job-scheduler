# Agentic job scheduler

A Java 21 learning project for immediate Kafka job processing and GH-600 agent
supervision. [Spec 002](specs/002-kafka-job-scheduling.md) defines the intended
worker service. **Currently implemented: L01 envelope validation and L02 completion/
offset tracking as library components.**
There is no running Vert.x worker, Kafka consumer, limiter, or DLQ publisher yet.

## Build and test

Requirements: JDK 21 or newer and Maven 3.6.3 or newer. Compilation targets Java
21. Maven needs access to Maven Central on the first build; Docker is not required
for this increment.

```sh
mvn test
mvn package
```

Unit results are in `target/surefire-reports/`. Packaging produces
`target/agentic-job-scheduler-0.1.0-SNAPSHOT.jar`, a library without an executable
main class. `mvn verify` currently runs the same unit/package lifecycle; no Kafka
integration tests exist yet. Do not interpret a successful verify as Kafka proof.

## Validation API

`com.example.agenticjobscheduler.messaging.EnvelopeValidator` takes a set of
registered job types. Call `validateMain(byte[] key, byte[] value)` or
`validateRetry(byte[] key, byte[] value)` with raw UTF-8 bytes. The main method
returns a normalized JobEnvelope; omitted maxAttempts becomes 3. Retry validation
returns RetryEnvelope and requires the nested job's maxAttempts explicitly.

```java
var validator = new EnvelopeValidator(Set.of("demo.echo"));
JobEnvelope job = validator.validateMain(keyBytes, messageBytes);
```

On rejection it throws EnvelopeValidationException with a stable `code()` and
sanitized message. It does not attach the underlying parser exception or include
payloads. A future consumer will use the code and original record to construct
bounded DLQ output. No I/O or Kafka offset changes occur during validation.

The validator rejects unknown fields (including scheduledAt), invalid or
noncanonical UUIDs, mismatched keys, unknown job types, wrong JSON types, invalid
attempts, malformed UTF-8/JSON, duplicate properties, trailing content, and records
beyond 256 KiB main / 512 KiB retry limits. Payloads must be JSON objects and are
returned as defensive copies. JSON nesting is capped at 100 main levels (101 for the retry wrapper) and
numeric tokens at 1,000 characters. Partition routing needs Kafka metadata and is deferred.

## Completion and offset API

`com.example.agenticjobscheduler.execution.OffsetCommitTracker` tracks a fixed set
of assigned topic partitions with a configurable total retained-record bound.
Use one owner context for every call; this class is not thread-safe.

1. Register every fetched record in increasing partition-offset order with
   `delivered(SourceRecord)`, before dispatch. Retain its opaque delivery handle.
2. Call `complete(handle, outcome)` only on handler success, retry acknowledgment,
   or DLQ acknowledgment. Pending/failed sends leave the source incomplete.
3. `beginCommit()` returns an optional immutable map of explicit next offsets.
   Send that snapshot through the future Kafka adapter. It cannot cross an
   incomplete delivered record, handles offset gaps, and advances partitions
   independently. Another request cannot begin until this one finishes.
4. Report the exact request to `commitSucceeded(batch)` or `commitFailed(batch)`.
   Success frees only the acknowledged prefix; failure retains all results for
   retry. Report synchronous send failures too. Do not release an outstanding
   request merely because a local timer expired.

The bound includes completed-but-uncommitted records. Admission failure requires
the caller to retain fetched work and pause intake, never drop records. Payload
byte limits, fetch overshoot, process-wide accounting, and pause/resume are not
implemented here. Rebuild the tracker on ownership changes; the future runtime
must invalidate old callbacks and handle revocations before using a new tracker.

Tests establish B05/B15 library behavior and **simulate** B09 crash/replay using a
fake broker offset store. They do not establish live Kafka durability. Success or
handoff acknowledgment before commit can replay after a crash, including duplicate
handoffs. This follows Kafka's [manual offset/replay contract](https://kafka.apache.org/41/javadoc/org/apache/kafka/clients/consumer/KafkaConsumer.html).
The adapter, leader-epoch metadata, actual commits, and broker tests remain pending.

## Retry wire example

```json
{
  "schemaVersion": 1,
  "job": {
    "schemaVersion": 1,
    "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
    "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
    "jobType": "demo.echo",
    "payload": {},
    "maxAttempts": 3
  },
  "attempt": 2,
  "handoffId": "92a0e16a8b0cc0557cc22b7a6c6ae4b6687e2d12f8b65bb2bd9db2b347b1d5e3",
  "originalSource": { "topic": "jobs.main.v1", "partition": 0, "offset": 42 },
  "failedSource": { "topic": "jobs.main.v1", "partition": 0, "offset": 42 },
  "errorCode": "TRANSIENT_FAILURE"
}
```

Handoff IDs are SHA-256 of failed-source topic, partition, offset, and next attempt,
separated by newline characters without a final newline. They identify a handoff,
not a permanent execution-deduplication guarantee. See Spec 002 for main/retry
ordering and at-least-once delivery limitations.

## Review and learning

Start with the [L01 task record](docs/tasks/L01-envelope-validation.md),
[L02 task record](docs/tasks/L02-completion-offsets.md), and the
[GH-600 learning guide](docs/gh-600-learning-guide.md). B01 validation is covered;
B17's invalid-input rejection is covered but wrong-partition detection and actual
DLQ publication are not. Unit tests do not demonstrate handler safety, broker
handoffs, concurrency, or retry execution.

Pinned baseline dependencies: [Jackson 2.20.1](https://github.com/FasterXML/jackson/wiki/Jackson-Release-2.20.1)
and [JUnit 5.14.1](https://docs.junit.org/5.14.1/_exports/junit-user-guide-5.14.1.html).
No security scan has been performed as part of L01.
