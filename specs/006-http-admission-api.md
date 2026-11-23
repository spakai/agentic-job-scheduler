# Spec 006: HTTP admission API for Kafka jobs

Status: **Draft — requirements and acceptance criteria proposed; implementation
not authorized by this spec draft alone**

Project: `agentic-job-scheduler`  
Date: 2026-11-23

Depends on: [Spec 002](002-kafka-job-scheduling.md)  
Motivation: allow clients that already use HTTP to submit work without requiring
them to own a Kafka producer.

## 1. Purpose

Add an HTTP-to-Kafka admission boundary for the Kafka-only scheduler. Clients
submit jobs over HTTP; the service validates the request, constructs the
canonical Spec 002 main-topic envelope, and publishes it to `jobs.main.v1` with
the `jobId` as the record key.

An accepted HTTP response means Kafka acknowledged the record. It does not mean
a worker started or completed the job. The API never consumes worker topics to
construct a status response.

This spec does not revive the PostgreSQL scheduling model in
[Spec 001](001-job-lifecycle.md). There is no jobs table, scheduled time, state
machine, inbox/outbox, status endpoint, or query API.

## 2. Scope and boundaries

### In scope

- A versioned HTTP endpoint for submitting one job at a time.
- Request validation compatible with Spec 002's main envelope.
- Optional client-selected `jobId` and `executionId` for retry-safe HTTP calls.
- Generation of those identifiers when the client omits them.
- A bounded, asynchronous Kafka producer and its lifecycle.
- Broker-acknowledged admission responses and explicit error responses.
- Local backpressure and request-body limits before Kafka publication.
- Health, readiness, metrics, logs, and deployment configuration for an
  independently scalable API role.
- Broker-backed admission tests and HTTP contract tests.

### Non-goals

- No GET, list, cancel, update, or execution-status endpoints.
- No `scheduledAt`, delayed work, timers, or scheduler state.
- No durable admission ledger, database, duplicate-detection table, or
  transactional outbox.
- No durable idempotency guarantee for an HTTP client that repeats a request
  after a timeout.
- No synchronous execution, result retrieval, or streaming progress endpoint.
- No cross-topic jobId exclusion, global ordering, durable deduplication, or
  external-effect fencing. Main and retry execution remain independent as
  defined by Spec 002.
- No general Kafka proxy. Only the registered job types and the canonical
  `jobs.main.v1` envelope are admitted.
- No automatic publication to retry or DLQ topics from the HTTP API.
- Authentication, authorization, TLS termination, and internet exposure are
  deployment concerns. Production deployment must place the API behind an
  authenticated TLS boundary; this increment does not define anonymous public
  access.

## 3. Architecture and roles

```text
HTTP client
    |
    | POST /v1/jobs
    v
HTTP admission role
    | validate and normalize
    | generate IDs only when both are omitted
    | publish keyed canonical envelope
    v
jobs.main.v1
    |
    v
Spec 002 main worker -> jobs.retry.v1 / jobs.dlq.v1
```

The HTTP admission role is a separate process/deployment from main and retry
workers. It owns an HTTP server and Kafka producer only. It does not deploy a
consumer verticle or instantiate a job handler.

Scaling HTTP instances increases request capacity only. It does not change
consumer-group ownership, worker concurrency, topic partition counts, or retry
throughput.

The API role may share the Java 21, Vert.x, Maven, Kafka client, and Jackson
dependencies with the worker, but it must remain launchable independently through
an explicit role/entry-point configuration.

## 4. REST contract

All request and response bodies are UTF-8 JSON with camelCase field names.
Endpoints are relative to the configured public base URL.

### 4.1 Submit a job

```http
POST /v1/jobs
Content-Type: application/json
```

```json
{
  "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
  "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
  "jobType": "demo.echo",
  "payload": {
    "message": "hello"
  },
  "remainingAttempts": 3
}
```

Rules:

1. `jobId` and `executionId` are either both present or both omitted.
2. When omitted, the API generates two fresh canonical lowercase UUID values.
   If the HTTP client loses the response after broker acknowledgment, it cannot
   safely repeat that request without knowing the generated identities.
3. When present, both values must be canonical lowercase UUIDs. They are passed
   through unchanged so a client can retry an unconfirmed HTTP request with the
   same logical identity.
4. `jobType` is required, 1–128 characters, and registered for this API role.
   Registration must be consistent with the corresponding workers.
5. `payload` is required and must be a JSON object. `{}` is valid; arrays,
   scalars, and null are invalid.
6. `remainingAttempts` is optional, defaults to 3, and accepts integers from 1
   through 100. It includes the first execution and has the exact Spec 002
   meaning.
7. Unknown fields are rejected, including `schemaVersion`, `scheduledAt`,
   `notBefore`, `attempt`, `maxAttempts`, and HTTP client-supplied Kafka source
   coordinates.
8. Duplicate JSON properties, malformed UTF-8, malformed JSON, trailing content,
   type coercion, and numeric-token overflow are rejected.
9. The raw request body is limited to a configurable maximum that defaults to
   256 KiB. The serialized Kafka envelope must independently satisfy Spec 002's
   256 KiB main-record limit. A request that is valid alone but would exceed the
   envelope limit is rejected rather than truncated.
10. The API accepts only `application/json`. Parameters such as a charset are
    permitted only when the effective media type is `application/json`; any
    other content type returns `415`.

The API sets `schemaVersion` to 1 and serializes this exact main envelope:

```json
{
  "schemaVersion": 1,
  "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
  "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
  "jobType": "demo.echo",
  "payload": {
    "message": "hello"
  },
  "remainingAttempts": 3
}
```

The same validation library used by workers should validate the encoded Kafka
key/value before publication. This prevents the HTTP path from creating a
different wire format from direct Kafka producers.

### 4.2 Accepted response

After the Kafka producer future confirms acknowledgment, return:

```http
HTTP/1.1 202 Accepted
Content-Type: application/json
```

```json
{
  "status": "accepted",
  "jobId": "e2be7c92-05f9-48d3-aeb2-4e695d857151",
  "executionId": "5d0d8409-4276-4ec8-8fd0-941ed977ff00",
  "schemaVersion": 1,
  "remainingAttempts": 3
}
```

`202` is intentionally used instead of `201`: no persisted resource can be
queried and execution has not completed. Do not return `Location` unless a real
status resource is introduced by a later spec.

Do not include raw payloads, Kafka bootstrap servers, credentials, or absolute
broker addresses in the response. Kafka topic, partition, and offset are
implementation metadata for this increment and are not part of the HTTP
contract.

### 4.3 Error responses

Use the following stable shape:

```json
{
  "error": {
    "code": "VALIDATION_ERROR",
    "message": "jobType is required"
  }
}
```

Messages must be safe to return to the caller and must not echo malformed input,
payload values, stack traces, or broker configuration.

| Status | Code | Meaning |
| --- | --- | --- |
| `400` | `MALFORMED_JSON`, `VALIDATION_ERROR` | Invalid JSON or request contract |
| `413` | `REQUEST_TOO_LARGE` | HTTP body or resulting envelope exceeds its configured limit |
| `415` | `UNSUPPORTED_MEDIA_TYPE` | Effective request content type is not JSON |
| `429` | `ADMISSION_OVERLOADED` | The API's bounded in-flight publication capacity is full |
| `503` | `KAFKA_UNAVAILABLE` | Publication was not acknowledged within the bounded admission deadline |
| `500` | `INTERNAL_ERROR` | An unexpected server failure occurred |

`429` is local backpressure, not a guarantee about worker backlog. A client may
retry it with the same request, provided it retains or supplies stable IDs.

If Kafka returns a conclusive failure, map it to `503`. If the publication
outcome remains ambiguous at the request deadline, return `503` rather than
claiming rejection. The record may already have reached Kafka.

## 5. Identity, retries, and delivery semantics

For direct Kafka clients, Spec 002 requires a client retry to reuse both
identifiers and identical content. The HTTP API preserves the same convention:

- If IDs are both supplied, resubmitting the exact request can produce another
  physical Kafka record. The service does not promise deduplication.
- A new logical invocation must use a new `executionId`. The client may retain
  the `jobId` when the new invocation belongs to the same serialization domain.
- The client must not reuse identities with changed `jobType`, payload, or
  retry budget.
- Omitted IDs are generated on every request. Clients that require repeatable
  request recovery must generate and submit both IDs.
- Worker replay and HTTP client retry may both cause the same logical identity
  to run more than once. Handlers must retain the Spec 002 replay and concurrency
  contract.

Returning `202` is not evidence that the handler ran. A subsequent `503` is not
evidence that the job was absent from Kafka. Client retries therefore retain
at-least-once, possibly duplicate admission semantics; the API must not label
them as exactly-once.

The API does not attempt to compare repeated identities or payloads in memory.
An in-memory set would be bounded but still false after restart and would imply
a stronger deduplication guarantee than the architecture supports.

## 6. Publication and failure handling

Use one bounded Kafka producer for each HTTP API process:

1. Encode the canonical envelope as UTF-8 bytes.
2. Use UTF-8 `jobId` bytes as the key and the envelope bytes as the value.
3. Validate the encoded key/value with the Spec 002 envelope validator.
4. Use keyed partitioning through the same Kafka Java client partition
   convention expected by workers.
5. Publish only to `jobs.main.v1`.
6. Return `202` only after the producer acknowledges the record.

Use an idempotent Kafka producer with `acks=all` and finite request and delivery
timeouts. Configure a bounded producer buffer. Do not use Kafka transactions:
the API performs one append and receives no database or offset coordination
benefit from a transaction.

A single request may remain in flight while the producer retries inside its
configured delivery timeout. The HTTP API must bound independently:

- Concurrent admitted publication futures per process.
- Buffered producer bytes.
- Admission wait time.
- Per-request time waiting for broker acknowledgment.
- Graceful-shutdown drain time.

Rejecting a request before a producer future exists is conclusive local
admission failure and may return `429`, `400`, `413`, or `415`. Once publication
has been attempted, an application-level retry that sends another record is
forbidden because it can duplicate an append whose outcome is ambiguous. The
Kafka producer may perform its configured idempotent send retries.

On shutdown, stop accepting new requests, wait for in-flight producer futures
up to a bounded deadline, close the HTTP server, and close the producer. Do not
claim acceptance for in-flight futures that remain unresolved at shutdown.
Process termination may leave the client with an ambiguous outcome.

## 7. Backpressure

HTTP admission capacity is separate from Kafka consumer backpressure. The API
must not read worker lag and convert it into admission rejection in this
increment.

When either the in-flight request bound or producer buffer admission bound is
reached, reject new requests with `429` before copying or publishing their
envelopes. Already accepted requests continue waiting for their own bounded
acknowledgment deadlines.

The API must account for and expose queued/active request pressure. Do not use
an unbounded event-loop queue, executor queue, or retry loop to absorb excess
load.

Recommended initial defaults are required before implementation:

| Setting | Proposed default |
| --- | ---: |
| HTTP port | `8080` |
| Maximum request/envelope body | `256 KiB` |
| Maximum in-flight publications per pod | `1,000` |
| Admission acknowledgment deadline | `10 s` |
| Graceful drain | `30 s` |
| Kafka delivery timeout | `9 s` |

Implementation may change these defaults only with documented load evidence and
without weakening the response-bound or producer-buffer invariants.

## 8. Health, observation, and security

### 8.1 Health endpoints

- `GET /health/live` returns `200` when the event loop and HTTP role can respond.
- `GET /health/ready` returns `200` only when required configuration is valid,
  the producer can reach Kafka, required application topics exist, and the
  `jobs.main.v1` partition count matches configuration.

Readiness failure must not mutate the topic layout or reset consumer offsets.
The HTTP role must never expand an existing topic to satisfy configuration.

### 8.2 Metrics

Expose bounded-cardinality metrics for:

- requests by outcome and status class;
- accepted, locally overloaded, invalid, and Kafka-unavailable counts;
- active and queued admission requests;
- serialized request/envelope bytes;
- validation latency and broker acknowledgment latency;
- Kafka send failures and ambiguous deadline outcomes;
- in-flight publication capacity and buffer pressure;
- readiness transitions.

Do not use `jobId`, `executionId`, payload values, client identity, or raw
error text as metric labels.

### 8.3 Logs

Structured success logs may include generated/supplied `jobId`, `executionId`,
topic, partition, and offset after acknowledgment. Logs must not contain
payloads, credentials, authorization headers, or raw request bodies.

Validation and 4xx logs may include stable validation codes but not rejected
values. Broker and 5xx logs may include sanitized Kafka error categories and
correlation IDs, never bootstrap credentials or record content.

### 8.4 Trust boundary

The public ingress must enforce TLS and authentication/authorization before
requests reach the admission role. The required policy is expected to identify
which authenticated caller may submit which registered `jobType`; the mechanism
and caller identity format are deployment-specific and require a separate
security decision before internet exposure.

This spec does not authorize accepting untrusted internet traffic. Local Compose
use is a demonstration environment and must not be described as a production
security configuration.

## 9. Configuration

Add an independently selectable API role while retaining existing worker roles.
At minimum:

| Variable | Purpose |
| --- | --- |
| `HTTP_PORT` | Local bind port |
| `HTTP_MAX_BODY_BYTES` | Raw body and prepublication envelope bound |
| `HTTP_MAX_IN_FLIGHT` | Maximum concurrent publication futures |
| `HTTP_ACK_DEADLINE_MILLIS` | Request acknowledgment deadline |
| `KAFKA_BOOTSTRAP_SERVERS` | Kafka connection |
| `KAFKA_TOPIC_PARTITIONS` | Expected main-topic partition count |
| `REGISTERED_JOB_TYPES` | API allowlist for canonical job types |
| `KAFKA_DELIVERY_TIMEOUT_MILLIS` | Bounded producer delivery time |
| `HTTP_GRACEFUL_SHUTDOWN_MILLIS` | Bounded admission drain |

Validate all configuration before opening the HTTP listener. Fail startup for
invalid limits, unknown role selection, empty job-type registration, missing
Kafka configuration, or contradictory timeout relationships. Do not silently
fall back to worker-default role configuration.

## 10. Invariants

1. Every `202` response follows a Kafka acknowledgment for a valid canonical
   Spec 002 main envelope keyed by `jobId`.
2. Invalid or oversized HTTP input is never published or handed to a worker.
3. `jobs.main.v1` is the only topic the HTTP role writes.
4. The HTTP role adds no database-backed state, worker status, or durable
   deduplication.
5. A Kafka-unavailable response is not a conclusive proof that no record was
   published.
6. Main and retry worker ownership, handler admission, retries, and commits are
   unchanged by this API.
7. Reusing `(jobId, executionId)` preserves handler identity but does not
   prevent duplicate execution; changing content under reused identities is a
   client contract violation.
8. One HTTP process has one bounded producer and cannot unboundedly queue work
   in request futures, event-loop tasks, or producer buffers.
9. The API never logs or returns payload content as diagnostic context.
10. The API cannot expose per-job completion because it has no authoritative
    completion read path.

## 11. Acceptance criteria

| ID | Requirement and acceptance evidence |
| --- | --- |
| API01 | A valid request with both IDs returns `202` only after Testcontainers Kafka acknowledges a record on `jobs.main.v1`; assert topic, UTF-8 key, exact canonical value, and generated/default fields. |
| API02 | A request omitting both IDs receives fresh canonical lowercase UUIDs, and the stored envelope and `202` body contain those exact IDs. |
| API03 | Supplying only one of `jobId` or `executionId` returns `400` and publishes nothing. |
| API04 | A client resubmission with the same IDs and body creates a duplicate physical record and the API does not claim deduplication. Both copies remain valid Spec 002 input. |
| API05 | A client resubmission that omits IDs creates a new identity pair. This demonstrates the documented limit of server-generated IDs. |
| API06 | Unknown fields, noncanonical UUIDs, unregistered job types, wrong JSON types, duplicate properties, malformed UTF-8/JSON, and trailing content return the mapped 4xx code and publish nothing. |
| API07 | Raw-body and encoded-envelope limits return `413` without truncation or publication at the boundary. |
| API08 | Unsupported media types return `415`; no-body and malformed-body cases return the documented `400` codes. |
| API09 | A saturated in-flight bound returns `429` promptly, does not enqueue unbounded request futures, and resumes admission after capacity is released. |
| API10 | Broker refusal or a broker acknowledgment deadline returns `503`. An injected ambiguous producer timeout proves the API does not claim the record was rejected. |
| API11 | A Kafka producer send can be idempotently retried by the Kafka client, but the HTTP layer itself does not manufacture a second send after an ambiguous attempt. |
| API12 | Shutdown closes intake, drains acknowledged in-flight work within the bound, and closes the producer. A stalled future is bounded and not reported as accepted. |
| API13 | API readiness fails for invalid configuration, missing Kafka, a missing topic, or a partition-count mismatch. No startup path expands an existing topic. |
| API14 | HTTP-originated jobs preserve Spec 002 acceptance B01, B03, B04, B05, and B09: valid records enter normal worker handling, same-topic identity gating is unchanged, different IDs may run concurrently, safe-prefix commits hold, and replay remains possible. |
| API15 | Metrics and logs contain no payload, credential, or raw-input data; error responses use stable sanitized messages. |
| API16 | A full Compose smoke test submits through HTTP, observes main-worker processing, and then confirms that no PostgreSQL or state-store service is introduced. |

API14 uses the existing worker contract rather than retesting all of Spec 002 for
each transport. It must be an integration assertion across the HTTP and worker
boundary, not a private mapping-function test.

## 12. Implementation order

1. **Validation and mapping:** extract or reuse the canonical envelope
   validation/serialization boundary so HTTP and direct Kafka publication cannot
   diverge.
2. **Producer adapter:** bounded asynchronous publication, finite deadlines,
   ambiguous-outcome handling, and producer lifecycle.
3. **HTTP contract:** route, body limits, status/error mapping, health, and
   readiness.
4. **Observation and configuration:** metrics, sanitized logs, startup
   validation, and role-specific deployment.
5. **Integration and deployment:** Testcontainers API-to-Kafka acceptance and
   failure checks, followed by a Compose smoke path.

Keep each phase reviewable. The producer adapter should be testable before route
handlers are added, and the route contract should not require a live worker.

## 13. Verification

- Run focused unit tests for request decoding, ID generation, canonical envelope
  mapping, body limits, error mapping, timeout boundaries, and producer
  admission accounting.
- Run a local HTTP contract test without Kafka for all deterministic 4xx and
  overload cases, using a controlled producer future rather than private
  implementation assertions.
- Run Testcontainers Kafka cases for API01, API02, API04, API10, API13, and API14.
- Run the existing unit suite with `mvn test`.
- Build the library/application artifact with `mvn package`.
- If Docker is available, run the relevant broker-backed tests with `mvn verify`.
  Do not report API integration coverage unless an API-to-Kafka test actually
  ran.
- Update [README.md](../README.md), [arch.md](../arch.md), and Compose examples
  only when the corresponding runtime and evidence exist.

Record command, commit, outcome, and evidence location separately. A planned
check, a skipped check, an unavailable Docker environment, and a passing test
are distinct results.

## 14. Decisions deferred

These choices require separate requirements and must not be implied by this
spec:

- Durable HTTP idempotency keys and exactly-once admission, which would require
  a durable idempotency store or transactional outbox design.
- Job-status queries, completion result storage, or callback/webhook delivery.
- Authentication schemes, authorization mappings, tenant isolation, rate-limit
  ownership, and identity propagation.
- Multi-topic routing by tenant or job type.
- HTTP batch submission and request-level atomicity across multiple jobs.
- Externally managed request quotas and cluster-wide admission budgets.
- TLS and ingress provisioning, Kubernetes deployment, or public exposure.
- Kafka transactions or cross-system exactly-once coordination.
