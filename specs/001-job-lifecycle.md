# Spec 001: Core job lifecycle and persistence

Status: **Draft — awaiting user approval before implementation**

Project: `agentic-job-scheduler`  
Date: 2026-10-08

## 1. Purpose and scope

Build a Java 21 and Vert.x service that accepts jobs, persists them in PostgreSQL,
and activates due jobs safely across multiple application instances. This spec
provides a small, reviewable learning increment for agentic SDLC practices aligned
with the project's GH-600 learning objective; it does not claim certification or
complete coverage of an external syllabus.

This iteration includes REST creation and queries, automatic Flyway migrations,
a periodic scheduler, structured transition logs, unit and integration tests, and
local Docker Compose startup. PostgreSQL is the sole authoritative state store.

Kafka, execution, worker pools, retries, leases, heartbeats, recovery of RUNNING
jobs, admission budgets, global concurrency limits, adaptive rate limiting,
circuit breakers, metrics, tracing, and CI/CD are explicitly deferred. READY jobs
remain READY in this iteration.

## 2. Architecture and responsibilities

```text
Client -> Vert.x REST API -> Job service -> PostgreSQL repository
                                               |
Scheduler Verticle -> activation service -------+
                        SCHEDULED -> READY
```

- `api`: routes, JSON mapping, request validation, HTTP error mapping.
- `domain`: immutable job model, status enum, creation and activation services,
  explicit lifecycle rules, repository interface.
- `persistence`: parameterized SQL and asynchronous Vert.x PostgreSQL client.
- `scheduler`: timer lifecycle and bounded activation calls; no HTTP concerns.
- `config`: validated environment configuration, Flyway startup, composition,
  resource ownership, and graceful shutdown.

Use packages under `com.example.agenticjobscheduler`, keeping these directories
under that package in `src/main/java`. Tests mirror the production packages.
Use Maven, JUnit 5, Testcontainers, Flyway, and Docker Compose. Pin compatible,
stable dependency versions during implementation, targeting Java 21 and
PostgreSQL 18; use the same PostgreSQL major version in tests and Compose.

Runtime database operations use the non-blocking Vert.x PostgreSQL pool. Flyway
uses JDBC on a Vert.x blocking worker during startup, before HTTP or scheduler
deployment. Migration or configuration failure aborts startup and closes resources.
Package root `db/migration` scripts as classpath `db/migration` resources so the
executable artifact does not depend on its working directory.

## 3. State machine

All six states exist in the Java model and database constraint:

| State | Meaning | Produced by this spec? |
| --- | --- | --- |
| SCHEDULED | Persisted and awaiting scheduled time | Yes, on creation |
| READY | Due and eligible for future dispatch | Yes, by scheduler |
| RUNNING | An execution attempt is in progress | No |
| SUCCESS | Execution completed successfully | No |
| FAILED | Execution permanently failed | No |
| RETRY_WAIT | Waiting for a later execution attempt | No |

The only implemented lifecycle operations are creation into SCHEDULED and
SCHEDULED -> READY when `scheduled_at <= statement_timestamp()` in PostgreSQL.
There is no public arbitrary status-update endpoint. Activation never increments
`attempt` or changes execution timestamps. Repeating activation after success
is a no-op because READY no longer matches the update predicate.

Future specs will define READY -> RUNNING, RUNNING -> SUCCESS/FAILED/RETRY_WAIT,
and RETRY_WAIT -> READY, including their guards and attempt semantics. Their
presence in the enum does not enable those transitions now.

## 4. REST contract

All request and response bodies use JSON and camelCase. Timestamps accept RFC
3339 date-times with an explicit offset and are returned as UTC instants.
Database timestamp precision is microseconds. All job responses include every
field listed in the schema below, including explicit nulls.

### POST /jobs

```json
{
  "jobType": "example.email",
  "payload": { "recipient": "learner@example.com" },
  "scheduledAt": "2026-10-09T01:00:00Z",
  "maxAttempts": 3
}
```

- `jobType`: required string, trimmed, 1–128 characters after trimming.
- `payload`: required JSON object; `{}` is valid. Arrays, scalars and null are invalid.
- `scheduledAt`: optional; omitted means database current time. Past times are
  allowed and become eligible on the next scheduler pass. Explicit null is invalid.
- `maxAttempts`: optional integer, defaults to 3; valid range 1–100.
- Reject unknown fields, including client-supplied IDs, status and attempt values.
- Limit the entire request body to 1 MiB.

Generate a UUID jobId server-side and insert with status SCHEDULED and attempt 0.
Return `201 Created`, `Location: /jobs/{jobId}`, and the persisted job only after
the database transaction commits. The returned creation snapshot is SCHEDULED;
a concurrent scheduler may already have activated the job by a subsequent GET.

POST is not idempotent: separate successful requests create separate jobs.
Request deduplication is a future feature, distinct from idempotent transitions.

### GET /jobs/{jobId}

Return `200` with the persisted job, `400` for an invalid UUID, and `404` for an
unknown UUID. Read from PostgreSQL without an application cache.

### GET /jobs?status=SCHEDULED&limit=50&offset=0

`status` is optional; when supplied it must exactly match one of the six uppercase
states. Without it, list all states. `limit` defaults to 50 and accepts 1–200;
`offset` defaults to 0 and accepts nonnegative 32-bit integers. Invalid or repeated
query parameters and unknown query parameters return `400`.

Return `200` with `{ "items": [...], "limit": 50, "offset": 0 }`, ordered by
`created_at ASC, job_id ASC`. Empty results use an empty array. Pagination is a
live view, not a snapshot across requests, and does not include a total count.

### Errors

Use `{ "error": { "code": "VALIDATION_ERROR", "message": "..." } }`.
Use `400` for malformed JSON/validation, `413` for oversized bodies, `415` for
unsupported POST content types, `404` for missing resources, `503` for database
unavailability/timeouts, and `500` for unexpected internal errors. Never expose
SQL, credentials, stack traces, or payloads in client errors or application logs.

## 5. Database schema and migration

Migration `db/migration/V1__create_jobs.sql` creates `jobs`:

| SQL column | Type | Constraints / initial value |
| --- | --- | --- |
| job_id | uuid | Primary key; maps to jobId |
| job_type | varchar(128) | NOT NULL; trimmed, nonempty |
| payload | jsonb | NOT NULL; JSON object |
| status | varchar(16) | NOT NULL; CHECK six supported states |
| created_at | timestamptz | NOT NULL; database statement time |
| scheduled_at | timestamptz | NOT NULL; request time or database statement time |
| started_at | timestamptz | Nullable; initially NULL |
| completed_at | timestamptz | Nullable; initially NULL |
| attempt | integer | NOT NULL DEFAULT 0; 0 <= attempt <= max_attempts |
| max_attempts | integer | NOT NULL DEFAULT 3; 1–100 |
| next_retry_at | timestamptz | Nullable; initially NULL |
| last_error | text | Nullable; initially NULL |

Add a partial due-job index on `(scheduled_at, job_id) WHERE status = 'SCHEDULED'`,
plus indexes on `(created_at, job_id)` and `(status, created_at, job_id)` for lists.
Use CHECK constraints rather than a PostgreSQL enum to simplify later migrations.

Also create `job_state_transitions` as a small durable audit log with
`transition_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY`, `job_id uuid
NOT NULL REFERENCES jobs(job_id)`, nullable `previous_state`, non-null `new_state`,
`attempt integer NOT NULL`, and `occurred_at timestamptz NOT NULL DEFAULT
statement_timestamp()`. State columns have the same allowed-value constraints;
attempt must be nonnegative. Index `(job_id, transition_id)`.

Creation records NULL -> SCHEDULED in the same transaction as the insert.
Activation records SCHEDULED -> READY in the same transaction as the update.
This audit table closes the crash window between a committed transition and
emitting a process log. It is not an event bus or Kafka outbox.

## 6. Concurrency and scheduler algorithm

Each instance runs a Scheduler Verticle. Default poll interval is 1,000 ms and
batch size is 100. Start with an immediate pass, then poll periodically. Allow
only one in-flight pass per verticle; skip ticks while it is busy. This local
flag limits resource use and is not a correctness lock or authoritative state.

Run a bounded atomic SQL statement shaped as follows, using a prepared batch
size parameter and an explicit transaction:

```sql
WITH due AS (
    SELECT job_id
    FROM jobs
    WHERE status = 'SCHEDULED'
      AND scheduled_at <= statement_timestamp()
    ORDER BY scheduled_at, job_id
    LIMIT $1
    FOR UPDATE SKIP LOCKED
), activated AS (
    UPDATE jobs AS j
    SET status = 'READY'
    FROM due
    WHERE j.job_id = due.job_id
      AND j.status = 'SCHEDULED'
    RETURNING j.job_id, j.attempt
)
INSERT INTO job_state_transitions (job_id, previous_state, new_state, attempt)
SELECT job_id, 'SCHEDULED', 'READY', attempt
FROM activated
RETURNING job_id, previous_state, new_state, attempt, occurred_at;
```

At PostgreSQL READ COMMITTED isolation, row locks prevent competing schedulers
from activating the same row concurrently, and SKIP LOCKED allows them to work
on different rows. The status predicate makes later passes idempotent. Locks
last only through the short database transaction; no external distributed lock
or leader election is needed. Database time determines eligibility independently
of application clock skew. Process at most one batch per tick for bounded work.

PostgreSQL documents [SKIP LOCKED for queue-like consumers](https://www.postgresql.org/docs/current/sql-select.html#SQL-FOR-UPDATE-SHARE)
and [UPDATE RETURNING](https://www.postgresql.org/docs/current/sql-update.html).

## 7. Logging and failure behavior

After successful commit, emit one structured JSON log per returned transition,
including `event=job_state_transition`, `jobId`, `previousState`, `newState`,
`attempt`, and timestamp. Creation logs use `previousState=null`. Do not log a
transition for an unchanged row or a rolled-back transaction. Durable audit rows
remain available even if the process dies before its stdout log is emitted;
exactly-once stdout delivery is not promised.

| Failure | Required behavior |
| --- | --- |
| Invalid create request | Return validation error; no job or audit row |
| Database unavailable at startup | Fail startup; do not serve HTTP or schedule |
| Migration failure | Fail startup with diagnostic log; no partial serving |
| Database unavailable during API request | Return 503; do not fabricate success |
| Scheduler query fails | Roll back, log error, release in-flight flag, retry on a later tick |
| Process dies before commit | Transaction rolls back; another pass can activate the job |
| Process dies after commit | Job and audit row survive; future passes leave READY unchanged |
| Commit result or HTTP response is lost | Outcome may be ambiguous; client can query a known ID; retrying POST can duplicate a job |
| Multiple app instances start | Flyway coordinates migrations; row-level SQL coordinates activation |
| Application restarts | Persisted jobs remain; overdue SCHEDULED jobs activate after startup |
| Large due backlog | Drain bounded batches over subsequent ticks; no strict activation-latency guarantee |

Configure finite connection acquisition and database statement timeouts so a
failed operation does not occupy a scheduler pass indefinitely. Graceful shutdown
stops HTTP admission and timers, waits a bounded period for in-flight operations,
then closes the pool and Vert.x. Abrupt termination relies on database atomicity.

## 8. Local operation and repository artifacts

Deliver `pom.xml`, `Dockerfile`, `docker-compose.yml`, `README.md`, `AGENTS.md`,
this spec, migrations, Java sources, and tests. Compose builds the Java application
and runs PostgreSQL with a health check and a named volume. The application starts
after PostgreSQL is healthy. Document `docker compose up --build`, example curl
requests, restart verification, logs, and that `docker compose down -v` destroys
local persisted data. Local development credentials must be clearly labeled.

Configuration includes HTTP port (8080), PostgreSQL host/port/database/user/password,
pool size (10), scheduler interval (1,000 ms), batch size (100), and finite database
timeouts. Validate positive values and required credentials before startup. Allow
scheduler disabling for isolated API tests. Never print passwords.

Use Maven Surefire for unit tests and Failsafe for `*IT` integration tests:
`mvn test` runs unit tests; `mvn verify` runs both. Integration tests require a
working Docker runtime and must fail clearly rather than silently skip when it
is unavailable. README records the Java/Maven requirements and exact commands.

## 9. Acceptance tests

Use real PostgreSQL via Testcontainers for integration tests; do not substitute
an in-memory database. Tests use isolated data, bounded asynchronous waits, and
real committed SQL reads. Avoid relying on arbitrary sleeps for concurrency.

| ID | Scenario and assertion | Level |
| --- | --- | --- |
| A01 | Valid POST returns 201 and Location; matching job and creation audit row exist in PostgreSQL | Integration |
| A02 | Defaults produce SCHEDULED, attempt 0, maxAttempts 3, database-derived times, null execution fields | Integration |
| A03 | Future job remains SCHEDULED after activation pass | Integration |
| A04 | Overdue job becomes READY with unchanged attempt and execution fields, exactly one activation audit row | Integration |
| A05 | A running scheduler activates a near-future job once database time reaches scheduledAt | Integration |
| A06 | Two independent scheduler instances/pools start competing passes against the same due jobs; returned ID sets are disjoint, their union covers the jobs after bounded draining, and each job has one activation audit row | Integration |
| A07 | Repeated activation produces no additional transition or audit row for READY jobs | Integration |
| A08 | GET by ID and list/filter/pagination return persisted jobs; all supported status filters are accepted | Integration |
| A09 | Close and recreate application and database pools against the same container; jobs survive and overdue jobs activate | Integration |
| A10 | Startup migrates an empty database; repeated and concurrent startup safely validate/reuse migrations | Integration |
| A11 | Forced transaction rollback leaves job state and transition audit unchanged | Integration |
| A12 | Scheduler survives a failed repository call and performs a subsequent pass; overlapping ticks do not start concurrent local passes | Unit |
| A13 | Validation rejects invalid payloads, job types, times, attempts, unknown fields, malformed UUIDs and pagination; HTTP errors follow contract | Unit + integration |
| A14 | Transition logs contain required fields and are emitted only after successful commit | Unit + integration |
| A15 | Database outage returns controlled API error; scheduler recovers after connectivity returns | Integration |
| A16 | A batch never exceeds configured size; remaining jobs activate on later passes | Integration |
| A17 | Clean Docker Compose startup, REST creation, activation, and app restart persistence work as documented | Local smoke test |

Unit tests also cover all status values, request-to-domain mapping, the explicit
activation rule, configuration validation, and error mapping. Integration tests
exercise actual API routing and SQL; mocks alone cannot establish concurrency
or persistence guarantees. Record checks run and any environment limitations.

## 10. Extension seams and agentic workflow

Keep repository operations intention-specific (`create`, `findById`, `list`,
`activateDueJobs`) rather than exposing a generic status setter. Later specs can
add guarded execution/retry transitions and an atomic dispatch/outbox mechanism
without moving authority away from PostgreSQL. No Kafka dependencies, publishers,
consumers, dummy executors, or speculative retry logic belong in this increment.

Workflow: review and approve this spec; implement against acceptance IDs; run
unit/integration and local smoke checks; review SQL concurrency, error handling,
and evidence against the spec. `AGENTS.md` will record scope boundaries, build
commands, non-blocking runtime requirements, migration discipline, and the need
to report test results honestly. Material scope changes require updating the
spec for review. Approval is pending; no implementation is included in this draft.
