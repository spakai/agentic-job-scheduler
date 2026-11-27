# Spec 008: Main-worker initial concurrency

Status: **Draft**

Project: `agentic-job-scheduler`

Date: 2026-11-27

Depends on: [Spec 002](002-kafka-job-scheduling.md) B13 and
[Spec 007](007-adaptive-external-service-pressure.md). When implemented, this
spec supersedes their main-worker initial-default statements, but not their
adaptive runtime bounds or decision rules.

## Scope Baseline

- **Discovery method**: Reviewed the worker configuration, adaptive limiter,
  demo configuration, and existing adaptive-limiter specifications.
- **Items in scope**: Main-worker initial concurrency default and valid range,
  configuration validation, directly affected documentation, and tests.
- **Out of scope**: Changing the retry-worker default, adaptive runtime maximum,
  adaptive decision rules, Kafka behavior, and historical demo evidence.

## User Scenarios & Testing

### User Story 1 - Start the main worker conservatively (Priority: P1)

An operator starts a main worker without specifying its initial concurrency.
The worker starts with four permits and remains free to adapt as before.

**Independent test**: Create a main-worker configuration without a concurrency
override and verify its initial limit and existing adaptive behavior.

**Acceptance scenarios**:

1. **Given** `WORKER_CONCURRENCY` is unset, **when** a main worker starts,
   **then** its initial concurrency is 4.
2. **Given** the adaptive runtime maximum permits it and qualifying healthy
   windows occur, **when** the limit reaches 16, **then** it may grow beyond 16.
3. **Given** the default configuration of a retry worker, **when** it starts,
   **then** its existing concurrency default remains unchanged.

### User Story 2 - Configure the initial limit safely (Priority: P1)

An operator can select a main-worker initial concurrency from 1 through 16
inclusive, provided it also fits the configured adaptive runtime bounds.

**Independent test**: Validate boundary values and incompatible adaptive bounds
without starting a worker workload.

**Acceptance scenarios**:

1. **Given** compatible adaptive bounds, **when** the initial concurrency is
   set to 1, 4, or 16, **then** configuration succeeds with the chosen value.
2. **Given** an initial concurrency of 0, 17, an empty value, or a non-integer,
   **when** configuration is validated, **then** startup fails with a clear
   error.
3. **Given** a chosen or default initial concurrency outside the configured
   adaptive minimum or maximum, **when** configuration is validated, **then**
   startup fails rather than silently clamping it.

### Edge Cases

- The 16-permit ceiling applies to the initial main-worker setting, not to
  the configurable adaptive runtime maximum, which still defaults to 128.
- Explicit overrides in Compose or other deployment configurations may mask
  the new worker default; assess them and update only those that should use
  the new default.
- A decrease never cancels already-running handlers. Each worker's limit is
  per process, not a cluster-wide budget.

## Requirements

### Functional Requirements

- **REQ-001**: An unset `WORKER_CONCURRENCY` MUST produce an initial
  main-worker limit of 4.
- **REQ-002**: The main-worker initial limit MUST be a whole number from 1
  through 16 inclusive and MUST also lie within the configured adaptive
  minimum and maximum.
- **REQ-003**: Invalid or incompatible main-worker configuration MUST fail
  before processing begins with an actionable error; it MUST NOT be silently
  clamped or replaced by a default.
- **REQ-004**: The existing adaptive runtime maximum, including its default of
  128 and operator configurability, MUST remain unchanged. Healthy growth
  MUST remain able to exceed 16 when the runtime maximum allows it.
- **REQ-005**: The retry worker's existing concurrency defaults and limits
  MUST remain unchanged.
- **REQ-006**: Directly affected configuration documentation and demo guidance
  MUST accurately distinguish worker defaults from explicit demo overrides
  before implementation is merged. Historical graphs and results MUST remain
  identified as historical evidence.

### Assumptions

- `WORKER_CONCURRENCY` is the main worker's initial adaptive setting; it is not
  a permanent maximum on adaptive growth.
- An implementation PR may initially leave demo guidance unchanged to exercise
  the demo-review agent; affected guidance must be corrected before merging.

## Success Criteria

### Measurable Outcomes

- **SC-001**: An unset main-worker setting starts at 4; 1, 4, and 16 are
  accepted with compatible bounds, while 0, 17, empty, non-integer, and
  incompatible values are rejected explicitly.
- **SC-002**: Focused evidence shows adaptive growth can exceed 16 under a
  compatible runtime maximum, and that the retry default is unchanged.
- **SC-003**: Directly affected documentation distinguishes the new worker
  default from demo overrides, and historical evidence remains unchanged.
- **SC-004**: Record the exact validation commands, commit, outcomes, and
  evidence locations; do not report an unrun demo or test as passing.
