package com.example.agenticjobscheduler.execution;

import io.vertx.core.Future;

/**
 * Nonblocking, replay-safe and concurrency-safe handler. The future, whether
 * successful or failed, must finish only after the operation has actually stopped.
 * A synchronous throw is allowed only before starting work. Never return null.
 * Offload blocking work to a bounded executor; do not block the owner event loop.
 * Main/retry overlap and replay are possible: use the execution's jobId and
 * executionId as the stable target-side idempotency key, not its source offset.
 */
@FunctionalInterface
public interface JobHandler {
    Future<Void> execute(JobExecution execution);
}
