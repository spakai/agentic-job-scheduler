package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobHandler;
import io.vertx.core.Future;
import io.vertx.core.Vertx;

/** Keeps the execution pool alive until the consumer has drained. */
final class WorkerRuntime {
    private final Vertx vertx;
    private final String consumerDeployment;
    private final ExecutionVerticlePool workers;
    private Future<Void> closing;

    private WorkerRuntime(Vertx vertx, String consumerDeployment, ExecutionVerticlePool workers) {
        this.vertx = vertx;
        this.consumerDeployment = consumerDeployment;
        this.workers = workers;
    }

    static Future<WorkerRuntime> deploy(Vertx vertx, WorkerConfig config, JobHandler handler) {
        if (Vertx.currentContext() != null) {
            return Future.failedFuture("Worker runtime must be deployed outside a verticle context");
        }
        return ExecutionVerticlePool.deploy(vertx, handler, Math.min(config.concurrency(), 4))
                .compose(workers -> vertx.deployVerticle(new ConsumerVerticle(config, workers))
                        .map(id -> new WorkerRuntime(vertx, id, workers))
                        .recover(failure -> closeAfterFailure(workers, failure)));
    }

    synchronized Future<Void> close() {
        if (closing == null) {
            closing = vertx.undeploy(consumerDeployment).compose(
                    ignored -> workers.close(), failure -> closeAfterFailure(workers, failure));
        }
        return closing;
    }

    private static <T> Future<T> closeAfterFailure(ExecutionVerticlePool workers, Throwable failure) {
        return workers.close().compose(ignored -> Future.failedFuture(failure), cleanupFailure -> {
            failure.addSuppressed(cleanupFailure);
            return Future.failedFuture(failure);
        });
    }
}
