package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.JobHandler;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.VerticleBase;
import io.vertx.core.Vertx;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.logging.Logger;

/** Dispatches nonblocking handler invocations onto separate verticle contexts. */
final class ExecutionVerticlePool implements JobHandler {
    private static final Logger LOG = Logger.getLogger(ExecutionVerticlePool.class.getName());
    private final Vertx vertx;
    private final String deploymentId;
    private final List<ExecutionVerticle> workers;
    private final AtomicInteger next = new AtomicInteger();

    private ExecutionVerticlePool(Vertx vertx, String deploymentId, List<ExecutionVerticle> workers) {
        this.vertx = vertx;
        this.deploymentId = deploymentId;
        this.workers = List.copyOf(workers);
    }

    static Future<ExecutionVerticlePool> deploy(Vertx vertx, JobHandler handler, int size) {
        Objects.requireNonNull(vertx, "vertx");
        Objects.requireNonNull(handler, "handler");
        if (size < 1) {
            throw new IllegalArgumentException("Worker verticle count must be positive");
        }
        List<ExecutionVerticle> workers = Collections.synchronizedList(new ArrayList<>());
        return vertx.deployVerticle(() -> {
            ExecutionVerticle worker = new ExecutionVerticle(handler);
            workers.add(worker);
            return worker;
        }, new DeploymentOptions().setInstances(size))
                .map(id -> new ExecutionVerticlePool(vertx, id, workers));
    }

    @Override
    public Future<Void> execute(JobExecution execution) {
        return execute(execution, () -> true);
    }

    Future<Void> execute(JobExecution execution, BooleanSupplier eligible) {
        return dispatch(execution, eligible, new AtomicLong()).completion();
    }

    Dispatch dispatch(JobExecution execution, BooleanSupplier eligible, AtomicLong startedAt) {
        int index = Math.floorMod(next.getAndIncrement(), workers.size());
        return workers.get(index).dispatch(execution, eligible, startedAt);
    }

    Future<Void> close() {
        return vertx.undeploy(deploymentId);
    }

    record Dispatch(Future<Long> started, Future<Void> completion) { }

    private static final class ExecutionVerticle extends VerticleBase {
        private final JobHandler handler;

        private ExecutionVerticle(JobHandler handler) {
            this.handler = handler;
        }

        private Dispatch dispatch(JobExecution execution, BooleanSupplier eligible, AtomicLong startedAt) {
            Promise<Long> started = Promise.promise();
            Promise<Void> result = Promise.promise();
            context.runOnContext(ignored -> {
                if (!eligible.getAsBoolean()) {
                    result.tryFail("Assignment revoked before handler start");
                    return;
                }
                long now = System.nanoTime();
                startedAt.set(now);
                started.tryComplete(now);
                Future<Void> completion;
                try {
                    completion = handler.execute(execution);
                } catch (RuntimeException failure) {
                    result.tryFail(failure);
                    return;
                }
                if (completion == null) {
                    LOG.severe("Handler returned null; retaining the job gate and source record");
                    return;
                }
                completion.onComplete(result);
            });
            return new Dispatch(started.future(), result.future());
        }
    }
}
