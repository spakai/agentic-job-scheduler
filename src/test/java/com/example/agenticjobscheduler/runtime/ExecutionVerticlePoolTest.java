package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ExecutionVerticlePoolTest {
    @Test
    void executesOnWorkerContextsAndPreservesAsynchronousSettlement() throws Exception {
        Vertx vertx = Vertx.vertx();
        Promise<Void> blocked = Promise.promise();
        Promise<Context> firstContext = Promise.promise();
        Promise<Context> secondContext = Promise.promise();
        AtomicInteger starts = new AtomicInteger();
        ExecutionVerticlePool pool = ExecutionVerticlePool.deploy(vertx, execution -> {
            if (starts.getAndIncrement() == 0) {
                firstContext.tryComplete(Vertx.currentContext());
                return blocked.future();
            }
            secondContext.tryComplete(Vertx.currentContext());
            return Future.succeededFuture();
        }, 2).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            Future<Void> first = pool.execute(execution());
            firstContext.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            Future<Void> second = pool.execute(execution());
            Context secondWorker = secondContext.future().toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            second.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertNotSame(firstContext.future().result(), secondWorker);
            assertFalse(first.isComplete(), "a worker remains active until the handler settles");
            blocked.complete();
            first.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        } finally {
            blocked.tryComplete();
            pool.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void revokedDeliveryNeverInvokesHandlerAndFailureIsPreserved() throws Exception {
        Vertx vertx = Vertx.vertx();
        AtomicInteger starts = new AtomicInteger();
        IllegalStateException failure = new IllegalStateException("handler failed");
        ExecutionVerticlePool pool = ExecutionVerticlePool.deploy(vertx, execution -> {
            starts.incrementAndGet();
            return Future.failedFuture(failure);
        }, 1).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            Future<Void> revoked = pool.execute(execution(), () -> false);
            assertThrows(ExecutionException.class,
                    () -> revoked.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals(0, starts.get());
            Future<Void> result = pool.execute(execution());
            ExecutionException actual = assertThrows(ExecutionException.class,
                    () -> result.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertTrue(actual.getCause() == failure);
            assertEquals(1, starts.get());
        } finally {
            pool.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void revocationWhileWaitingForWorkerContextSuppressesHandler() throws Exception {
        Vertx vertx = Vertx.vertx();
        CountDownLatch release = new CountDownLatch(1);
        Promise<Void> firstStarted = Promise.promise();
        AtomicInteger starts = new AtomicInteger();
        AtomicBoolean active = new AtomicBoolean(true);
        ExecutionVerticlePool pool = ExecutionVerticlePool.deploy(vertx, execution -> {
            starts.incrementAndGet();
            firstStarted.tryComplete();
            try {
                if (!release.await(10, TimeUnit.SECONDS)) {
                    return Future.failedFuture("Test barrier timed out");
                }
            } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return Future.failedFuture(interrupted);
            }
            return Future.succeededFuture();
        }, 1).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        try {
            Future<Void> first = pool.execute(execution());
            firstStarted.future().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            AtomicLong startedAt = new AtomicLong();
            ExecutionVerticlePool.Dispatch queued = pool.dispatch(execution(), active::get, startedAt);
            assertFalse(queued.started().isComplete(), "timeout must not start while delivery is queued");
            active.set(false);
            release.countDown();
            first.toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertThrows(ExecutionException.class,
                    () -> queued.completion().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals(0, startedAt.get());
            assertEquals(1, starts.get());
        } finally {
            release.countDown();
            pool.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static JobExecution execution() {
        JobEnvelope job = new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode(), 3);
        return new JobExecution(new SourceRecord("jobs.main.v1", 0, 0), job, 1);
    }
}
