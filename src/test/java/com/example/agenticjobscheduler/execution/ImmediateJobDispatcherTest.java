package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.example.agenticjobscheduler.execution.ImmediateJobDispatcher.State.*;
import static com.example.agenticjobscheduler.execution.OffsetCommitTracker.Completion.*;
import static org.junit.jupiter.api.Assertions.*;

class ImmediateJobDispatcherTest {
    private static final String MAIN = "jobs.main.v1";
    private static final String RETRY = "jobs.retry.v1";
    private static final UUID A = new UUID(0, 1);
    private static final UUID B = new UUID(0, 2);
    private Vertx vertx;
    private Context context;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx(new VertxOptions().setEventLoopPoolSize(2));
        context = vertx.getOrCreateContext();
    }

    @AfterEach
    void closeVertx() throws Exception {
        await(vertx.close());
    }

    @Test
    void startsImmediatelyWithoutTimersAndDoesNotBlockEventLoop() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 10, handler);
        var execution = job(MAIN, 0, 10, A, 101);
        var submitted = on(() -> {
            var result = dispatcher.submit(execution);
            assertEquals(List.of(execution), handler.starts);
            assertEquals(RUNNING, dispatcher.state(result));
            assertEquals(1, dispatcher.activeHandlers());
            return result;
        });
        assertFalse(submitted.handlerStopped().isComplete());
        assertTrue(on(() -> Context.isOnEventLoopThread())); // Loop responds while handler is held.
        handler.succeed(execution);
        assertEquals(ImmediateJobDispatcher.HandlerResult.SUCCESS, await(submitted.handlerStopped()));
        assertEquals(COMPLETED, on(() -> dispatcher.state(submitted)));
        assertEquals(List.of(context), handler.contexts);
    }

    @Test
    void sameJobIsFifoWhileDifferentJobInSamePartitionOverlapsAndOffsetsStaySafe() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 2, 10, handler);
        var a1 = job(MAIN, 0, 10, A, 101);
        var a2 = job(MAIN, 0, 11, A, 102);
        var b1 = job(MAIN, 0, 12, B, 201);
        var submissions = on(() -> List.of(dispatcher.submit(a1), dispatcher.submit(a2), dispatcher.submit(b1)));
        var trace = new ArrayList<String>();
        assertEquals(List.of(a1, b1), handler.starts);
        trace.add("start A1; start B1; A2 waits");
        assertEquals(QUEUED, on(() -> dispatcher.state(submissions.get(1))));
        handler.succeed(b1);
        await(submissions.get(2).handlerStopped());
        assertTrue(on(dispatcher::beginCommit).isEmpty());
        assertEquals(List.of(a1, b1), handler.starts);
        trace.add("finish B1; commit blocked by A1");
        handler.succeed(a1);
        await(submissions.get(0).handlerStopped());
        assertEquals(List.of(a1, b1, a2), handler.starts);
        var prefix = on(() -> dispatcher.beginCommit().orElseThrow());
        assertEquals(Map.of(new OffsetCommitTracker.Partition(MAIN, 0), 11L), prefix.offsets());
        trace.add("finish A1; start A2; safe offset 11");
        on(() -> { dispatcher.commitSucceeded(prefix); return null; });
        handler.succeed(a2);
        await(submissions.get(1).handlerStopped());
        var complete = on(() -> dispatcher.beginCommit().orElseThrow());
        assertEquals(Map.of(new OffsetCommitTracker.Partition(MAIN, 0), 13L), complete.offsets());
        on(() -> { dispatcher.commitSucceeded(complete); return null; });
        assertEquals(0, on(dispatcher::retainedRecords));
        trace.add("finish A2; safe offset 13; commit acknowledged");
        System.out.println("L03 controlled trace: " + String.join(" -> ", trace));
    }

    @ParameterizedTest
    @EnumSource(value = OffsetCommitTracker.Completion.class,
            names = {"RETRY_ACKNOWLEDGED", "DLQ_ACKNOWLEDGED"})
    void failedHandlerReleasesCapacityButHandoffMustReleaseGate(OffsetCommitTracker.Completion ack)
            throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 10, handler);
        var a1 = job(MAIN, 0, 10, A, 101);
        var a2 = job(MAIN, 0, 11, A, 102);
        var b1 = job(MAIN, 0, 12, B, 201);
        var submissions = on(() -> List.of(dispatcher.submit(a1), dispatcher.submit(a2), dispatcher.submit(b1)));
        handler.fail(a1);
        assertEquals(ImmediateJobDispatcher.HandlerResult.FAILURE, await(submissions.get(0).handlerStopped()));
        assertEquals(List.of(a1, b1), handler.starts);
        assertEquals(HANDOFF_PENDING, on(() -> dispatcher.state(submissions.get(0))));
        handler.succeed(b1);
        await(submissions.get(2).handlerStopped());
        // Pending/failed publisher sends cannot release A1: there is no acknowledgment.
        assertEquals(0, on(dispatcher::activeHandlers));
        assertTrue(on(dispatcher::beginCommit).isEmpty());
        assertEquals(List.of(a1, b1), handler.starts); // A1 never reruns, A2 still waits.
        on(() -> {
            assertThrows(IllegalArgumentException.class,
                    () -> dispatcher.handoffAcknowledged(submissions.get(0), HANDLER_SUCCESS));
            dispatcher.handoffAcknowledged(submissions.get(0), ack);
            assertThrows(IllegalStateException.class,
                    () -> dispatcher.handoffAcknowledged(submissions.get(0), ack));
            return null;
        });
        assertEquals(List.of(a1, b1, a2), handler.starts);
        handler.succeed(a2);
        await(submissions.get(1).handlerStopped());
        assertEquals(13L, on(() -> dispatcher.beginCommit().orElseThrow().offsets()
                .get(new OffsetCommitTracker.Partition(MAIN, 0))));
    }

    @Test
    void mainAndRetryHaveIndependentGatesAndPreserveHandlerIdentity() throws Exception {
        var mainHandler = new ControlledHandler();
        var retryHandler = new ControlledHandler();
        var main = create(MAIN, 1, 10, mainHandler);
        var retry = create(RETRY, 1, 10, retryHandler);
        var a1 = job(MAIN, 0, 0, A, 101);
        var a2 = job(MAIN, 0, 1, A, 102);
        var submissions = on(() -> List.of(main.submit(a1), main.submit(a2)));
        mainHandler.fail(a1);
        await(submissions.get(0).handlerStopped());
        var retried = new JobExecution(new SourceRecord(RETRY, 0, 0), a1.job(), 2);
        var retrySubmission = on(() -> retry.submit(retried));
        on(() -> { main.handoffAcknowledged(submissions.get(0), RETRY_ACKNOWLEDGED); return null; });
        assertEquals(RUNNING, on(() -> main.state(submissions.get(1))));
        assertEquals(RUNNING, on(() -> retry.state(retrySubmission)));
        assertEquals(a1.job().jobId(), retryHandler.starts.getFirst().job().jobId());
        assertEquals(a1.job().executionId(), retryHandler.starts.getFirst().job().executionId());
        assertEquals(2, retryHandler.starts.getFirst().attempt());
        assertEquals(RETRY, retryHandler.starts.getFirst().source().topic());
        mainHandler.succeed(a2);
        retryHandler.succeed(retried);
        await(submissions.get(1).handlerStopped());
        await(retrySubmission.handlerStopped());
    }

    @Test
    void readyPartitionsTakeTurnsAndSamePartitionLanesRemainFair() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 10, handler);
        var a1 = job(MAIN, 0, 0, A, 101);
        var a2 = job(MAIN, 0, 1, A, 102);
        var b = job(MAIN, 0, 2, B, 201);
        var c = job(MAIN, 1, 0, new UUID(0, 3), 301);
        var d = job(MAIN, 1, 1, new UUID(0, 4), 401);
        var submissions = on(() -> List.of(dispatcher.submit(a1), dispatcher.submit(a2),
                dispatcher.submit(b), dispatcher.submit(c), dispatcher.submit(d)));
        var expected = List.of(a1, c, b, d, a2);
        for (int index = 0; index < expected.size(); index++) {
            assertEquals(expected.subList(0, index + 1), handler.starts);
            var current = expected.get(index);
            handler.succeed(current);
            var submission = submissions.stream().filter(s -> s.execution().equals(current)).findFirst().orElseThrow();
            await(submission.handlerStopped());
        }
        assertEquals(0, on(dispatcher::activeHandlers));
    }

    @Test
    void sameJobIsExcludedEvenAcrossAssignedPartitions() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 2, 10, handler);
        var first = job(MAIN, 0, 0, A, 101);
        var second = job(MAIN, 1, 0, A, 102);
        var submissions = on(() -> List.of(dispatcher.submit(first), dispatcher.submit(second)));
        assertEquals(List.of(first), handler.starts);
        handler.succeed(first);
        await(submissions.getFirst().handlerStopped());
        assertEquals(List.of(first, second), handler.starts);
        handler.succeed(second);
        await(submissions.get(1).handlerStopped());
    }

    @Test
    void boundAndFailedCommitRetainWorkWithoutRerunningHandlers() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 2, 2, handler);
        var a = job(MAIN, 0, 0, A, 101);
        var b = job(MAIN, 0, 1, B, 201);
        var c = job(MAIN, 0, 2, new UUID(0, 3), 301);
        var submissions = on(() -> List.of(dispatcher.submit(a), dispatcher.submit(b)));
        handler.succeed(b);
        await(submissions.get(1).handlerStopped());
        on(() -> { assertThrows(IllegalStateException.class, () -> dispatcher.submit(c)); return null; });
        handler.succeed(a);
        await(submissions.getFirst().handlerStopped());
        on(() -> {
            var batch = dispatcher.beginCommit().orElseThrow();
            assertTrue(dispatcher.beginCommit().isEmpty());
            dispatcher.commitFailed(batch);
            assertEquals(2, dispatcher.retainedRecords());
            assertThrows(IllegalStateException.class, () -> dispatcher.submit(c));
            dispatcher.commitSucceeded(dispatcher.beginCommit().orElseThrow());
            return null;
        });
        var accepted = on(() -> dispatcher.submit(c));
        assertEquals(List.of(a, b, c), handler.starts);
        handler.succeed(c);
        await(accepted.handlerStopped());
    }

    @Test
    void pendingHandoffCountsTowardAdmissionBound() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 1, handler);
        var a = job(MAIN, 0, 0, A, 101);
        var submission = on(() -> dispatcher.submit(a));
        handler.fail(a);
        await(submission.handlerStopped());
        on(() -> {
            assertThrows(IllegalStateException.class, () -> dispatcher.submit(job(MAIN, 0, 1, B, 201)));
            assertEquals(1, dispatcher.retainedRecords());
            assertEquals(HANDOFF_PENDING, dispatcher.state(submission));
            return null;
        });
    }

    @Test
    void alreadyCompletedFuturesAndSynchronousFailuresDoNotReenterDispatch() throws Exception {
        var starts = new ArrayList<JobExecution>();
        var dispatcher = create(MAIN, 1, 10, execution -> {
            starts.add(execution);
            if (execution.source().offset() == 0) throw new IllegalStateException("synthetic pre-start failure");
            return Future.succeededFuture();
        });
        var submissions = on(() -> List.of(dispatcher.submit(job(MAIN, 0, 0, A, 101)),
                dispatcher.submit(job(MAIN, 0, 1, A, 102)), dispatcher.submit(job(MAIN, 0, 2, B, 201))));
        await(submissions.getFirst().handlerStopped());
        await(submissions.get(2).handlerStopped());
        assertEquals(2, on(starts::size));
        on(() -> { dispatcher.handoffAcknowledged(submissions.getFirst(), DLQ_ACKNOWLEDGED); return null; });
        await(submissions.get(1).handlerStopped());
        assertEquals(3, on(starts::size));
        assertEquals(0, on(dispatcher::activeHandlers));
    }

    @Test
    void nullFutureCannotManufactureStoppedWorkOrReleaseGate() throws Exception {
        var dispatcher = create(MAIN, 1, 10, execution -> null);
        var submissions = on(() -> List.of(dispatcher.submit(job(MAIN, 0, 0, A, 101)),
                dispatcher.submit(job(MAIN, 0, 1, A, 102)), dispatcher.submit(job(MAIN, 0, 2, B, 201))));
        on(() -> {
            assertEquals(STUCK, dispatcher.state(submissions.getFirst()));
            assertEquals(QUEUED, dispatcher.state(submissions.get(1)));
            assertEquals(QUEUED, dispatcher.state(submissions.get(2)));
            assertEquals(1, dispatcher.activeHandlers());
            assertTrue(dispatcher.beginCommit().isEmpty());
            assertThrows(IllegalStateException.class,
                    () -> dispatcher.handoffAcknowledged(submissions.getFirst(), DLQ_ACKNOWLEDGED));
            return null;
        });
        assertFalse(submissions.getFirst().handlerStopped().isComplete());
    }

    @Test
    void rejectsWrongContextTopicPartitionOffsetAndForeignHandles() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 10, handler);
        assertThrows(IllegalStateException.class, dispatcher::beginCommit);
        assertThrows(IllegalStateException.class, () -> dispatcher.submit(job(MAIN, 0, 0, A, 101)));
        var other = create(MAIN, 1, 10, handler);
        on(() -> {
            assertThrows(IllegalArgumentException.class, () -> dispatcher.submit(job(RETRY, 0, 0, A, 101)));
            assertThrows(IllegalArgumentException.class, () -> dispatcher.submit(job(MAIN, 2, 0, A, 101)));
            assertThrows(IllegalArgumentException.class, () -> dispatcher.submit(job(MAIN, 0, -1, A, 101)));
            assertEquals(0, dispatcher.retainedRecords());
            var own = dispatcher.submit(job(MAIN, 0, 0, A, 101));
            assertThrows(IllegalStateException.class, () -> dispatcher.handoffAcknowledged(own, DLQ_ACKNOWLEDGED));
            assertThrows(IllegalArgumentException.class, () -> other.state(own));
            assertThrows(IllegalArgumentException.class, () -> dispatcher.submit(job(MAIN, 0, 0, B, 201)));
            assertEquals(1, dispatcher.retainedRecords());
            return null;
        });
        Context otherContext = vertx.getOrCreateContext();
        assertNotSame(context, otherContext);
        var rejected = new CompletableFuture<Boolean>();
        otherContext.runOnContext(ignored -> {
            try {
                dispatcher.activeHandlers();
                rejected.complete(false);
            } catch (IllegalStateException expected) {
                rejected.complete(true);
            }
        });
        assertTrue(rejected.get(5, TimeUnit.SECONDS));
    }

    @Test
    void replayKeepsIdentityAndRunsAgainWithoutDeduplication() throws Exception {
        var handler = new ControlledHandler();
        var dispatcher = create(MAIN, 1, 10, handler);
        var original = job(MAIN, 0, 0, A, 101);
        var repeated = new JobExecution(new SourceRecord(MAIN, 0, 1), original.job(), 1);
        var submissions = on(() -> List.of(dispatcher.submit(original), dispatcher.submit(repeated)));
        handler.succeed(original);
        await(submissions.getFirst().handlerStopped());
        assertEquals(List.of(original, repeated), handler.starts);
        assertEquals(original.job().executionId(), handler.starts.get(1).job().executionId());
        handler.succeed(repeated);
        await(submissions.get(1).handlerStopped());
    }

    @Test
    void validatesConfigurationAndLogicalAttempt() throws Exception {
        on(() -> {
            assertThrows(IllegalArgumentException.class,
                    () -> new ImmediateJobDispatcher(context, MAIN, Set.of(0), 0, 1, e -> Future.succeededFuture()));
            assertThrows(IllegalArgumentException.class,
                    () -> new ImmediateJobDispatcher(context, MAIN, Set.of(), 1, 1, e -> Future.succeededFuture()));
            assertThrows(IllegalArgumentException.class,
                    () -> new ImmediateJobDispatcher(context, MAIN, Set.of(-1), 1, 1, e -> Future.succeededFuture()));
            return null;
        });
        var valid = job(MAIN, 0, 0, A, 101);
        assertThrows(IllegalArgumentException.class, () -> new JobExecution(valid.source(), valid.job(), 0));
        assertThrows(IllegalArgumentException.class, () -> new JobExecution(valid.source(), valid.job(), 4));
    }

    private ImmediateJobDispatcher create(String topic, int capacity, int bound, JobHandler handler)
            throws Exception {
        return on(() -> new ImmediateJobDispatcher(context, topic, Set.of(0, 1), capacity, bound, handler));
    }

    private static JobExecution job(String topic, int partition, long offset, UUID jobId, long executionId) {
        var envelope = new JobEnvelope(1, jobId, new UUID(0, executionId), "demo.echo",
                JsonNodeFactory.instance.objectNode(), 3);
        return new JobExecution(new SourceRecord(topic, partition, offset), envelope, 1);
    }

    private <T> T on(Callable<T> action) throws Exception {
        var result = new CompletableFuture<T>();
        context.runOnContext(ignored -> {
            try { result.complete(action.call()); }
            catch (Throwable failure) { result.completeExceptionally(failure); }
        });
        return result.get(5, TimeUnit.SECONDS);
    }

    private static <T> T await(Future<T> future) throws Exception {
        return future.toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private static final class ControlledHandler implements JobHandler {
        private final Map<SourceRecord, Promise<Void>> gates = new ConcurrentHashMap<>();
        private final List<JobExecution> starts = new CopyOnWriteArrayList<>();
        private final List<Context> contexts = new CopyOnWriteArrayList<>();

        public Future<Void> execute(JobExecution execution) {
            contexts.add(Vertx.currentContext());
            starts.add(execution);
            return gates.computeIfAbsent(execution.source(), ignored -> Promise.promise()).future();
        }

        private void succeed(JobExecution execution) { gates.get(execution.source()).complete(); }
        private void fail(JobExecution execution) { gates.get(execution.source()).fail("SYNTHETIC_FAILURE"); }
    }
}
