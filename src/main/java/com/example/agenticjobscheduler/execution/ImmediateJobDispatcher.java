package com.example.agenticjobscheduler.execution;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Immediate per-job FIFO dispatch for one topic under stable ownership.
 * Construct and call on one event-loop context. Handler callbacks are marshalled
 * back to that context. This is not a consumer, retry policy, or rebalance manager.
 * Capacity is fixed per instance; process-wide adaptive/rate limits come later.
 */
public final class ImmediateJobDispatcher {
    public enum State { QUEUED, RUNNING, HANDOFF_PENDING, COMPLETED, STUCK }
    public enum HandlerResult { SUCCESS, FAILURE }

    /** Opaque delivery handle. Futures expose results without exposing mutable state. */
    public static final class Submission {
        private final ImmediateJobDispatcher owner;
        private final JobExecution execution;
        private final OffsetCommitTracker.Delivery delivery;
        private final Promise<HandlerResult> stopped = Promise.promise();
        private State state = State.QUEUED;

        private Submission(ImmediateJobDispatcher owner, JobExecution execution,
                           OffsetCommitTracker.Delivery delivery) {
            this.owner = owner;
            this.execution = execution;
            this.delivery = delivery;
        }

        public JobExecution execution() { return execution; }

        /** Completes on the owner context after state changes and dispatch are applied. */
        public Future<HandlerResult> handlerStopped() { return stopped.future(); }
    }

    private final Context context;
    private final String topic;
    private final JobHandler handler;
    private final int capacity;
    private final OffsetCommitTracker tracker;
    private final List<Integer> partitionOrder;
    private final Map<Integer, ArrayDeque<UUID>> runnable = new HashMap<>();
    private final Map<UUID, ArrayDeque<Submission>> lanes = new HashMap<>();
    private int partitionCursor;
    private int activeHandlers;
    private boolean dispatching;

    public ImmediateJobDispatcher(Context context, String topic, Set<Integer> assignment,
                                  int capacity, int maxRetainedRecords, JobHandler handler) {
        this.context = Objects.requireNonNull(context, "context");
        requireContext();
        this.topic = Objects.requireNonNull(topic, "topic");
        this.handler = Objects.requireNonNull(handler, "handler");
        Objects.requireNonNull(assignment, "assignment");
        if (topic.isBlank() || assignment.isEmpty() || capacity < 1) {
            throw new IllegalArgumentException("Topic, assignment and capacity must be valid");
        }
        this.capacity = capacity;
        var partitions = assignment.stream()
                .map(id -> new OffsetCommitTracker.Partition(topic, id)).collect(Collectors.toSet());
        tracker = new OffsetCommitTracker(partitions, maxRetainedRecords);
        partitionOrder = assignment.stream().sorted().toList();
        partitionOrder.forEach(id -> runnable.put(id, new ArrayDeque<>()));
    }

    /** Register fetched records in increasing offset order per partition; never drop rejected input. */
    public Submission submit(JobExecution execution) {
        requireContext();
        Objects.requireNonNull(execution, "execution");
        if (!topic.equals(execution.source().topic())) {
            throw new IllegalArgumentException("Wrong topic for dispatcher");
        }
        // Admission is atomic: tracker rejection must not add a lane or invoke a handler.
        var delivery = tracker.delivered(execution.source());
        var submission = new Submission(this, execution, delivery);
        var lane = lanes.computeIfAbsent(execution.job().jobId(), ignored -> new ArrayDeque<>());
        lane.addLast(submission);
        if (lane.size() == 1) makeRunnable(submission);
        dispatch();
        return submission;
    }

    /** Only the publisher's actual retry/DLQ acknowledgment may release a failed job's gate. */
    public void handoffAcknowledged(Submission submission, OffsetCommitTracker.Completion outcome) {
        requireSubmission(submission);
        if (outcome != OffsetCommitTracker.Completion.RETRY_ACKNOWLEDGED
                && outcome != OffsetCommitTracker.Completion.DLQ_ACKNOWLEDGED) {
            throw new IllegalArgumentException("Expected retry or DLQ acknowledgment");
        }
        if (submission.state != State.HANDOFF_PENDING) {
            throw new IllegalStateException("Submission is not waiting for handoff");
        }
        complete(submission, outcome);
        dispatch();
    }

    public State state(Submission submission) {
        requireSubmission(submission);
        return submission.state;
    }

    public int activeHandlers() {
        requireContext();
        return activeHandlers;
    }

    public int retainedRecords() {
        requireContext();
        return tracker.retainedRecords();
    }

    public Optional<OffsetCommitTracker.CommitBatch> beginCommit() {
        requireContext();
        return tracker.beginCommit();
    }

    public void commitSucceeded(OffsetCommitTracker.CommitBatch batch) {
        requireContext();
        tracker.commitSucceeded(batch);
    }

    public void commitFailed(OffsetCommitTracker.CommitBatch batch) {
        requireContext();
        tracker.commitFailed(batch);
    }

    private void dispatch() {
        if (dispatching) return;
        dispatching = true;
        try {
            while (activeHandlers < capacity) {
                Submission next = nextRunnable();
                if (next == null) break;
                next.state = State.RUNNING;
                activeHandlers++;
                Future<Void> result;
                try {
                    result = handler.execute(next.execution);
                } catch (Exception failure) {
                    // Contract: synchronous throw is allowed only before starting work.
                    result = Future.failedFuture("HANDLER_START_FAILURE");
                }
                if (result == null) {
                    // Contract violation: cannot prove the operation stopped. Keep gate/permit.
                    next.state = State.STUCK;
                    continue;
                }
                result.onComplete(outcome -> context.runOnContext(ignored -> stopped(next, outcome.succeeded())));
            }
        } finally {
            dispatching = false;
        }
    }

    private Submission nextRunnable() {
        for (int checked = 0; checked < partitionOrder.size(); checked++) {
            int partition = partitionOrder.get(partitionCursor);
            partitionCursor = (partitionCursor + 1) % partitionOrder.size();
            UUID jobId = runnable.get(partition).pollFirst();
            if (jobId != null) return lanes.get(jobId).getFirst();
        }
        return null;
    }

    private void stopped(Submission submission, boolean succeeded) {
        requireContext();
        if (submission.state != State.RUNNING) return;
        activeHandlers--;
        if (succeeded) {
            complete(submission, OffsetCommitTracker.Completion.HANDLER_SUCCESS);
        } else {
            submission.state = State.HANDOFF_PENDING;
        }
        dispatch();
        // Notify after accounting and dispatch; observers cannot see a half-applied transition.
        submission.stopped.complete(succeeded ? HandlerResult.SUCCESS : HandlerResult.FAILURE);
    }

    private void complete(Submission submission, OffsetCommitTracker.Completion outcome) {
        tracker.complete(submission.delivery, outcome);
        submission.state = State.COMPLETED;
        UUID jobId = submission.execution.job().jobId();
        var lane = lanes.get(jobId);
        lane.removeFirst();
        if (lane.isEmpty()) lanes.remove(jobId);
        else makeRunnable(lane.getFirst());
    }

    private void makeRunnable(Submission submission) {
        runnable.get(submission.execution.source().partition()).addLast(submission.execution.job().jobId());
    }

    private void requireSubmission(Submission submission) {
        requireContext();
        if (submission == null || submission.owner != this) {
            throw new IllegalArgumentException("Submission belongs to another dispatcher");
        }
    }

    private void requireContext() {
        if (Vertx.currentContext() != context || !context.isEventLoopContext()
                || !Context.isOnEventLoopThread()) {
            throw new IllegalStateException("Dispatcher requires its owner event-loop context");
        }
    }
}
