package com.example.agenticjobscheduler.execution;

import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Applies a process-wide processing-window signal to one Vert.x Kafka consumer.
 * Register the assignment callbacks with this controller and call synchronize
 * after each reservation or acknowledged commit. It owns partition-level
 * pause/resume state for this consumer; it never pauses the ReadStream globally.
 */
public final class ProcessingWindowKafkaController {
    private final Context context;
    private final KafkaConsumer<?, ?> consumer;
    private final ProcessingWindow window;
    private final Set<TopicPartition> assigned = new HashSet<>();
    private final Set<TopicPartition> pausedByWindow = new HashSet<>();
    private Future<Void> synchronization = Future.succeededFuture();

    public ProcessingWindowKafkaController(Context context, KafkaConsumer<?, ?> consumer,
                                           ProcessingWindow window) {
        this.context = Objects.requireNonNull(context, "context");
        requireContext();
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.window = Objects.requireNonNull(window, "window");
    }

    /** Reserve a delivered batch before dispatch; retain rejected records in the caller. */
    public Optional<ProcessingWindow.Reservation> reserveFetched(int records, long bytes) {
        requireContext();
        return window.tryReserve(records, bytes);
    }

    /** Apply the current window state after admission, including a refused batch. */
    public Future<Void> synchronize() {
        requireContext();
        Future<Void> next = synchronization
                .recover(failure -> Future.succeededFuture())
                .compose(ignored -> synchronizeOnce());
        synchronization = next;
        return next;
    }

    /** Record a new assignment and pause it immediately if the shared window is high. */
    public Future<Void> partitionsAssigned(Set<TopicPartition> partitions) {
        requireContext();
        assigned.addAll(Objects.requireNonNull(partitions, "partitions"));
        return synchronize();
    }

    /** Forget revoked partitions; they are no longer controlled by this owner. */
    public void partitionsRevoked(Set<TopicPartition> partitions) {
        requireContext();
        assigned.removeAll(Objects.requireNonNull(partitions, "partitions"));
        pausedByWindow.removeAll(partitions);
    }

    /** Release a reservation only after source-commit acknowledgment, then reconcile. */
    public Future<Void> commitAcknowledged(ProcessingWindow.Reservation reservation) {
        requireContext();
        window.commitAcknowledged(reservation);
        return synchronize();
    }

    private Future<Void> synchronizeOnce() {
        if (window.intakePaused()) {
            Set<TopicPartition> toPause = new HashSet<>(assigned);
            toPause.removeAll(pausedByWindow);
            if (toPause.isEmpty()) {
                return Future.succeededFuture();
            }
            return consumer.pause(Set.copyOf(toPause)).compose(ignored -> {
                toPause.stream().filter(assigned::contains).forEach(pausedByWindow::add);
                return synchronizeOnce();
            });
        }

        Set<TopicPartition> toResume = new HashSet<>(pausedByWindow);
        toResume.retainAll(assigned);
        if (toResume.isEmpty()) {
            return Future.succeededFuture();
        }
        return consumer.resume(Set.copyOf(toResume)).compose(ignored -> {
            pausedByWindow.removeAll(toResume);
            return synchronizeOnce();
        });
    }

    private void requireContext() {
        if (Vertx.currentContext() != context || !context.isEventLoopContext()
                || !Context.isOnEventLoopThread()) {
            throw new IllegalStateException("Kafka window controller requires its owner event-loop context");
        }
    }
}
