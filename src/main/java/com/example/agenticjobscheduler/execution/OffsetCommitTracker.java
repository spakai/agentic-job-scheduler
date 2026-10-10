package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.SourceRecord;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Completion bookkeeping for a fixed, stable assignment. Contains no Kafka I/O.
 * All calls, including commit results, must be serialized on the owner's context.
 * Register every delivered record in partition order before dispatching it.
 * Create a fresh tracker after ownership changes; this is not a rebalance manager.
 */
public final class OffsetCommitTracker {
    public record Partition(String topic, int partition) {
        public Partition {
            Objects.requireNonNull(topic, "topic");
            if (topic.isBlank() || partition < 0) {
                throw new IllegalArgumentException("Invalid partition");
            }
        }
    }

    /** A failed or still-pending publication is deliberately not a completion. */
    public enum Completion { HANDLER_SUCCESS, RETRY_ACKNOWLEDGED, DLQ_ACKNOWLEDGED }

    /** Opaque token for one delivery in this tracker, not a durable identity. */
    public static final class Delivery {
        private final SourceRecord source;
        private boolean complete;

        private Delivery(SourceRecord source) {
            this.source = source;
        }

        public SourceRecord source() { return source; }
    }

    /** Immutable explicit offsets; pass this exact handle back on commit completion. */
    public static final class CommitBatch {
        private final Map<Partition, Long> offsets;

        private CommitBatch(Map<Partition, Long> offsets) {
            this.offsets = Map.copyOf(offsets);
        }

        public Map<Partition, Long> offsets() { return offsets; }
    }

    private static final class PartitionState {
        private final Map<Long, Delivery> records = new LinkedHashMap<>();
        private long lastDelivered = -1;
    }

    private final Map<Partition, PartitionState> partitions = new LinkedHashMap<>();
    private final int maxRetainedRecords;
    private int retainedRecords;
    private CommitBatch inFlight;

    /** Bound includes incomplete and completed-but-not-acknowledged records. */
    public OffsetCommitTracker(Set<Partition> assignment, int maxRetainedRecords) {
        Objects.requireNonNull(assignment, "assignment");
        if (maxRetainedRecords < 1) {
            throw new IllegalArgumentException("Record bound must be positive");
        }
        for (Partition partition : assignment) {
            partitions.put(Objects.requireNonNull(partition, "partition"), new PartitionState());
        }
        this.maxRetainedRecords = maxRetainedRecords;
    }

    /**
     * Rejects admission at the bound without changing state. The caller must retain
     * fetched records and pause intake; rejection never authorizes dropping work.
     */
    public Delivery delivered(SourceRecord source) {
        Objects.requireNonNull(source, "source");
        PartitionState state = state(source);
        if (source.offset() < 0 || source.offset() == Long.MAX_VALUE
                || source.offset() <= state.lastDelivered) {
            throw new IllegalArgumentException("Offsets must increase and allow a next offset");
        }
        if (retainedRecords == maxRetainedRecords) {
            throw new IllegalStateException("Retained record bound reached");
        }
        Delivery delivery = new Delivery(source);
        state.records.put(source.offset(), delivery);
        state.lastDelivered = source.offset();
        retainedRecords++;
        return delivery;
    }

    /** Call only after handler success or the required broker handoff acknowledgment. */
    public void complete(Delivery delivery, Completion completion) {
        Objects.requireNonNull(delivery, "delivery");
        Objects.requireNonNull(completion, "completion");
        PartitionState state = state(delivery.source);
        if (state.records.get(delivery.source.offset()) != delivery) {
            throw new IllegalArgumentException("Delivery is not retained by this tracker");
        }
        delivery.complete = true;
    }

    /**
     * Reserves at most one commit request. Empty means no safe progress or a commit
     * already in flight. The caller sends these explicit offsets, never consumer
     * position, and must report success/failure (including synchronous send failure).
     * Do not report a local timeout as failure while a request is still outstanding.
     */
    public Optional<CommitBatch> beginCommit() {
        if (inFlight != null) {
            return Optional.empty();
        }
        Map<Partition, Long> offsets = new LinkedHashMap<>();
        partitions.forEach((partition, state) -> {
            for (Delivery delivery : state.records.values()) {
                if (!delivery.complete) {
                    break;
                }
                offsets.put(partition, delivery.source.offset() + 1);
            }
        });
        if (offsets.isEmpty()) {
            return Optional.empty();
        }
        inFlight = new CommitBatch(offsets);
        return Optional.of(inFlight);
    }

    /** Release only records covered by this acknowledged snapshot. */
    public void commitSucceeded(CommitBatch batch) {
        requireInFlight(batch);
        batch.offsets.forEach((partition, nextOffset) -> {
            var iterator = partitions.get(partition).records.entrySet().iterator();
            while (iterator.hasNext()) {
                if (iterator.next().getKey() >= nextOffset) {
                    break;
                }
                iterator.remove();
                retainedRecords--;
            }
        });
        inFlight = null;
    }

    /** Retain every result for a later explicit commit; do not rerun handlers. */
    public void commitFailed(CommitBatch batch) {
        requireInFlight(batch);
        inFlight = null;
    }

    public int retainedRecords() { return retainedRecords; }

    private PartitionState state(SourceRecord source) {
        PartitionState state = partitions.get(new Partition(source.topic(), source.partition()));
        if (state == null) {
            throw new IllegalArgumentException("Partition is not assigned");
        }
        return state;
    }

    private void requireInFlight(CommitBatch batch) {
        if (batch == null || batch != inFlight) {
            throw new IllegalArgumentException("Commit is not the active request");
        }
    }
}
