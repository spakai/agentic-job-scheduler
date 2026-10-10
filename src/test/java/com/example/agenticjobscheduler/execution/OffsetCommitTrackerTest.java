package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.SourceRecord;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import static com.example.agenticjobscheduler.execution.OffsetCommitTracker.Completion.*;
import static org.junit.jupiter.api.Assertions.*;

class OffsetCommitTrackerTest {
    private static final OffsetCommitTracker.Partition MAIN =
            new OffsetCommitTracker.Partition("jobs.main.v1", 0);
    private static final OffsetCommitTracker.Partition OTHER =
            new OffsetCommitTracker.Partition("jobs.main.v1", 1);
    private static final OffsetCommitTracker.Partition RETRY =
            new OffsetCommitTracker.Partition("jobs.retry.v1", 0);

    @Test
    void laterCompletionsCannotSkipUnfinishedHead() {
        var tracker = tracker(3);
        var first = deliver(tracker, MAIN, 10);
        var second = deliver(tracker, MAIN, 11);
        var third = deliver(tracker, MAIN, 12);
        tracker.complete(second, HANDLER_SUCCESS);
        tracker.complete(third, HANDLER_SUCCESS);
        assertTrue(tracker.beginCommit().isEmpty());
        assertEquals(3, tracker.retainedRecords());
        tracker.complete(first, HANDLER_SUCCESS);
        var batch = tracker.beginCommit().orElseThrow();
        assertEquals(Map.of(MAIN, 13L), batch.offsets());
        tracker.commitSucceeded(batch);
        assertEquals(0, tracker.retainedRecords());
        assertTrue(tracker.beginCommit().isEmpty());
    }

    @Test
    void gapsAreNotMissingWorkAndIncompleteMiddleStillBlocks() {
        var tracker = tracker(3);
        var first = deliver(tracker, MAIN, 10);
        var middle = deliver(tracker, MAIN, 20);
        var last = deliver(tracker, MAIN, 35);
        tracker.complete(first, HANDLER_SUCCESS);
        tracker.complete(last, HANDLER_SUCCESS);
        var batch = tracker.beginCommit().orElseThrow();
        assertEquals(Map.of(MAIN, 11L), batch.offsets());
        tracker.commitSucceeded(batch);
        assertTrue(tracker.beginCommit().isEmpty());
        tracker.complete(middle, HANDLER_SUCCESS);
        assertEquals(Map.of(MAIN, 36L), tracker.beginCommit().orElseThrow().offsets());
    }

    @Test
    void partitionsAndTopicsAdvanceIndependently() {
        var tracker = tracker(3);
        deliver(tracker, MAIN, 10);
        tracker.complete(deliver(tracker, OTHER, 50), HANDLER_SUCCESS);
        tracker.complete(deliver(tracker, RETRY, 10), RETRY_ACKNOWLEDGED);
        var batch = tracker.beginCommit().orElseThrow();
        assertEquals(Map.of(OTHER, 51L, RETRY, 11L), batch.offsets());
        tracker.commitSucceeded(batch);
        assertEquals(1, tracker.retainedRecords());
        assertTrue(tracker.beginCommit().isEmpty());
    }

    @Test
    void commitSnapshotIsImmutableAndOnlyOneRequestCanBeOutstanding() {
        var tracker = tracker(3);
        tracker.complete(deliver(tracker, MAIN, 10), HANDLER_SUCCESS);
        var batch = tracker.beginCommit().orElseThrow();
        assertThrows(UnsupportedOperationException.class, () -> batch.offsets().put(MAIN, 999L));
        tracker.complete(deliver(tracker, MAIN, 11), HANDLER_SUCCESS);
        tracker.complete(deliver(tracker, OTHER, 42), HANDLER_SUCCESS);
        assertTrue(tracker.beginCommit().isEmpty());
        assertEquals(Map.of(MAIN, 11L), batch.offsets());
        tracker.commitSucceeded(batch);
        assertEquals(2, tracker.retainedRecords());
        assertEquals(Map.of(MAIN, 12L, OTHER, 43L), tracker.beginCommit().orElseThrow().offsets());
    }

    @Test
    void failedCommitRetainsResultsAndCanRetryWithoutAnotherCompletion() {
        var tracker = tracker(2);
        tracker.complete(deliver(tracker, MAIN, 10), HANDLER_SUCCESS);
        var failed = tracker.beginCommit().orElseThrow();
        tracker.commitFailed(failed);
        assertEquals(1, tracker.retainedRecords());
        var retry = tracker.beginCommit().orElseThrow();
        assertEquals(failed.offsets(), retry.offsets());
        assertThrows(IllegalArgumentException.class, () -> tracker.commitSucceeded(failed));
        assertThrows(IllegalArgumentException.class, () -> tracker.commitFailed(failed));
        assertTrue(tracker.beginCommit().isEmpty());
        tracker.commitSucceeded(retry);
        assertEquals(0, tracker.retainedRecords());
    }

    @Test
    void failedCommitCanCoalesceNewSafeProgress() {
        var tracker = tracker(2);
        tracker.complete(deliver(tracker, MAIN, 10), HANDLER_SUCCESS);
        var failed = tracker.beginCommit().orElseThrow();
        tracker.complete(deliver(tracker, MAIN, 11), HANDLER_SUCCESS);
        tracker.commitFailed(failed);
        assertEquals(Map.of(MAIN, 12L), tracker.beginCommit().orElseThrow().offsets());
    }

    @ParameterizedTest
    @EnumSource(value = OffsetCommitTracker.Completion.class,
            names = {"RETRY_ACKNOWLEDGED", "DLQ_ACKNOWLEDGED"})
    void pendingOrFailedHandoffDoesNotCompleteSource(OffsetCommitTracker.Completion acknowledgment) {
        var tracker = tracker(2);
        var source = deliver(tracker, MAIN, 10);
        tracker.complete(deliver(tracker, MAIN, 11), HANDLER_SUCCESS);
        // The publisher has no acknowledgment: pending and failed sends make no completion call.
        assertTrue(tracker.beginCommit().isEmpty());
        assertEquals(2, tracker.retainedRecords());
        tracker.complete(source, acknowledgment);
        assertEquals(Map.of(MAIN, 12L), tracker.beginCommit().orElseThrow().offsets());
    }

    @Test
    void boundIncludesCompletedAndCommitPendingRecordsAcrossPartitions() {
        var tracker = tracker(2);
        tracker.complete(deliver(tracker, MAIN, 0), HANDLER_SUCCESS);
        var other = deliver(tracker, OTHER, 0);
        var batch = tracker.beginCommit().orElseThrow();
        assertThrows(IllegalStateException.class, () -> deliver(tracker, MAIN, 1));
        tracker.commitFailed(batch);
        assertThrows(IllegalStateException.class, () -> deliver(tracker, MAIN, 1));
        tracker.commitSucceeded(tracker.beginCommit().orElseThrow());
        deliver(tracker, MAIN, 1); // Rejected admission did not consume the offset.
        assertEquals(2, tracker.retainedRecords());
        tracker.complete(other, HANDLER_SUCCESS);
        assertThrows(IllegalStateException.class, () -> deliver(tracker, OTHER, 1));
    }

    @Test
    void deliveryHandlesCannotCompleteAnotherTrackerOrAnAlreadyCommittedDelivery() {
        var first = tracker(1);
        var second = tracker(1);
        var original = deliver(first, MAIN, 10);
        var replay = deliver(second, MAIN, 10);
        assertThrows(IllegalArgumentException.class, () -> second.complete(original, HANDLER_SUCCESS));
        assertTrue(second.beginCommit().isEmpty());
        first.complete(original, HANDLER_SUCCESS);
        first.complete(original, HANDLER_SUCCESS); // Duplicate callback is harmless while retained.
        var batch = first.beginCommit().orElseThrow();
        second.complete(replay, HANDLER_SUCCESS);
        var secondBatch = second.beginCommit().orElseThrow();
        assertThrows(IllegalArgumentException.class, () -> second.commitSucceeded(batch));
        first.commitSucceeded(batch);
        assertThrows(IllegalArgumentException.class, () -> first.complete(original, HANDLER_SUCCESS));
        assertThrows(IllegalArgumentException.class, () -> first.commitSucceeded(batch));
        second.commitSucceeded(secondBatch);
    }

    @Test
    void rejectsInvalidAdmissionWithoutChangingState() {
        var tracker = tracker(3);
        assertThrows(IllegalArgumentException.class, () -> deliver(tracker, MAIN, -1));
        assertThrows(IllegalArgumentException.class, () -> deliver(tracker, MAIN, Long.MAX_VALUE));
        assertThrows(IllegalArgumentException.class,
                () -> deliver(tracker, new OffsetCommitTracker.Partition("unknown", 0), 0));
        var record = deliver(tracker, MAIN, 10);
        assertThrows(IllegalArgumentException.class, () -> deliver(tracker, MAIN, 10));
        assertThrows(IllegalArgumentException.class, () -> deliver(tracker, MAIN, 9));
        tracker.complete(record, HANDLER_SUCCESS);
        tracker.commitSucceeded(tracker.beginCommit().orElseThrow());
        assertThrows(IllegalArgumentException.class, () -> deliver(tracker, MAIN, 10));
        assertEquals(0, tracker.retainedRecords());
        tracker.complete(deliver(tracker, MAIN, Long.MAX_VALUE - 1), HANDLER_SUCCESS);
        assertEquals(Map.of(MAIN, Long.MAX_VALUE), tracker.beginCommit().orElseThrow().offsets());
    }

    @Test
    void rejectsInvalidConfigurationAndNullCompletion() {
        assertThrows(IllegalArgumentException.class, () -> tracker(0));
        assertThrows(IllegalArgumentException.class, () -> new OffsetCommitTracker.Partition(" ", 0));
        assertThrows(IllegalArgumentException.class, () -> new OffsetCommitTracker.Partition("topic", -1));
        var tracker = tracker(1);
        var delivery = deliver(tracker, MAIN, 0);
        assertThrows(NullPointerException.class, () -> tracker.complete(delivery, null));
        assertThrows(IllegalArgumentException.class, () -> tracker.commitSucceeded(null));
        assertTrue(tracker.beginCommit().isEmpty());
        assertTrue(new OffsetCommitTracker(Set.of(), 1).beginCommit().isEmpty());
    }

    @ParameterizedTest
    @EnumSource(OffsetCommitTracker.Completion.class)
    void simulatedCrashAfterCompletionBeforeCommitReplays(OffsetCommitTracker.Completion outcome) {
        var broker = new SimulatedBroker();
        var beforeCrash = tracker(1);
        var source = broker.deliverNext(beforeCrash);
        int effects = 1; // Handler success, retry publication, or DLQ publication happened.
        beforeCrash.complete(source, outcome);
        beforeCrash.beginCommit().orElseThrow(); // Crash before the broker stores this request.
        assertEquals(10, broker.committedOffset);

        var afterRestart = tracker(1); // Only broker state survives; completion memory is lost.
        var replay = broker.deliverNext(afterRestart);
        assertEquals(source.source(), replay.source());
        effects++;
        afterRestart.complete(replay, outcome);
        broker.acknowledge(afterRestart, afterRestart.beginCommit().orElseThrow());
        assertEquals(2, effects); // Duplicates are permitted, not deduplicated by the service.
        assertEquals(11, broker.committedOffset);
        assertNull(broker.deliverNext(tracker(1)));
    }

    @Test
    void simulatedCrashAfterBrokerCommitBeforeCallbackUsesBrokerPosition() {
        var broker = new SimulatedBroker();
        var tracker = tracker(1);
        tracker.complete(broker.deliverNext(tracker), HANDLER_SUCCESS);
        broker.store(tracker.beginCommit().orElseThrow()); // Callback lost in crash.
        assertEquals(1, tracker.retainedRecords());
        assertNull(broker.deliverNext(tracker(1)));
    }

    @Test
    void shuffledCompletionsAndCommitFailuresNeverCrossFirstIncompleteRecord() {
        // Reproducible adversarial schedules; reference frontier uses delivery order.
        for (int seed = 0; seed < 40; seed++) {
            var tracker = tracker(20);
            var deliveries = new ArrayList<OffsetCommitTracker.Delivery>();
            var order = new ArrayList<Integer>();
            boolean[] done = new boolean[20];
            for (int i = 0; i < done.length; i++) {
                deliveries.add(deliver(tracker, MAIN, 10 + i * 3L));
                order.add(i);
            }
            Collections.shuffle(order, new Random(seed));
            long acknowledged = -1;
            for (int index : order) {
                tracker.complete(deliveries.get(index), HANDLER_SUCCESS);
                done[index] = true;
                int prefix = 0;
                while (prefix < done.length && done[prefix]) prefix++;
                long safe = prefix == 0 ? -1 : 11 + (prefix - 1) * 3L;
                var request = tracker.beginCommit();
                assertEquals(safe > acknowledged, request.isPresent());
                if (request.isPresent()) {
                    var batch = request.orElseThrow();
                    assertEquals(Map.of(MAIN, safe), batch.offsets());
                    if (index % 3 == 0) {
                        tracker.commitFailed(batch);
                    } else {
                        tracker.commitSucceeded(batch);
                        acknowledged = safe;
                    }
                }
            }
            tracker.beginCommit().ifPresent(tracker::commitSucceeded);
            assertEquals(0, tracker.retainedRecords());
        }
    }

    private static OffsetCommitTracker tracker(int capacity) {
        return new OffsetCommitTracker(Set.of(MAIN, OTHER, RETRY), capacity);
    }

    private static OffsetCommitTracker.Delivery deliver(
            OffsetCommitTracker tracker, OffsetCommitTracker.Partition partition, long offset) {
        return tracker.delivered(new SourceRecord(partition.topic(), partition.partition(), offset));
    }

    /** Test model only; this does not establish Kafka durability or real crash behavior. */
    private static final class SimulatedBroker {
        private final List<SourceRecord> records = List.of(new SourceRecord(MAIN.topic(), 0, 10));
        private long committedOffset = 10;

        private OffsetCommitTracker.Delivery deliverNext(OffsetCommitTracker tracker) {
            return records.stream().filter(record -> record.offset() >= committedOffset)
                    .findFirst().map(tracker::delivered).orElse(null);
        }

        private void store(OffsetCommitTracker.CommitBatch batch) {
            committedOffset = batch.offsets().get(MAIN);
        }

        private void acknowledge(OffsetCommitTracker tracker, OffsetCommitTracker.CommitBatch batch) {
            store(batch);
            tracker.commitSucceeded(batch);
        }
    }
}
