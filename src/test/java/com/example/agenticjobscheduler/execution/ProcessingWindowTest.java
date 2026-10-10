package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.SourceRecord;
import java.util.Set;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class ProcessingWindowTest {
    @Test
    void pausesAtCountWatermarkAndResumesOnlyBelowHalf() {
        var window = new ProcessingWindow(10, 1_000, 2, 400);
        var first = window.tryReserve(2, 100).orElseThrow();
        var second = window.tryReserve(2, 100).orElseThrow();
        var third = window.tryReserve(2, 100).orElseThrow();
        assertFalse(window.intakePaused());

        var fourth = window.tryReserve(2, 200).orElseThrow();
        assertTrue(window.intakePaused());
        assertEquals(8, window.retainedRecords());
        assertEquals(500, window.retainedBytes());
        assertTrue(window.tryReserve(1, 10).isEmpty());

        window.commitAcknowledged(first);
        assertTrue(window.intakePaused());
        window.commitAcknowledged(second);
        assertTrue(window.intakePaused());
        window.commitAcknowledged(third);
        assertFalse(window.intakePaused());
        assertEquals(2, window.retainedRecords());
        assertEquals(200, window.retainedBytes());

        window.commitAcknowledged(fourth);
        assertEquals(0, window.retainedRecords());
        assertEquals(0, window.retainedBytes());
    }

    @Test
    void byteWatermarkPausesAndCountReleaseAloneCannotResume() {
        var window = new ProcessingWindow(20, 1_000, 2, 200);
        var reservations = new ProcessingWindow.Reservation[4];
        for (int i = 0; i < reservations.length; i++) {
            reservations[i] = window.tryReserve(1, 200).orElseThrow();
        }
        assertTrue(window.intakePaused());
        assertEquals(800, window.retainedBytes());

        window.commitAcknowledged(reservations[0]);
        window.commitAcknowledged(reservations[1]);
        assertTrue(window.intakePaused());
        window.commitAcknowledged(reservations[2]);
        assertFalse(window.intakePaused());
        window.commitAcknowledged(reservations[3]);
        assertFalse(window.intakePaused());
    }

    @Test
    void oneOutstandingFetchCanOvershootPauseWatermarkButNeverHardBound() {
        var window = new ProcessingWindow(10, 1_000, 3, 400);
        var current = window.tryReserve(2, 250).orElseThrow();
        var previous = window.tryReserve(2, 250).orElseThrow();
        assertFalse(window.intakePaused());

        var fetchedOvershoot = window.tryReserve(2, 200).orElseThrow();
        assertTrue(window.intakePaused());
        assertEquals(6, window.retainedRecords());
        assertEquals(700, window.retainedBytes());
        assertTrue(window.tryReserve(1, 1).isEmpty());
        assertEquals(6, window.retainedRecords());
        assertEquals(700, window.retainedBytes());

        window.commitAcknowledged(current);
        assertTrue(window.intakePaused());
        window.commitAcknowledged(previous);
        assertFalse(window.intakePaused());
        window.commitAcknowledged(fetchedOvershoot);
    }

    @Test
    void rejectsInvalidOrForeignReservationsAndDoesNotFreeBeforeCommitAck() {
        var window = new ProcessingWindow(10, 1_000, 2, 200);
        var reservation = window.tryReserve(2, 100).orElseThrow();
        assertThrows(IllegalArgumentException.class, () -> window.tryReserve(3, 100));
        assertThrows(IllegalArgumentException.class, () -> window.tryReserve(1, 201));
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingWindow(2, 1_000, 2, 200));
        assertEquals(2, window.retainedRecords());
        assertEquals(100, window.retainedBytes());
        assertThrows(IllegalArgumentException.class,
                () -> new ProcessingWindow(10, 1_000, 2, 200).commitAcknowledged(reservation));

        window.commitAcknowledged(reservation);
        assertThrows(IllegalArgumentException.class, () -> window.commitAcknowledged(reservation));
        assertEquals(0, window.retainedRecords());
        assertEquals(0, window.retainedBytes());
    }

    @Test
    void retainsWindowReservationAcrossFailedCommitAndReleasesAfterAcknowledgment() {
        var partition = new OffsetCommitTracker.Partition("jobs.main.v1", 0);
        var tracker = new OffsetCommitTracker(Set.of(partition), 2);
        var window = new ProcessingWindow(10, 1_000, 2, 200);
        var source = new SourceRecord(partition.topic(), partition.partition(), 0);
        var delivery = tracker.delivered(source);
        var reservation = window.tryReserve(1, 100).orElseThrow();
        tracker.complete(delivery, OffsetCommitTracker.Completion.HANDLER_SUCCESS);

        var failed = tracker.beginCommit().orElseThrow();
        tracker.commitFailed(failed);
        assertEquals(1, window.retainedRecords());
        assertEquals(100, window.retainedBytes());

        var retry = tracker.beginCommit().orElseThrow();
        tracker.commitSucceeded(retry);
        window.commitAcknowledged(reservation);
        assertEquals(0, window.retainedRecords());
        assertEquals(0, window.retainedBytes());
    }
}
