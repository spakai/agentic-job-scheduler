package com.example.agenticjobscheduler.execution;

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * Thread-safe process-wide accounting for fetched work retained until its source
 * commit is acknowledged. The caller must include key/value bytes consistently
 * and release reservations only for broker-acknowledged committed records.
 */
public final class ProcessingWindow {
    public static final int DEFAULT_MAX_RECORDS = 1_000;
    public static final long DEFAULT_MAX_BYTES = 16L * 1024 * 1024;

    /** Opaque reservation for fetched work; one reservation may cover a fetched batch. */
    public static final class Reservation {
        private final ProcessingWindow owner;
        private final int records;
        private final long bytes;
        private boolean active = true;

        private Reservation(ProcessingWindow owner, int records, long bytes) {
            this.owner = owner;
            this.records = records;
            this.bytes = bytes;
        }
    }

    private final int maxRecords;
    private final long maxBytes;
    private final int maxFetchRecords;
    private final long maxFetchBytes;
    private final int pauseAtRecords;
    private final long pauseAtBytes;
    private final Set<Reservation> active =
            Collections.newSetFromMap(new IdentityHashMap<>());
    private int retainedRecords;
    private long retainedBytes;
    private boolean intakePaused;

    /**
     * @param maxRecords hard bound for all retained records
     * @param maxBytes hard bound for retained key/value bytes
     * @param maxFetchRecords maximum records in the one outstanding fetch batch
     * @param maxFetchBytes maximum key/value bytes in the one outstanding fetch batch
     */
    public ProcessingWindow(int maxRecords, long maxBytes, int maxFetchRecords, long maxFetchBytes) {
        if (maxRecords < 2 || maxBytes < 2 || maxFetchRecords < 1
                || maxFetchRecords >= maxRecords || maxFetchBytes < 1 || maxFetchBytes >= maxBytes) {
            throw new IllegalArgumentException("Invalid processing-window or fetch bounds");
        }
        this.maxRecords = maxRecords;
        this.maxBytes = maxBytes;
        this.maxFetchRecords = maxFetchRecords;
        this.maxFetchBytes = maxFetchBytes;
        pauseAtRecords = maxRecords - maxFetchRecords;
        pauseAtBytes = maxBytes - maxFetchBytes;
    }

    public ProcessingWindow() {
        this(DEFAULT_MAX_RECORDS, DEFAULT_MAX_BYTES, 100, 2L * 1024 * 1024);
    }

    /**
     * Accounts one fetched batch. A rejected batch must remain owned by the
     * caller; it must not be discarded or dispatched without a reservation.
     */
    public synchronized Optional<Reservation> tryReserve(int records, long bytes) {
        if (records < 1 || records > maxFetchRecords || bytes < 0 || bytes > maxFetchBytes) {
            throw new IllegalArgumentException("Fetched batch exceeds configured fetch bounds");
        }
        if (intakePaused || retainedRecords > maxRecords - records || retainedBytes > maxBytes - bytes) {
            intakePaused = true;
            return Optional.empty();
        }
        var reservation = new Reservation(this, records, bytes);
        active.add(reservation);
        retainedRecords += records;
        retainedBytes += bytes;
        updatePauseState();
        return Optional.of(reservation);
    }

    /** Release only after the corresponding source prefix is commit-acknowledged. */
    public synchronized void commitAcknowledged(Reservation reservation) {
        Objects.requireNonNull(reservation, "reservation");
        if (reservation.owner != this || !reservation.active || !active.remove(reservation)) {
            throw new IllegalArgumentException("Reservation is not active in this processing window");
        }
        reservation.active = false;
        retainedRecords -= reservation.records;
        retainedBytes -= reservation.bytes;
        updatePauseState();
    }

    public synchronized boolean intakePaused() {
        return intakePaused;
    }

    public synchronized int retainedRecords() {
        return retainedRecords;
    }

    public synchronized long retainedBytes() {
        return retainedBytes;
    }

    private void updatePauseState() {
        if (intakePaused) {
            if (retainedRecords < pauseAtRecords / 2 + pauseAtRecords % 2
                    && retainedBytes < pauseAtBytes / 2 + pauseAtBytes % 2) {
                intakePaused = false;
            }
        } else if (retainedRecords >= pauseAtRecords || retainedBytes >= pauseAtBytes) {
            intakePaused = true;
        }
    }
}
