package com.example.agenticjobscheduler.execution;

/** A non-transient handler signal excluded from adaptive pressure metrics.
 * Like all handler failures, it consumes remainingAttempts and reaches DLQ only at zero. */
public final class PermanentJobFailure extends RuntimeException {
    public PermanentJobFailure() {
        super("PERMANENT_HANDLER_FAILURE");
    }
}
