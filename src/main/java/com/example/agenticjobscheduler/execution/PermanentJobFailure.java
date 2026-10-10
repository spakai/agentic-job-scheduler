package com.example.agenticjobscheduler.execution;

/** A handler failure that must go directly to the DLQ without retrying. */
public final class PermanentJobFailure extends RuntimeException {
    public PermanentJobFailure() {
        super("PERMANENT_HANDLER_FAILURE");
    }
}
