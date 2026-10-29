package com.example.agenticjobscheduler.messaging;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;

/** Normalized output of EnvelopeValidator; payload access returns a defensive copy. */
public record JobEnvelope(int schemaVersion, UUID jobId, UUID executionId,
                          String jobType, ObjectNode payload, int remainingAttempts) {
    public JobEnvelope {
        if (remainingAttempts < 0 || remainingAttempts > 100) {
            throw new IllegalArgumentException("Invalid remaining attempts");
        }
        payload = payload.deepCopy();
    }

    /** One failed execution consumes one unit; zero is terminal and only serialized to DLQ. */
    public JobEnvelope afterFailure() {
        if (remainingAttempts == 0) {
            throw new IllegalStateException("No attempts remain");
        }
        return new JobEnvelope(schemaVersion, jobId, executionId, jobType, payload,
                remainingAttempts - 1);
    }

    @Override
    public ObjectNode payload() {
        return payload.deepCopy();
    }

    @Override
    public String toString() {
        return "JobEnvelope[jobId=" + jobId + ", executionId=" + executionId + "]";
    }
}
