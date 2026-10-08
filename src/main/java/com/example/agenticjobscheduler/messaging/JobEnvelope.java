package com.example.agenticjobscheduler.messaging;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.UUID;

/** Normalized output of EnvelopeValidator; payload access returns a defensive copy. */
public record JobEnvelope(int schemaVersion, UUID jobId, UUID executionId,
                          String jobType, ObjectNode payload, int maxAttempts) {
    public JobEnvelope {
        payload = payload.deepCopy();
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
