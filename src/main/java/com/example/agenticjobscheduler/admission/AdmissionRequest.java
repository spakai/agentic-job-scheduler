package com.example.agenticjobscheduler.admission;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.Optional;
import java.util.UUID;

public record AdmissionRequest(
        Optional<UUID> jobId,
        Optional<UUID> executionId,
        String jobType,
        ObjectNode payload,
        int remainingAttempts) {
    public AdmissionRequest {
        payload = payload.deepCopy();
    }

    @Override
    public ObjectNode payload() {
        return payload.deepCopy();
    }
}
