package com.example.agenticjobscheduler.admission;

import java.util.Arrays;
import java.util.UUID;

public record AdmissionEnvelope(UUID jobId, UUID executionId, int remainingAttempts,
                                byte[] key, byte[] value) {
    public AdmissionEnvelope {
        key = Arrays.copyOf(key, key.length);
        value = Arrays.copyOf(value, value.length);
    }

    @Override
    public byte[] key() {
        return Arrays.copyOf(key, key.length);
    }

    @Override
    public byte[] value() {
        return Arrays.copyOf(value, value.length);
    }
}
