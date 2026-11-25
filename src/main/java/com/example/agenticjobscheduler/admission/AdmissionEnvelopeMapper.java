package com.example.agenticjobscheduler.admission;

import com.example.agenticjobscheduler.messaging.EnvelopeValidationException;
import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.runtime.JobRecordEncoder;
import java.util.Objects;
import java.util.UUID;

public final class AdmissionEnvelopeMapper {
    private final EnvelopeValidator validator;

    public AdmissionEnvelopeMapper(EnvelopeValidator validator) {
        this.validator = Objects.requireNonNull(validator, "validator");
    }

    public AdmissionEnvelope map(AdmissionRequest request) {
        UUID jobId = request.jobId().orElseGet(UUID::randomUUID);
        UUID executionId = request.executionId().orElseGet(UUID::randomUUID);
        JobEnvelope job = new JobEnvelope(
                1, jobId, executionId, request.jobType(), request.payload(),
                request.remainingAttempts());
        byte[] key = JobRecordEncoder.key(jobId);
        byte[] value = JobRecordEncoder.main(job);
        try {
            validator.validateMain(key, value);
        } catch (EnvelopeValidationException failure) {
            throw switch (failure.code()) {
                case RECORD_TOO_LARGE -> new AdmissionException(
                        413, "REQUEST_TOO_LARGE", "Resulting Kafka envelope is too large");
                case UNREGISTERED_JOB_TYPE -> AdmissionException.validation(
                        "jobType is not registered");
                default -> AdmissionException.validation("Job request is invalid");
            };
        }
        return new AdmissionEnvelope(jobId, executionId, request.remainingAttempts(), key, value);
    }
}
