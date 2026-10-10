package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import java.util.Objects;

/** A validated job invocation; adapters supply main attempt 1 or validated retry attempt. */
public record JobExecution(SourceRecord source, JobEnvelope job, int attempt) {
    public JobExecution {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(job.jobId(), "jobId");
        Objects.requireNonNull(job.executionId(), "executionId");
        if (attempt < 1 || attempt > job.maxAttempts()) {
            throw new IllegalArgumentException("Invalid attempt");
        }
    }
}
