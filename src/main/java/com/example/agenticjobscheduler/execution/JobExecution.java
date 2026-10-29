package com.example.agenticjobscheduler.execution;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import java.util.Objects;

/** A validated job invocation; adapters supply main attempt 1 or validated retry attempt. */
public record JobExecution(SourceRecord source, JobEnvelope job, int attempt,
                           HandlerCancellation cancellation) {
    public JobExecution(SourceRecord source, JobEnvelope job, int attempt) {
        this(source, job, attempt, new HandlerCancellation());
    }

    public JobExecution {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(job, "job");
        Objects.requireNonNull(cancellation, "cancellation");
        Objects.requireNonNull(job.jobId(), "jobId");
        Objects.requireNonNull(job.executionId(), "executionId");
        if (attempt < 1 || job.remainingAttempts() < 1
                || attempt > 101 - job.remainingAttempts()) {
            throw new IllegalArgumentException("Invalid attempt");
        }
    }
}
