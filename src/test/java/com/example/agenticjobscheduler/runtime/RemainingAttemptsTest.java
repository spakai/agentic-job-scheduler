package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RemainingAttemptsTest {
    @Test
    void serializesCountdownWithStableIdentityAndTerminalZero() throws Exception {
        JobEnvelope original = job(3);
        SourceRecord source = new SourceRecord("jobs.main.v1", 0, 0);
        var validator = new EnvelopeValidator(Set.of("demo.echo"));
        byte[] key = JobRecordEncoder.key(original.jobId());
        JobEnvelope current = validator.validateMain(key, JobRecordEncoder.main(original));
        for (int attempt = 2; attempt <= 3; attempt++) {
            current = current.afterFailure();
            SourceRecord failed = attempt == 2 ? source : new SourceRecord("jobs.retry.v1", 0, 0);
            byte[] encoded = JobRecordEncoder.retry(current, attempt, failed, source, "HANDLER_FAILURE");
            var retry = validator.validateRetry(key, encoded);
            assertEquals(4 - attempt, retry.job().remainingAttempts());
            assertEquals(original.jobId(), retry.job().jobId());
            assertEquals(original.executionId(), retry.job().executionId());
            assertEquals(original.payload(), retry.job().payload());
            assertDoesNotThrow(() -> new JobExecution(failed, retry.job(), retry.attempt()));
            current = retry.job();
        }
        JobEnvelope exhausted = current.afterFailure();
        var dlq = new ObjectMapper().readTree(JobRecordEncoder.deadLetter(exhausted, 3,
                new SourceRecord("jobs.retry.v1", 0, 1), source, "ATTEMPTS_EXHAUSTED"));
        assertEquals(0, dlq.path("job").path("remainingAttempts").asInt(-1));
        assertEquals(3, original.remainingAttempts(), "source envelope must remain immutable for replay");
        assertThrows(IllegalArgumentException.class, () -> new JobExecution(source, exhausted, 4));
        assertThrows(IllegalStateException.class, exhausted::afterFailure);
    }

    @Test
    void oneRemainingAttemptExhaustsOnFirstFailure() {
        assertEquals(0, job(1).afterFailure().remainingAttempts());
    }

    private static JobEnvelope job(int remaining) {
        return new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode(), remaining);
    }
}
