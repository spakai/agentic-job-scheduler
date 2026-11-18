package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.JobHandler;
import io.vertx.core.Future;
import java.time.Instant;
import java.util.logging.Logger;

/** Replay-safe demo handler with no irreversible external effects. */
public final class DemoJobHandler implements JobHandler {
    private static final Logger LOG = Logger.getLogger(DemoJobHandler.class.getName());

    @Override
    public Future<Void> execute(JobExecution execution) {
        boolean fail = execution.job().payload().path("demoFail").asBoolean(false);
        LOG.info(() -> (fail ? "Demo failure jobType=" : "Handled jobType=") + execution.job().jobType()
                + " timestamp=" + Instant.now()
                + " jobId=" + execution.job().jobId()
                + " executionId=" + execution.job().executionId()
                + " loadSequence=" + execution.job().payload().path("loadSequence").asText("-")
                + " loadTotal=" + execution.job().payload().path("loadTotal").asText("-")
                + " attempt=" + execution.attempt()
                + " remainingAttempts=" + execution.job().remainingAttempts()
                + " topic=" + execution.source().topic()
                + " partition=" + execution.source().partition()
                + " offset=" + execution.source().offset());
        return fail ? Future.failedFuture(new IllegalStateException("DEMO_FAILURE"))
                : Future.succeededFuture();
    }
}
