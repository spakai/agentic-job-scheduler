package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.JobHandler;
import io.vertx.core.Future;
import java.util.logging.Logger;

/** Replay-safe demo handler with no irreversible external effects. */
public final class DemoJobHandler implements JobHandler {
    private static final Logger LOG = Logger.getLogger(DemoJobHandler.class.getName());

    @Override
    public Future<Void> execute(JobExecution execution) {
        LOG.info(() -> "Handled jobType=" + execution.job().jobType()
                + " jobId=" + execution.job().jobId()
                + " executionId=" + execution.job().executionId()
                + " attempt=" + execution.attempt());
        return Future.succeededFuture();
    }
}
