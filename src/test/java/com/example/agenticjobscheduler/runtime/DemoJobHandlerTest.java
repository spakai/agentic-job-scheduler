package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Vertx;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DemoJobHandlerTest {
    @Test
    void loadCanOverlapInFlightHandlerExecutions() throws Exception {
        DemoJobHandler.resetMetrics();
        Vertx vertx = Vertx.vertx();
        CountDownLatch allStopped = new CountDownLatch(32);
        try {
            vertx.runOnContext(ignored -> {
                for (int index = 0; index < 32; index++) {
                    var payload = JsonNodeFactory.instance.objectNode()
                            .put("loadMode", "success")
                            .put("loadSequence", index + 1)
                            .put("loadTotal", 32)
                            .put("demoDelayMillis", 50);
                    var execution = new JobExecution(
                            new SourceRecord("jobs.main.v1", 0, index),
                            new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(),
                                    "demo.echo", payload, 3),
                            1);
                    new DemoJobHandler().execute(execution)
                            .onComplete(result -> allStopped.countDown());
                }
            });
            assertTrue(allStopped.await(2, TimeUnit.SECONDS));
            var metrics = DemoJobHandler.metrics();
            assertTrue(metrics.peakActive() > 1,
                    "Expected overlapping demo handlers, peak=" + metrics.peakActive());
            assertEquals(32, metrics.completed());
            assertEquals(32, metrics.loadCompleted());
            assertEquals(0, metrics.active());
        } finally {
            vertx.close();
        }
    }

    @Test
    void normalSampleSucceeds() {
        assertTrue(new DemoJobHandler().execute(execution(false)).succeeded());
    }

    @Test
    void explicitFailureSampleFailsWithoutChangingInputBudget() {
        var execution = execution(true);
        var result = new DemoJobHandler().execute(execution);
        assertTrue(result.failed());
        assertEquals("DEMO_FAILURE", result.cause().getMessage());
        assertEquals(3, execution.job().remainingAttempts());
    }

    private static JobExecution execution(boolean fail) {
        var payload = JsonNodeFactory.instance.objectNode();
        if (fail) {
            payload.put("demoFail", true);
        }
        return new JobExecution(new SourceRecord("jobs.main.v1", 0, 0),
                new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(), "demo.echo", payload, 3), 1);
    }
}
