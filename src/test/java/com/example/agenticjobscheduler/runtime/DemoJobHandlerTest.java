package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class DemoJobHandlerTest {
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
