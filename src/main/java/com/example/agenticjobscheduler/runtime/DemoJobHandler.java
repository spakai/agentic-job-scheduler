package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.DatabasePressureAdapter;
import com.example.agenticjobscheduler.execution.ExternalPressureRegistry;
import com.example.agenticjobscheduler.execution.ExternalPressureReporter;
import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.JobHandler;
import com.example.agenticjobscheduler.execution.RestPressureAdapter;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

/** Replay-safe demo handler with no irreversible external effects. */
public final class DemoJobHandler implements JobHandler {
    private static final Logger LOG = Logger.getLogger(DemoJobHandler.class.getName());
    private static final AtomicInteger ACTIVE = new AtomicInteger();
    private static final AtomicInteger PEAK_ACTIVE = new AtomicInteger();
    private static final AtomicInteger COMPLETED = new AtomicInteger();
    private static final AtomicInteger LOAD_COMPLETED = new AtomicInteger();
    private static final ExternalPressureRegistry PRESSURE_REGISTRY =
            new ExternalPressureRegistry(List.of("demo-rest", "demo-db"));

    record Metrics(int active, int peakActive, int completed, int loadCompleted) { }

    @Override
    public Future<Void> execute(JobExecution execution) {
        return execute(execution, ExternalPressureReporter.NONE);
    }

    @Override
    public Future<Void> execute(JobExecution execution, ExternalPressureReporter pressure) {
        boolean fail = execution.job().payload().path("demoFail").asBoolean(false);
        String externalResponse = execution.job().payload()
                .path("demoExternalResponse").asText("none");
        boolean pressureAccepted = reportExternalResponse(
                externalResponse, pressure);
        int active = ACTIVE.incrementAndGet();
        PEAK_ACTIVE.updateAndGet(peak -> Math.max(peak, active));
        long delayMillis = execution.job().payload().path("demoDelayMillis").asLong(0);

        LOG.info(() -> (fail ? "Demo failure jobType=" : "Handled jobType=") + execution.job().jobType()
                + " timestamp=" + Instant.now()
                + " jobId=" + execution.job().jobId()
                + " executionId=" + execution.job().executionId()
                + " loadSequence=" + execution.job().payload().path("loadSequence").asText("-")
                + " loadTotal=" + execution.job().payload().path("loadTotal").asText("-")
                + " loadMode=" + execution.job().payload().path("loadMode").asText("-")
                + " demoRunId=" + execution.job().payload().path("demoRunId").asText("-")
                + " externalResponse=" + externalResponse
                + " externalPressureAccepted=" + pressureAccepted
                + " demoDelayMillis=" + delayMillis
                + " activeAtStart=" + active
                + " peakActive=" + PEAK_ACTIVE.get()
                + " attempt=" + execution.attempt()
                + " remainingAttempts=" + execution.job().remainingAttempts()
                + " topic=" + execution.source().topic()
                + " partition=" + execution.source().partition()
                + " offset=" + execution.source().offset());
        Promise<Void> promise = Promise.promise();
        Runnable settleSuccess = () -> {
            COMPLETED.incrementAndGet();
            if (execution.job().payload().path("loadMode").asText("-").equals("success")) {
                int loadCompleted = LOAD_COMPLETED.incrementAndGet();
                int loadTotal = execution.job().payload().path("loadTotal").asInt(0);
                String podName = System.getenv().getOrDefault("HOSTNAME", "unknown");
                if (loadCompleted % 100 == 0
                        || (loadTotal > 0 && loadTotal == loadCompleted)) {
                    LOG.info(() -> "Pod load progress pod=" + podName
                            + " loadCompleted=" + loadCompleted
                            + " peakActive=" + PEAK_ACTIVE.get());
                }
            }
            ACTIVE.decrementAndGet();
            promise.complete();
        };
        Runnable settleFailure = () -> {
            COMPLETED.incrementAndGet();
            ACTIVE.decrementAndGet();
            promise.fail(new IllegalStateException("DEMO_FAILURE"));
        };

        if (delayMillis > 0) {
            io.vertx.core.Context context = Vertx.currentContext();
            if (context == null) {
                settleSuccess.run();
            } else {
                context.owner().setTimer(delayMillis, ignored ->
                        context.runOnContext(ignoredContext -> {
                            if (fail) settleFailure.run();
                            else settleSuccess.run();
                        }));
            }
        } else {
            if (fail) {
                settleFailure.run();
            } else {
                settleSuccess.run();
            }
        }
        return promise.future();
    }

    private static boolean reportExternalResponse(
            String externalResponse, ExternalPressureReporter pressure) {
        RestPressureAdapter rest = new RestPressureAdapter(
                PRESSURE_REGISTRY, pressure, "demo-rest");
        DatabasePressureAdapter database = new DatabasePressureAdapter(
                PRESSURE_REGISTRY, pressure, "demo-db");
        return switch (externalResponse) {
            case "none" -> false;
            case "rest-503" -> rest.observeStatus(503);
            case "rest-timeout" -> rest.observeTimeout(RestPressureAdapter.Timeout.READ);
            case "database-timeout" ->
                    database.observeTimeout(DatabasePressureAdapter.Timeout.STATEMENT);
            default -> throw new IllegalArgumentException(
                    "Unsupported demo external response " + externalResponse);
        };
    }

    static Metrics metrics() {
        return new Metrics(ACTIVE.get(), PEAK_ACTIVE.get(),
                COMPLETED.get(), LOAD_COMPLETED.get());
    }

    static void resetMetrics() {
        ACTIVE.set(0);
        PEAK_ACTIVE.set(0);
        COMPLETED.set(0);
        LOAD_COMPLETED.set(0);
    }
}
