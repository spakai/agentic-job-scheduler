package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkerConfigTest {
    @Test
    void retryStartRateDefaultsAndCanBeConfigured() {
        WorkerConfig defaults = WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "retry"));
        assertEquals(1, defaults.retryStartsPerSecond());

        WorkerConfig configured = WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "retry",
                "WORKER_RETRY_STARTS_PER_SECOND", "4"));
        assertEquals(4, configured.retryStartsPerSecond());
    }

    @Test
    void retryStartRateMustBePositive() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "retry",
                "WORKER_RETRY_STARTS_PER_SECOND", "0")));
        assertThrows(IllegalArgumentException.class, () -> new WorkerConfig(
                "localhost:9092", WorkerRole.RETRY, 2, 3, 0));
    }
}
