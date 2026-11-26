package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkerConfigTest {
    @Test
    void retainedWindowDefaultsAndCanBeConfigured() {
        WorkerConfig defaults = WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main"));
        assertEquals(1_000, defaults.maxRetainedRecords());

        WorkerConfig demo = WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main",
                "WORKER_MAX_RETAINED_RECORDS", "250"));
        assertEquals(250, demo.maxRetainedRecords());
    }
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

    @Test
    void adaptivePolicyAndPressureLabelsCanBeConfigured() {
        WorkerConfig config = WorkerConfig.fromEnvironment(Map.ofEntries(
                Map.entry("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
                Map.entry("WORKER_ROLE", "main"),
                Map.entry("WORKER_CONCURRENCY", "4"),
                Map.entry("WORKER_PRESSURE_DEPENDENCIES", "catalog, payments"),
                Map.entry("WORKER_ADAPTIVE_MINIMUM", "2"),
                Map.entry("WORKER_ADAPTIVE_MAXIMUM", "8"),
                Map.entry("WORKER_ADAPTIVE_WINDOW_MILLIS", "2500"),
                Map.entry("WORKER_ADAPTIVE_TARGET_P95_MILLIS", "500"),
                Map.entry("WORKER_ADAPTIVE_SAMPLE_FLOOR", "5"),
                Map.entry("WORKER_ADAPTIVE_FAILURE_THRESHOLD", "0.20"),
                Map.entry("WORKER_ADAPTIVE_RECOVERY_WINDOWS", "3"),
                Map.entry("WORKER_ADAPTIVE_SLOW_LATENCY_ENABLED", "false"),
                Map.entry("WORKER_ADAPTIVE_EXTERNAL_PRESSURE_ENABLED", "false")));

        assertEquals(List.of("catalog", "payments"), config.pressureDependencyNames());
        assertEquals(2, config.adaptiveLimiterPolicy().minimum());
        assertEquals(8, config.adaptiveLimiterPolicy().maximum());
        assertEquals(2_500, config.adaptiveLimiterPolicy().evaluationPeriodMillis());
        assertEquals(3, config.adaptiveLimiterPolicy().recoveryWindows());
        assertEquals(false, config.adaptiveLimiterPolicy().slowLatencyEnabled());
        assertEquals(false, config.adaptiveLimiterPolicy().externalPressureEnabled());
    }

    @Test
    void adaptiveConfigurationRejectsUnsafeValuesAndLabels() {
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main",
                "WORKER_CONCURRENCY", "1",
                "WORKER_ADAPTIVE_MINIMUM", "2")));
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main",
                "WORKER_PRESSURE_DEPENDENCIES", "https://catalog.example")));
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main",
                "WORKER_ADAPTIVE_FAILURE_THRESHOLD", "NaN")));
        assertThrows(IllegalArgumentException.class, () -> WorkerConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "WORKER_ROLE", "main",
                "WORKER_ADAPTIVE_EXTERNAL_PRESSURE_ENABLED", "sometimes")));
    }
}
