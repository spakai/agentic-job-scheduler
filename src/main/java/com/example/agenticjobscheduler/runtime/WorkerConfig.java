package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.AdaptiveLimiter;
import com.example.agenticjobscheduler.execution.ExternalPressureRegistry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public record WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency,
                           int topicPartitions, int retryStartsPerSecond,
                           int maxRetainedRecords, long maxRetainedBytes,
                           List<String> pressureDependencyNames,
                           AdaptiveLimiter.Policy adaptiveLimiterPolicy) {
    public static final int DEFAULT_MAX_RETAINED_RECORDS = 1_000;
    public static final long DEFAULT_MAX_RETAINED_BYTES = 16L * 1024 * 1024;

    public WorkerConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(role, "role");
        Objects.requireNonNull(pressureDependencyNames, "pressureDependencyNames");
        Objects.requireNonNull(adaptiveLimiterPolicy, "adaptiveLimiterPolicy");
        if (bootstrapServers.isBlank() || concurrency < 1 || topicPartitions < 1
                || retryStartsPerSecond < 1 || maxRetainedRecords < 2
                || maxRetainedBytes < 2) {
            throw new IllegalArgumentException("Invalid worker configuration");
        }
        pressureDependencyNames = List.copyOf(pressureDependencyNames);
        new ExternalPressureRegistry(pressureDependencyNames);
        if (role == WorkerRole.MAIN
                && (concurrency < adaptiveLimiterPolicy.minimum()
                || concurrency > adaptiveLimiterPolicy.maximum())) {
            throw new IllegalArgumentException(
                    "Main worker concurrency is outside adaptive limiter bounds");
        }
    }

    public WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency, int topicPartitions) {
        this(bootstrapServers, role, concurrency, topicPartitions, 1,
                DEFAULT_MAX_RETAINED_RECORDS, DEFAULT_MAX_RETAINED_BYTES);
    }

    public WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency,
                        int topicPartitions, int retryStartsPerSecond) {
        this(bootstrapServers, role, concurrency, topicPartitions, retryStartsPerSecond,
                DEFAULT_MAX_RETAINED_RECORDS, DEFAULT_MAX_RETAINED_BYTES);
    }

    public WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency,
                        int topicPartitions, int retryStartsPerSecond,
                        int maxRetainedRecords, long maxRetainedBytes) {
        this(bootstrapServers, role, concurrency, topicPartitions, retryStartsPerSecond,
                maxRetainedRecords, maxRetainedBytes, List.of(), AdaptiveLimiter.Policy.DEFAULT);
    }

    public static WorkerConfig fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String bootstrap = required(environment, "KAFKA_BOOTSTRAP_SERVERS");
        WorkerRole role = WorkerRole.parse(required(environment, "WORKER_ROLE"));
        int concurrency = integer(environment, "WORKER_CONCURRENCY",
                role == WorkerRole.MAIN ? 16 : 2);
        int partitions = integer(environment, "KAFKA_TOPIC_PARTITIONS", 12);
        int retryStartsPerSecond = integer(environment, "WORKER_RETRY_STARTS_PER_SECOND", 1);
        int maxRetainedRecords = integer(environment, "WORKER_MAX_RETAINED_RECORDS",
                DEFAULT_MAX_RETAINED_RECORDS);
        long maxRetainedBytes = longInteger(environment, "WORKER_MAX_RETAINED_BYTES",
                DEFAULT_MAX_RETAINED_BYTES);
        List<String> pressureDependencyNames = pressureDependencies(environment);
        AdaptiveLimiter.Policy policy = new AdaptiveLimiter.Policy(
                integer(environment, "WORKER_ADAPTIVE_MINIMUM", 1),
                integer(environment, "WORKER_ADAPTIVE_MAXIMUM", 128),
                longInteger(environment, "WORKER_ADAPTIVE_WINDOW_MILLIS", 5_000),
                longInteger(environment, "WORKER_ADAPTIVE_TARGET_P95_MILLIS", 1_000),
                longInteger(environment, "WORKER_ADAPTIVE_SAMPLE_FLOOR", 20),
                decimal(environment, "WORKER_ADAPTIVE_FAILURE_THRESHOLD", 0.10),
                integer(environment, "WORKER_ADAPTIVE_RECOVERY_WINDOWS", 2),
                booleanValue(environment, "WORKER_ADAPTIVE_SLOW_LATENCY_ENABLED", true),
                booleanValue(environment, "WORKER_ADAPTIVE_EXTERNAL_PRESSURE_ENABLED", true));
        return new WorkerConfig(bootstrap, role, concurrency, partitions, retryStartsPerSecond,
                maxRetainedRecords, maxRetainedBytes, pressureDependencyNames, policy);
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static int integer(Map<String, String> environment, String name, int defaultValue) {
        String value = environment.get(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            int parsed = Integer.parseInt(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be a positive integer", failure);
        }
    }

    private static long longInteger(Map<String, String> environment, String name, long defaultValue) {
        String value = environment.get(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            long parsed = Long.parseLong(value);
            if (parsed < 1) {
                throw new IllegalArgumentException(name + " must be positive");
            }
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be a positive integer", failure);
        }
    }

    private static double decimal(
            Map<String, String> environment, String name, double defaultValue) {
        String value = environment.get(name);
        if (value == null) {
            return defaultValue;
        }
        try {
            double parsed = Double.parseDouble(value);
            if (!Double.isFinite(parsed) || parsed < 0.0 || parsed > 1.0) {
                throw new IllegalArgumentException(name + " must be between 0 and 1");
            }
            return parsed;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException(name + " must be between 0 and 1", failure);
        }
    }

    private static List<String> pressureDependencies(Map<String, String> environment) {
        String value = environment.get("WORKER_PRESSURE_DEPENDENCIES");
        if (value == null || value.isBlank()) {
            return List.of();
        }
        List<String> names = Arrays.stream(value.split(",", -1))
                .map(String::trim)
                .toList();
        if (names.stream().anyMatch(String::isBlank)) {
            throw new IllegalArgumentException(
                    "WORKER_PRESSURE_DEPENDENCIES must contain only nonblank labels");
        }
        return names;
    }

    private static boolean booleanValue(
            Map<String, String> environment, String name, boolean defaultValue) {
        String value = environment.get(name);
        if (value == null) {
            return defaultValue;
        }
        return switch (value.toLowerCase(java.util.Locale.ROOT)) {
            case "true" -> true;
            case "false" -> false;
            default -> throw new IllegalArgumentException(name + " must be true or false");
        };
    }
}
