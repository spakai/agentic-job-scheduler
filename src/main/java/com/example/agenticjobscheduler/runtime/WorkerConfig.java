package com.example.agenticjobscheduler.runtime;

import java.util.Map;
import java.util.Objects;

public record WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency,
                           int topicPartitions, int retryStartsPerSecond,
                           int maxRetainedRecords, long maxRetainedBytes) {
    public static final int DEFAULT_MAX_RETAINED_RECORDS = 1_000;
    public static final long DEFAULT_MAX_RETAINED_BYTES = 16L * 1024 * 1024;

    public WorkerConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(role, "role");
        if (bootstrapServers.isBlank() || concurrency < 1 || topicPartitions < 1
                || retryStartsPerSecond < 1 || maxRetainedRecords < 2
                || maxRetainedBytes < 2) {
            throw new IllegalArgumentException("Invalid worker configuration");
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
        if (role == WorkerRole.MAIN && concurrency > 128) {
            throw new IllegalArgumentException("Main worker concurrency cannot exceed 128");
        }
        return new WorkerConfig(bootstrap, role, concurrency, partitions, retryStartsPerSecond,
                maxRetainedRecords, maxRetainedBytes);
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
}
