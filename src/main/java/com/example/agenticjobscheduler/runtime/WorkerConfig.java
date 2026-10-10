package com.example.agenticjobscheduler.runtime;

import java.util.Map;
import java.util.Objects;

public record WorkerConfig(String bootstrapServers, WorkerRole role, int concurrency,
                           int topicPartitions) {
    public WorkerConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(role, "role");
        if (bootstrapServers.isBlank() || concurrency < 1 || topicPartitions < 1) {
            throw new IllegalArgumentException("Invalid worker configuration");
        }
    }

    public static WorkerConfig fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String bootstrap = required(environment, "KAFKA_BOOTSTRAP_SERVERS");
        WorkerRole role = WorkerRole.parse(required(environment, "WORKER_ROLE"));
        int concurrency = integer(environment, "WORKER_CONCURRENCY",
                role == WorkerRole.MAIN ? 16 : 2);
        int partitions = integer(environment, "KAFKA_TOPIC_PARTITIONS", 12);
        if (role == WorkerRole.MAIN && concurrency > 128) {
            throw new IllegalArgumentException("Main worker concurrency cannot exceed 128");
        }
        return new WorkerConfig(bootstrap, role, concurrency, partitions);
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
}
