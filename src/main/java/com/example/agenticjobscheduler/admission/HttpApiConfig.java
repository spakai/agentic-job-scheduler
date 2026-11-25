package com.example.agenticjobscheduler.admission;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public record HttpApiConfig(
        String bootstrapServers,
        int port,
        int maxBodyBytes,
        int maxInFlight,
        long acknowledgmentDeadlineMillis,
        long deliveryTimeoutMillis,
        long gracefulShutdownMillis,
        int topicPartitions,
        Set<String> registeredJobTypes) {

    public HttpApiConfig {
        Objects.requireNonNull(bootstrapServers, "bootstrapServers");
        Objects.requireNonNull(registeredJobTypes, "registeredJobTypes");
        registeredJobTypes = Set.copyOf(registeredJobTypes);
        if (bootstrapServers.isBlank() || port < 1 || port > 65_535
                || maxBodyBytes < 1 || maxBodyBytes > EnvelopeValidator.MAIN_MAX_BYTES
                || maxInFlight < 1 || acknowledgmentDeadlineMillis < 1
                || deliveryTimeoutMillis < 1_000
                || deliveryTimeoutMillis >= acknowledgmentDeadlineMillis
                || gracefulShutdownMillis < 1 || topicPartitions < 1
                || registeredJobTypes.isEmpty()
                || registeredJobTypes.stream().anyMatch(type ->
                type.isBlank() || type.codePointCount(0, type.length()) > 128)) {
            throw new IllegalArgumentException("Invalid HTTP API configuration");
        }
    }

    public static HttpApiConfig fromEnvironment(Map<String, String> environment) {
        Objects.requireNonNull(environment, "environment");
        String bootstrap = required(environment, "KAFKA_BOOTSTRAP_SERVERS");
        Set<String> jobTypes = Arrays.stream(required(environment, "REGISTERED_JOB_TYPES").split(","))
                .map(String::trim)
                .filter(value -> !value.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
        return new HttpApiConfig(
                bootstrap,
                integer(environment, "HTTP_PORT", 8080),
                integer(environment, "HTTP_MAX_BODY_BYTES", EnvelopeValidator.MAIN_MAX_BYTES),
                integer(environment, "HTTP_MAX_IN_FLIGHT", 1_000),
                longInteger(environment, "HTTP_ACK_DEADLINE_MILLIS", 10_000),
                longInteger(environment, "KAFKA_DELIVERY_TIMEOUT_MILLIS", 9_000),
                longInteger(environment, "HTTP_GRACEFUL_SHUTDOWN_MILLIS", 30_000),
                integer(environment, "KAFKA_TOPIC_PARTITIONS", 12),
                jobTypes);
    }

    private static String required(Map<String, String> environment, String name) {
        String value = environment.get(name);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException(name + " is required");
        }
        return value;
    }

    private static int integer(Map<String, String> environment, String name, int defaultValue) {
        long value = longInteger(environment, name, defaultValue);
        if (value > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " is too large");
        }
        return (int) value;
    }

    private static long longInteger(Map<String, String> environment, String name,
                                    long defaultValue) {
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
