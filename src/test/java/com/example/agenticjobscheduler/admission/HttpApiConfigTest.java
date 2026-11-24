package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Map;
import org.junit.jupiter.api.Test;

class HttpApiConfigTest {
    @Test
    void defaultsAndOverridesAreValidated() {
        HttpApiConfig config = HttpApiConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "REGISTERED_JOB_TYPES", "demo.echo, demo.notify"));

        assertEquals(8080, config.port());
        assertEquals(1_000, config.maxInFlight());
        assertEquals(256 * 1024, config.maxBodyBytes());
        assertEquals(2, config.registeredJobTypes().size());
    }

    @Test
    void rejectInvalidTimeoutRelationshipAndOversizedBody() {
        assertThrows(IllegalArgumentException.class, () -> HttpApiConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "REGISTERED_JOB_TYPES", "demo.echo",
                "HTTP_ACK_DEADLINE_MILLIS", "9000",
                "KAFKA_DELIVERY_TIMEOUT_MILLIS", "9000")));
        assertThrows(IllegalArgumentException.class, () -> HttpApiConfig.fromEnvironment(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", "localhost:9092",
                "REGISTERED_JOB_TYPES", "demo.echo",
                "HTTP_MAX_BODY_BYTES", "262145")));
    }
}
