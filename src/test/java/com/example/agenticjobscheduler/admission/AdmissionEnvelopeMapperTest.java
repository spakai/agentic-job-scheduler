package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import java.nio.charset.StandardCharsets;
import java.util.Set;
import org.junit.jupiter.api.Test;

class AdmissionEnvelopeMapperTest {
    private final EnvelopeValidator validator = new EnvelopeValidator(Set.of("demo.echo"));
    private final AdmissionEnvelopeMapper mapper = new AdmissionEnvelopeMapper(validator);

    @Test
    void generatesAValidCanonicalEnvelope() {
        AdmissionEnvelope first = mapper.map(AdmissionRequestDecoder.decode("""
                {"jobType":"demo.echo","payload":{"message":"hello"}}
                """.getBytes(StandardCharsets.UTF_8)));
        AdmissionEnvelope second = mapper.map(AdmissionRequestDecoder.decode("""
                {"jobType":"demo.echo","payload":{"message":"hello"}}
                """.getBytes(StandardCharsets.UTF_8)));

        assertNotEquals(first.jobId(), second.jobId());
        assertNotEquals(first.executionId(), second.executionId());
        assertEquals(3, first.remainingAttempts());
        assertArrayEquals(first.jobId().toString().getBytes(StandardCharsets.UTF_8), first.key());
        assertEquals(first.jobId(), validator.validateMain(first.key(), first.value()).jobId());
    }

    @Test
    void preservesSuppliedIdentityAndRejectsUnregisteredType() {
        AdmissionEnvelope envelope = mapper.map(AdmissionRequestDecoder.decode("""
                {"jobId":"e2be7c92-05f9-48d3-aeb2-4e695d857151",
                 "executionId":"5d0d8409-4276-4ec8-8fd0-941ed977ff00",
                 "jobType":"demo.echo","payload":{},"remainingAttempts":2}
                """.getBytes(StandardCharsets.UTF_8)));
        assertEquals("e2be7c92-05f9-48d3-aeb2-4e695d857151", envelope.jobId().toString());
        assertEquals(2, envelope.remainingAttempts());

        AdmissionException failure = assertThrows(AdmissionException.class,
                () -> mapper.map(AdmissionRequestDecoder.decode("""
                        {"jobType":"demo.unknown","payload":{}}
                        """.getBytes(StandardCharsets.UTF_8))));
        assertEquals("VALIDATION_ERROR", failure.code());
    }
}
