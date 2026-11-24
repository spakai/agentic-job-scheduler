package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;

class AdmissionRequestDecoderTest {
    @Test
    void acceptsMinimumRequestAndDefaultsAttempts() {
        AdmissionRequest request = decode("""
                {"jobType":"demo.echo","payload":{}}
                """);

        assertTrue(request.jobId().isEmpty());
        assertTrue(request.executionId().isEmpty());
        assertEquals(3, request.remainingAttempts());
    }

    @Test
    void acceptsCanonicalPairedIds() {
        AdmissionRequest request = decode("""
                {"jobId":"e2be7c92-05f9-48d3-aeb2-4e695d857151",
                 "executionId":"5d0d8409-4276-4ec8-8fd0-941ed977ff00",
                 "jobType":"demo.echo","payload":{},"remainingAttempts":4}
                """);

        assertEquals("e2be7c92-05f9-48d3-aeb2-4e695d857151",
                request.jobId().orElseThrow().toString());
        assertEquals(4, request.remainingAttempts());
    }

    @Test
    void rejectsOnlyOneIdentityAndUnknownFields() {
        AdmissionException partial = assertThrows(AdmissionException.class, () -> decode("""
                {"jobId":"e2be7c92-05f9-48d3-aeb2-4e695d857151",
                 "jobType":"demo.echo","payload":{}}
                """));
        assertEquals("VALIDATION_ERROR", partial.code());

        AdmissionException unknown = assertThrows(AdmissionException.class, () -> decode("""
                {"schemaVersion":1,"jobType":"demo.echo","payload":{}}
                """));
        assertEquals("VALIDATION_ERROR", unknown.code());
    }

    @Test
    void rejectsDuplicatePropertiesAndMalformedJson() {
        AdmissionException duplicate = assertThrows(AdmissionException.class, () -> decode("""
                {"jobType":"demo.echo","jobType":"demo.other","payload":{}}
                """));
        assertEquals("MALFORMED_JSON", duplicate.code());

        AdmissionException malformed = assertThrows(AdmissionException.class, () ->
                decode("{\"jobType\":"));
        assertEquals("MALFORMED_JSON", malformed.code());
    }

    private static AdmissionRequest decode(String value) {
        return AdmissionRequestDecoder.decode(value.getBytes(StandardCharsets.UTF_8));
    }
}
