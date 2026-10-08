package com.example.agenticjobscheduler.messaging;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Set;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import static com.example.agenticjobscheduler.messaging.EnvelopeValidationException.Code.*;
import static org.junit.jupiter.api.Assertions.*;

class EnvelopeValidatorTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static final String JOB_ID = "e2be7c92-05f9-48d3-aeb2-4e695d857151";
    private static final String EXECUTION_ID = "5d0d8409-4276-4ec8-8fd0-941ed977ff00";
    private static final byte[] KEY = bytes(JOB_ID);
    private final EnvelopeValidator validator = new EnvelopeValidator(Set.of("demo.echo"));

    @Test
    void acceptsMainAndPreservesIdentityAndArbitraryObjectPayload() {
        ObjectNode input = main();
        input.withObject("payload").putArray("nested").add(1).addNull().add("世界");
        JobEnvelope result = validator.validateMain(KEY, bytes(input.toString()));
        assertEquals(UUID.fromString(JOB_ID), result.jobId());
        assertEquals(UUID.fromString(EXECUTION_ID), result.executionId());
        assertEquals(input.get("payload"), result.payload());
        assertEquals(3, result.maxAttempts());
        assertEquals(1, result.schemaVersion());
        assertEquals("demo.echo", result.jobType());
        result.payload().put("mutated", true);
        assertFalse(result.payload().has("mutated"));
        assertFalse(result.toString().contains("世界"));
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 100})
    void acceptsAttemptBoundaries(int attempts) {
        assertEquals(attempts, validator.validateMain(KEY,
                bytes(main().put("maxAttempts", attempts).toString())).maxAttempts());
    }

    @ParameterizedTest
    @ValueSource(strings = {"0", "101", "-1", "3.0", "3e0", "\"3\"", "true", "null",
            "2147483648", "99999999999999999999999999999"})
    void rejectsInvalidMaxAttemptsWithoutCoercion(String literal) throws Exception {
        ObjectNode input = main();
        input.set("maxAttempts", JSON.readTree(literal));
        rejectMain(input, INVALID_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"schemaVersion", "jobId", "executionId", "jobType", "payload"})
    void rejectsMissingAndNullRequiredFields(String field) {
        ObjectNode missing = main();
        missing.remove(field);
        rejectMain(missing, INVALID_FIELD);
        rejectMain(main().putNull(field), INVALID_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"scheduledAt", "notBefore", "attempt", "unknown", "job"})
    void rejectsUnknownOrSchedulingFields(String field) {
        rejectMain(main().put(field, "secret"), UNKNOWN_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"[]", "null", "1", "true", "\"object\""})
    void rejectsNonObjectPayload(String literal) throws Exception {
        ObjectNode input = main();
        input.set("payload", JSON.readTree(literal));
        rejectMain(input, INVALID_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"1-1-1-1-1", "not-a-uuid", "E2BE7C92-05F9-48D3-AEB2-4E695D857151", ""})
    void rejectsNonCanonicalIdentifiers(String value) {
        rejectMain(main().put("jobId", value), INVALID_FIELD);
        rejectMain(main().put("executionId", value), INVALID_FIELD);
    }

    @Test
    void rejectsMissingMismatchedAndNonCanonicalKeys() {
        for (byte[] key : new byte[][] {null, bytes(""), bytes(EXECUTION_ID), bytes(JOB_ID.toUpperCase())}) {
            assertEquals(INVALID_KEY, assertThrows(EnvelopeValidationException.class,
                    () -> validator.validateMain(key, bytes(main().toString()))).code());
        }
    }

    @Test
    void requiresKnownVersionAndRegisteredType() {
        rejectMain(main().put("schemaVersion", 2), UNSUPPORTED_VERSION);
        rejectMain(main().put("schemaVersion", "1"), INVALID_FIELD);
        rejectMain(main().put("jobType", "unregistered"), UNREGISTERED_JOB_TYPE);
        rejectMain(main().put("jobType", " "), INVALID_FIELD);
        rejectMain(main().put("jobType", "x".repeat(129)), INVALID_FIELD);
        String longest = "x".repeat(128);
        assertEquals(longest, new EnvelopeValidator(Set.of(longest)).validateMain(KEY,
                bytes(main().put("jobType", longest).toString())).jobType());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "{", "{secret", "{\"schemaVersion\":1,\"schemaVersion\":1}",
            "{\"payload\":{\"x\":1,\"x\":2}}", "{} {}", "{} garbage", "{\"x\":NaN}", "/*comment*/{}"})
    void rejectsMalformedOrAmbiguousJsonWithoutLeakingInput(String raw) {
        EnvelopeValidationException e = assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, bytes(raw)));
        assertTrue(Set.of(INVALID_JSON, INVALID_FIELD).contains(e.code()));
        assertEquals(e.code().name(), e.getMessage());
        assertNull(e.getCause());
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "[]", "42", "\"text\""})
    void rejectsNonObjectRoots(String raw) {
        assertEquals(INVALID_FIELD, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, bytes(raw))).code());
    }

    @Test
    void rejectsInvalidUtf8AndNullBytes() {
        assertEquals(INVALID_UTF8, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, new byte[] {(byte) 0xc3, 0x28})).code());
        assertEquals(INVALID_JSON, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, null)).code());
        assertEquals(INVALID_UTF8, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, "{}".getBytes(StandardCharsets.UTF_16))).code());
    }

    @Test
    void mainSizeLimitCountsUtf8BytesAndAcceptsExactBoundary() {
        byte[] input = padded(main().toString(), EnvelopeValidator.MAIN_MAX_BYTES);
        assertDoesNotThrow(() -> validator.validateMain(KEY, input));
        assertEquals(RECORD_TOO_LARGE, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, Arrays.copyOf(input, input.length + 1))).code());
        ObjectNode multibyte = main();
        multibyte.withObject("payload").put("text", "界".repeat(90_000));
        assertTrue(multibyte.toString().length() < EnvelopeValidator.MAIN_MAX_BYTES);
        rejectMain(multibyte, RECORD_TOO_LARGE);
    }

    @Test
    void boundsJsonNesting() {
        String deeplyNested = "{\"payload\":" + "[".repeat(101) + "0" + "]".repeat(101) + "}";
        assertEquals(INVALID_JSON, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, bytes(deeplyNested))).code());
    }

    @Test
    void validMainAtNestingBoundaryCanStillBeWrappedForRetry() {
        ObjectNode job = main().put("maxAttempts", 3);
        var array = job.withObject("payload").putArray("nested");
        for (int i = 1; i < 98; i++) {
            array = array.addArray();
        }
        array.add(1);
        assertDoesNotThrow(() -> validator.validateMain(KEY, bytes(job.toString())));
        ObjectNode retry = retry();
        retry.set("job", job);
        assertDoesNotThrow(() -> validator.validateRetry(KEY, bytes(retry.toString())));
    }

    @Test
    void acceptsFirstRetryAndPreservesOriginalJob() {
        RetryEnvelope result = validator.validateRetry(KEY, bytes(retry().toString()));
        assertEquals(2, result.attempt());
        assertEquals(3, result.job().maxAttempts());
        assertEquals(UUID.fromString(JOB_ID), result.job().jobId());
        assertEquals(UUID.fromString(EXECUTION_ID), result.job().executionId());
        assertEquals(new SourceRecord("jobs.main.v1", 0, 42), result.originalSource());
        assertEquals(result.originalSource(), result.failedSource());
        assertEquals("TRANSIENT_FAILURE", result.errorCode());
    }

    @Test
    void acceptsLaterRetryWithNewFailedSourceAndStableOriginalSource() {
        ObjectNode input = retry().put("attempt", 3);
        SourceRecord failed = new SourceRecord("jobs.retry.v1", 2, Long.MAX_VALUE);
        input.set("failedSource", source(failed));
        input.put("handoffId", EnvelopeValidator.handoffId(failed, 3));
        RetryEnvelope result = validator.validateRetry(KEY, bytes(input.toString()));
        assertEquals(3, result.attempt());
        assertEquals("jobs.main.v1", result.originalSource().topic());
        assertEquals(failed, result.failedSource());
    }

    @ParameterizedTest
    @ValueSource(strings = {"1", "0", "4", "null", "2.0", "\"2\"", "2147483648"})
    void rejectsInvalidRetryAttempts(String literal) throws Exception {
        ObjectNode input = retry();
        input.set("attempt", JSON.readTree(literal));
        rejectRetry(input, INVALID_FIELD);
    }

    @ParameterizedTest
    @ValueSource(strings = {"schemaVersion", "job", "attempt", "handoffId", "originalSource", "failedSource", "errorCode"})
    void requiresAllRetryFields(String field) {
        ObjectNode input = retry();
        input.remove(field);
        rejectRetry(input, INVALID_FIELD);
        rejectRetry(retry().putNull(field), INVALID_FIELD);
    }

    @Test
    void rejectsNestedJobsUnknownFieldsAndNonNormalizedJobs() {
        ObjectNode input = retry();
        input.set("job", retry());
        rejectRetry(input, UNKNOWN_FIELD);
        rejectRetry(retry().put("notBefore", "tomorrow"), UNKNOWN_FIELD);
        input = retry();
        ((ObjectNode) input.get("job")).remove("maxAttempts");
        rejectRetry(input, INVALID_FIELD);
        input = retry();
        ((ObjectNode) input.get("job")).put("maxAttempts", 1);
        rejectRetry(input, INVALID_FIELD);
        input = retry();
        ((ObjectNode) input.get("job")).put("attempt", 1);
        rejectRetry(input, UNKNOWN_FIELD);
    }

    @Test
    void validatesRetryKeyVersionAndSize() {
        ObjectNode input = retry();
        assertEquals(INVALID_KEY, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateRetry(bytes(EXECUTION_ID), bytes(input.toString()))).code());
        rejectRetry(retry().put("schemaVersion", 2), UNSUPPORTED_VERSION);
        byte[] exact = padded(input.toString(), EnvelopeValidator.RETRY_MAX_BYTES);
        assertDoesNotThrow(() -> validator.validateRetry(KEY, exact));
        assertEquals(RECORD_TOO_LARGE, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateRetry(KEY, Arrays.copyOf(exact, exact.length + 1))).code());
    }

    @Test
    void verifiesHandoffIdentityAndOriginalSourceForFirstRetry() {
        rejectRetry(retry().put("handoffId", "forged"), INVALID_RETRY_HANDOFF);
        ObjectNode input = retry();
        ((ObjectNode) input.get("originalSource")).put("offset", 43);
        rejectRetry(input, INVALID_RETRY_HANDOFF);
    }

    @Test
    void handoffIdMatchesIndependentWireVector() {
        // SHA-256 of UTF-8 "jobs.main.v1\n0\n42\n2", no final newline.
        String expected = "92a0e16a8b0cc0557cc22b7a6c6ae4b6687e2d12f8b65bb2bd9db2b347b1d5e3";
        assertEquals(expected, EnvelopeValidator.handoffId(new SourceRecord("jobs.main.v1", 0, 42), 2));
        assertEquals(expected, validator.validateRetry(KEY,
                bytes(retry().put("handoffId", expected).toString())).handoffId());
    }

    @ParameterizedTest
    @ValueSource(strings = {"null", "\"42\"", "42.0", "-1", "9223372036854775808"})
    void rejectsInvalidSourceOffsets(String literal) throws Exception {
        ObjectNode input = retry();
        ((ObjectNode) input.get("failedSource")).set("offset", JSON.readTree(literal));
        rejectRetry(input, INVALID_FIELD);
    }

    @Test
    void validatesSourceCoordinatesAndSanitizedErrorCode() {
        ObjectNode input = retry();
        ((ObjectNode) input.get("failedSource")).put("partition", -1);
        rejectRetry(input, INVALID_FIELD);
        input = retry();
        ((ObjectNode) input.get("failedSource")).put("offset", -1);
        rejectRetry(input, INVALID_FIELD);
        input = retry();
        ((ObjectNode) input.get("failedSource")).put("topic", "bad\ntopic");
        rejectRetry(input, INVALID_FIELD);
        input = retry();
        ((ObjectNode) input.get("failedSource")).put("extra", true);
        rejectRetry(input, UNKNOWN_FIELD);
        rejectRetry(retry().put("errorCode", "stack trace / secret"), INVALID_FIELD);
        rejectRetry(retry().put("errorCode", "A".repeat(65)), INVALID_FIELD);
    }

    private void rejectMain(ObjectNode input, EnvelopeValidationException.Code code) {
        assertEquals(code, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateMain(KEY, bytes(input.toString()))).code());
    }

    private void rejectRetry(ObjectNode input, EnvelopeValidationException.Code code) {
        assertEquals(code, assertThrows(EnvelopeValidationException.class,
                () -> validator.validateRetry(KEY, bytes(input.toString()))).code());
    }

    private static ObjectNode main() {
        ObjectNode node = JSON.createObjectNode();
        node.put("schemaVersion", 1).put("jobId", JOB_ID).put("executionId", EXECUTION_ID)
                .put("jobType", "demo.echo");
        node.putObject("payload");
        return node;
    }

    private static ObjectNode retry() {
        SourceRecord original = new SourceRecord("jobs.main.v1", 0, 42);
        ObjectNode node = JSON.createObjectNode();
        node.put("schemaVersion", 1);
        node.set("job", main().put("maxAttempts", 3));
        node.put("attempt", 2).put("handoffId", EnvelopeValidator.handoffId(original, 2));
        node.set("originalSource", source(original));
        node.set("failedSource", source(original));
        node.put("errorCode", "TRANSIENT_FAILURE");
        return node;
    }

    private static ObjectNode source(SourceRecord source) {
        return JSON.createObjectNode().put("topic", source.topic())
                .put("partition", source.partition()).put("offset", source.offset());
    }

    private static byte[] padded(String json, int length) {
        byte[] encoded = bytes(json);
        byte[] padded = Arrays.copyOf(encoded, length);
        Arrays.fill(padded, encoded.length, length, (byte) ' ');
        return padded;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
