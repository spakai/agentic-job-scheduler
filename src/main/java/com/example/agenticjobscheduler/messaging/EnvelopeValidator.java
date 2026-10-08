package com.example.agenticjobscheduler.messaging;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.core.StreamReadFeature;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

import static com.example.agenticjobscheduler.messaging.EnvelopeValidationException.Code.*;

/** Strict wire validation only: no consumption, partition routing, execution, or DLQ I/O. */
public final class EnvelopeValidator {
    public static final int MAIN_MAX_BYTES = 256 * 1024;
    public static final int RETRY_MAX_BYTES = 512 * 1024;
    private static final Set<String> JOB_FIELDS = Set.of(
            "schemaVersion", "jobId", "executionId", "jobType", "payload", "maxAttempts");
    private static final Set<String> RETRY_FIELDS = Set.of(
            "schemaVersion", "job", "attempt", "handoffId", "originalSource", "failedSource", "errorCode");
    private static final Pattern UUID_TEXT = Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static final Pattern TOPIC = Pattern.compile("[a-zA-Z0-9._-]{1,249}");
    private static final Pattern ERROR_CODE = Pattern.compile("[A-Z][A-Z0-9_]{0,63}");
    private static final ObjectMapper MAIN_MAPPER = mapper(100);
    private static final ObjectMapper RETRY_MAPPER = mapper(101);

    private static ObjectMapper mapper(int depth) {
        return new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNestingDepth(depth).maxNumberLength(1000)
                    .maxStringLength(RETRY_MAX_BYTES).build())
            .build()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    }

    private final Set<String> registeredJobTypes;

    public EnvelopeValidator(Set<String> registeredJobTypes) {
        this.registeredJobTypes = Set.copyOf(registeredJobTypes);
        if (this.registeredJobTypes.stream().anyMatch(t -> t.isBlank()
                || t.codePointCount(0, t.length()) > 128)) {
            throw new IllegalArgumentException("Invalid job type registry");
        }
    }

    public JobEnvelope validateMain(byte[] key, byte[] value) {
        JobEnvelope job = job(parse(value, MAIN_MAX_BYTES), false);
        validateKey(key, job.jobId());
        return job;
    }

    public RetryEnvelope validateRetry(byte[] key, byte[] value) {
        ObjectNode root = parse(value, RETRY_MAX_BYTES);
        fields(root, RETRY_FIELDS);
        version(root);
        JobEnvelope job = job(object(root.get("job")), true);
        validateKey(key, job.jobId());
        int attempt = integer(root, "attempt", 2, job.maxAttempts());
        SourceRecord original = source(object(root.get("originalSource")));
        SourceRecord failed = source(object(root.get("failedSource")));
        // The first handoff is from the original record, not a nested retry.
        if (attempt == 2 && !original.equals(failed)) {
            throw invalid(INVALID_RETRY_HANDOFF);
        }
        String handoffId = text(root, "handoffId");
        if (!handoffId.equals(handoffId(failed, attempt))) {
            throw invalid(INVALID_RETRY_HANDOFF);
        }
        String errorCode = text(root, "errorCode");
        if (!ERROR_CODE.matcher(errorCode).matches()) {
            throw invalid(INVALID_FIELD);
        }
        return new RetryEnvelope(1, job, attempt, handoffId, original, failed, errorCode);
    }

    /** Stable ID for re-sending one failed record's handoff, not an execution deduplication key. */
    public static String handoffId(SourceRecord failedSource, int nextAttempt) {
        String input = failedSource.topic() + "\n" + failedSource.partition() + "\n"
                + failedSource.offset() + "\n" + nextAttempt;
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(input.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    private JobEnvelope job(ObjectNode root, boolean normalized) {
        fields(root, JOB_FIELDS);
        version(root);
        UUID jobId = uuid(text(root, "jobId"));
        UUID executionId = uuid(text(root, "executionId"));
        String type = text(root, "jobType");
        if (type.isBlank() || type.codePointCount(0, type.length()) > 128) {
            throw invalid(INVALID_FIELD);
        }
        if (!registeredJobTypes.contains(type)) {
            throw invalid(UNREGISTERED_JOB_TYPE);
        }
        ObjectNode payload = object(root.get("payload"));
        int attempts = !normalized && !root.has("maxAttempts")
                ? 3 : integer(root, "maxAttempts", 1, 100);
        return new JobEnvelope(1, jobId, executionId, type, payload, attempts);
    }

    private SourceRecord source(ObjectNode root) {
        fields(root, Set.of("topic", "partition", "offset"));
        String topic = text(root, "topic");
        if (!TOPIC.matcher(topic).matches() || topic.equals(".") || topic.equals("..")) {
            throw invalid(INVALID_FIELD);
        }
        int partition = integer(root, "partition", 0, Integer.MAX_VALUE);
        JsonNode offset = root.get("offset");
        if (offset == null || !offset.isIntegralNumber() || !offset.canConvertToLong()
                || offset.longValue() < 0) {
            throw invalid(INVALID_FIELD);
        }
        return new SourceRecord(topic, partition, offset.longValue());
    }

    private static ObjectNode parse(byte[] value, int limit) {
        if (value == null) {
            throw invalid(INVALID_JSON);
        }
        if (value.length > limit) {
            throw invalid(RECORD_TOO_LARGE);
        }
        String json = utf8(value);
        try {
            ObjectMapper mapper = limit == MAIN_MAX_BYTES ? MAIN_MAPPER : RETRY_MAPPER;
            return object(mapper.readTree(json));
        } catch (JsonProcessingException e) {
            // Do not attach the parser exception: it may contain raw payload data.
            throw invalid(INVALID_JSON);
        }
    }

    private static String utf8(byte[] value) {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(value)).toString();
        } catch (CharacterCodingException e) {
            throw invalid(INVALID_UTF8);
        }
    }

    private static void validateKey(byte[] key, UUID jobId) {
        if (key == null || key.length != 36 || !utf8(key).equals(jobId.toString())) {
            throw invalid(INVALID_KEY);
        }
    }

    private static UUID uuid(String value) {
        if (!UUID_TEXT.matcher(value).matches()) {
            throw invalid(INVALID_FIELD);
        }
        return UUID.fromString(value);
    }

    private static ObjectNode object(JsonNode node) {
        if (!(node instanceof ObjectNode object)) {
            throw invalid(INVALID_FIELD);
        }
        return object;
    }

    private static void fields(ObjectNode object, Set<String> allowed) {
        object.fieldNames().forEachRemaining(name -> {
            if (!allowed.contains(name)) {
                throw invalid(UNKNOWN_FIELD);
            }
        });
    }

    private static String text(ObjectNode root, String field) {
        JsonNode node = root.get(field);
        if (node == null || !node.isTextual()) {
            throw invalid(INVALID_FIELD);
        }
        return node.textValue();
    }

    private static int integer(ObjectNode root, String field, int min, int max) {
        JsonNode node = root.get(field);
        if (node == null || !node.isIntegralNumber() || !node.canConvertToInt()
                || node.intValue() < min || node.intValue() > max) {
            throw invalid(INVALID_FIELD);
        }
        return node.intValue();
    }

    private static void version(ObjectNode root) {
        if (integer(root, "schemaVersion", Integer.MIN_VALUE, Integer.MAX_VALUE) != 1) {
            throw invalid(UNSUPPORTED_VERSION);
        }
    }

    private static EnvelopeValidationException invalid(EnvelopeValidationException.Code code) {
        return new EnvelopeValidationException(code);
    }
}
