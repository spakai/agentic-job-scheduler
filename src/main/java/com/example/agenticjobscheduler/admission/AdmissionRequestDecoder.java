package com.example.agenticjobscheduler.admission;

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
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Pattern;

public final class AdmissionRequestDecoder {
    private static final Set<String> FIELDS = Set.of(
            "jobId", "executionId", "jobType", "payload", "remainingAttempts");
    private static final Pattern UUID_TEXT = Pattern.compile(
            "[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}");
    private static final ObjectMapper JSON = new ObjectMapper(JsonFactory.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .streamReadConstraints(StreamReadConstraints.builder()
                    .maxNestingDepth(100)
                    .maxNumberLength(1_000)
                    .maxStringLength(Integer.MAX_VALUE)
                    .build())
            .build()).enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);

    private AdmissionRequestDecoder() { }

    public static AdmissionRequest decode(byte[] raw) {
        ObjectNode root;
        try {
            String text = StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(raw)).toString();
            JsonNode parsed = JSON.readTree(text);
            if (!(parsed instanceof ObjectNode object)) {
                throw AdmissionException.validation("Request body must be a JSON object");
            }
            root = object;
        } catch (CharacterCodingException failure) {
            throw AdmissionException.validation("Request body must be valid UTF-8");
        } catch (JsonProcessingException failure) {
            throw AdmissionException.malformedJson();
        }

        root.fieldNames().forEachRemaining(name -> {
            if (!FIELDS.contains(name)) {
                throw AdmissionException.validation("Unknown field " + name);
            }
        });

        Optional<UUID> jobId = optionalUuid(root, "jobId");
        Optional<UUID> executionId = optionalUuid(root, "executionId");
        if (jobId.isPresent() != executionId.isPresent()) {
            throw AdmissionException.validation("jobId and executionId must be supplied together");
        }
        String jobType = text(root, "jobType");
        if (jobType.isBlank() || jobType.codePointCount(0, jobType.length()) > 128) {
            throw AdmissionException.validation("jobType must contain 1 to 128 characters");
        }
        JsonNode payload = root.get("payload");
        if (!(payload instanceof ObjectNode objectPayload)) {
            throw AdmissionException.validation("payload must be a JSON object");
        }
        int remainingAttempts = 3;
        if (root.has("remainingAttempts")) {
            JsonNode attempts = root.get("remainingAttempts");
            if (!attempts.isIntegralNumber() || !attempts.canConvertToInt()
                    || attempts.intValue() < 1 || attempts.intValue() > 100) {
                throw AdmissionException.validation(
                        "remainingAttempts must be an integer from 1 to 100");
            }
            remainingAttempts = attempts.intValue();
        }
        return new AdmissionRequest(jobId, executionId, jobType, objectPayload, remainingAttempts);
    }

    private static Optional<UUID> optionalUuid(ObjectNode root, String name) {
        if (!root.has(name)) {
            return Optional.empty();
        }
        String value = text(root, name);
        if (!UUID_TEXT.matcher(value).matches()) {
            throw AdmissionException.validation(name + " must be a canonical lowercase UUID");
        }
        return Optional.of(UUID.fromString(value));
    }

    private static String text(ObjectNode root, String name) {
        JsonNode value = root.get(name);
        if (value == null || !value.isTextual()) {
            throw AdmissionException.validation(name + " is required and must be a string");
        }
        return value.textValue();
    }
}
