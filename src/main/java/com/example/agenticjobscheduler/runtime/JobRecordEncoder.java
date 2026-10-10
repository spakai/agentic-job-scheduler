package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.HexFormat;
import java.util.UUID;

/** Encodes bounded, sanitized retry and dead-letter records. */
public final class JobRecordEncoder {
    private static final int INVALID_SAMPLE_BYTES = 256;
    private static final ObjectMapper JSON = new ObjectMapper();

    private JobRecordEncoder() { }

    public static byte[] main(JobEnvelope job) {
        return bytes(jobNode(job));
    }

    public static byte[] key(UUID jobId) {
        return jobId.toString().getBytes(StandardCharsets.UTF_8);
    }

    public static byte[] retry(JobEnvelope job, int nextAttempt, SourceRecord source,
                               SourceRecord originalSource, String errorCode) {
        ObjectNode root = JSON.createObjectNode();
        root.put("schemaVersion", 1);
        root.set("job", jobNode(job));
        root.put("attempt", nextAttempt);
        root.put("handoffId", EnvelopeValidator.handoffId(source, nextAttempt));
        root.set("originalSource", sourceNode(originalSource));
        root.set("failedSource", sourceNode(source));
        root.put("errorCode", errorCode);
        return bytes(root);
    }

    public static byte[] deadLetter(JobEnvelope job, int attempt, SourceRecord source,
                                    SourceRecord originalSource, String errorCode) {
        ObjectNode root = deadLetterBase(source, attempt, errorCode);
        root.set("job", jobNode(job));
        root.put("jobId", job.jobId().toString());
        root.put("executionId", job.executionId().toString());
        root.set("originalSource", sourceNode(originalSource));
        return bytes(root);
    }

    public static byte[] invalidDeadLetter(SourceRecord source, byte[] key, byte[] value,
                                           String errorCode) {
        ObjectNode root = deadLetterBase(source, 0, errorCode);
        root.put("inputSha256", sha256(value));
        root.put("keySha256", sha256(key));
        root.put("inputSampleBase64", java.util.Base64.getEncoder()
                .encodeToString(sample(value)));
        return bytes(root);
    }

    private static ObjectNode deadLetterBase(SourceRecord source, int attempt, String errorCode) {
        ObjectNode root = JSON.createObjectNode();
        root.put("schemaVersion", 1);
        root.put("handoffId", deadLetterId(source));
        root.put("attempt", attempt);
        root.put("errorCode", boundedCode(errorCode));
        root.put("reason", boundedCode(errorCode));
        root.put("timestamp", Instant.now().toString());
        root.set("failedSource", sourceNode(source));
        return root;
    }

    private static ObjectNode jobNode(JobEnvelope job) {
        ObjectNode node = JSON.createObjectNode();
        node.put("schemaVersion", job.schemaVersion());
        node.put("jobId", job.jobId().toString());
        node.put("executionId", job.executionId().toString());
        node.put("jobType", job.jobType());
        node.set("payload", job.payload());
        node.put("maxAttempts", job.maxAttempts());
        return node;
    }

    private static ObjectNode sourceNode(SourceRecord source) {
        ObjectNode node = JSON.createObjectNode();
        node.put("topic", source.topic());
        node.put("partition", source.partition());
        node.put("offset", source.offset());
        return node;
    }

    private static byte[] bytes(ObjectNode node) {
        try {
            return JSON.writeValueAsBytes(node);
        } catch (Exception failure) {
            throw new IllegalStateException("Could not encode bounded Kafka handoff", failure);
        }
    }

    private static byte[] sample(byte[] value) {
        if (value == null) {
            return new byte[0];
        }
        return java.util.Arrays.copyOf(value, Math.min(value.length, INVALID_SAMPLE_BYTES));
    }

    private static String sha256(byte[] value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256")
                    .digest(value == null ? new byte[0] : value));
        } catch (NoSuchAlgorithmException failure) {
            throw new IllegalStateException("SHA-256 is unavailable", failure);
        }
    }

    private static String deadLetterId(SourceRecord source) {
        String identity = source.topic() + "\n" + source.partition() + "\n" + source.offset() + "\nDLQ";
        return sha256(identity.getBytes(StandardCharsets.UTF_8));
    }

    private static String boundedCode(String errorCode) {
        if (errorCode != null && errorCode.matches("[A-Z][A-Z0-9_]{0,63}")) {
            return errorCode;
        }
        return "INVALID_RECORD";
    }
}
