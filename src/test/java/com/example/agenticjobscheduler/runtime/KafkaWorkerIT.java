package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AlterConfigOp;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class KafkaWorkerIT {
    private static final ObjectMapper JSON = new ObjectMapper();

    // Per-test container: tests share topics and one alters topic config.
    @Container
    private final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test
    void retryHandoffAndInvalidRecordsReachTheirAcknowledgedDestinations() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        Vertx vertx = Vertx.vertx();
        AtomicInteger mainAttempts = new AtomicInteger();
        AtomicInteger retryAttempts = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> attempts = new ConcurrentLinkedQueue<>();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 4, 3),
                execution -> {
                    mainAttempts.incrementAndGet();
                    attempts.add(execution.attempt());
                    return Future.failedFuture(new IllegalStateException("temporary failure"));
                });
        KafkaWorker retry = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.RETRY, 2, 3),
                execution -> {
                    retryAttempts.incrementAndGet();
                    attempts.add(execution.attempt());
                    return Future.succeededFuture();
                });
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            retry.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            UUID validJobId = UUID.randomUUID();
            JobEnvelope validJob = job(validJobId);
            byte[] validKey = JobRecordEncoder.key(validJobId);
            byte[] validValue = JobRecordEncoder.main(validJob);
            int expectedPartition = partition(validKey, 3);

            long validOffset;
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties(bootstrap))) {
                producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC, null,
                        "{\"broken\":".getBytes(StandardCharsets.UTF_8))).get(10, TimeUnit.SECONDS);
                byte[] oversized = new byte[EnvelopeValidator.MAIN_MAX_BYTES + 1];
                java.util.Arrays.fill(oversized, (byte) ' ');
                producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC, null, oversized))
                        .get(10, TimeUnit.SECONDS);
                producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        (expectedPartition + 1) % 3, validKey, validValue)).get(10, TimeUnit.SECONDS);
                validOffset = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        validKey, validValue)).get(10, TimeUnit.SECONDS).offset();
            }

            List<JsonNode> deadLetters = readJsonRecords(bootstrap, TopicProvisioner.DLQ_TOPIC, 3);
            assertEquals(3, deadLetters.size());
            assertTrue(deadLetters.stream().anyMatch(node ->
                    node.path("errorCode").asText().equals("RECORD_TOO_LARGE")));
            assertTrue(deadLetters.stream().anyMatch(node ->
                    node.path("errorCode").asText().equals("INVALID_JSON")));
            JsonNode wrongPartition = deadLetters.stream().filter(node ->
                    node.path("errorCode").asText().equals("WRONG_PARTITION"))
                    .findFirst().orElseThrow();
            assertNotNull(wrongPartition.path("job").get("jobId"));
            assertTrue(wrongPartition.path("handoffId").asText().matches("[0-9a-f]{64}"));
            assertTrue(deadLetters.stream()
                    .filter(node -> node.has("inputSampleBase64"))
                    .allMatch(node -> java.util.Base64.getDecoder()
                            .decode(node.path("inputSampleBase64").asText()).length <= 256));

            List<JsonNode> retryRecords = readJsonRecords(bootstrap, TopicProvisioner.RETRY_TOPIC, 1);
            assertEquals(1, retryRecords.size());
            JsonNode retryRecord = retryRecords.getFirst();
            assertEquals(2, retryRecord.path("attempt").asInt());
            assertEquals(validJobId.toString(), retryRecord.path("job").path("jobId").asText());
            assertGroupOffset(bootstrap, WorkerRole.RETRY.groupId(),
                    new TopicPartition(TopicProvisioner.RETRY_TOPIC,
                            partition(validKey, 3)), 1);
            assertEquals(1, mainAttempts.get());
            assertEquals(1, retryAttempts.get());
            assertEquals(Set.of(1, 2), Set.copyOf(attempts));
            // Null-key records may share this partition, so only a lower bound is stable.
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(),
                    new TopicPartition(TopicProvisioner.MAIN_TOPIC, expectedPartition), validOffset + 1);

            setTopicMaxMessageBytes(bootstrap, 1);
            UUID blockedJobId = UUID.randomUUID();
            byte[] blockedKey = JobRecordEncoder.key(blockedJobId);
            int blockedPartition = partition(blockedKey, 3);
            TopicPartition blockedSourcePartition = new TopicPartition(
                    TopicProvisioner.MAIN_TOPIC, blockedPartition);
            long committedBeforeBlockedJob = committedOffset(
                    bootstrap, WorkerRole.MAIN.groupId(), blockedSourcePartition);
            long blockedOffset;
            try {
                try (KafkaProducer<byte[], byte[]> producer =
                             new KafkaProducer<>(producerProperties(bootstrap))) {
                    var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            blockedKey, JobRecordEncoder.main(job(blockedJobId))))
                            .get(10, TimeUnit.SECONDS);
                    assertEquals(blockedPartition, metadata.partition());
                    blockedOffset = metadata.offset();
                }
                awaitCounter(mainAttempts, 2);
                Thread.sleep(2_000);
                assertEquals(2, mainAttempts.get(),
                        "a failed retry publication must not rerun the handler");
                assertEquals(committedBeforeBlockedJob,
                        committedOffset(bootstrap, WorkerRole.MAIN.groupId(), blockedSourcePartition),
                        "the source must remain uncommitted until retry publication is acknowledged");
                assertEquals(1, retryAttempts.get(),
                        "the retry handler must not see an unacknowledged handoff");
            } finally {
                setTopicMaxMessageBytes(bootstrap, 1_048_576);
            }

            assertGroupOffset(bootstrap, WorkerRole.MAIN.groupId(),
                    blockedSourcePartition, blockedOffset + 1);
            List<JsonNode> recoveredRetries = readJsonRecords(
                    bootstrap, TopicProvisioner.RETRY_TOPIC, 2);
            assertTrue(recoveredRetries.stream().anyMatch(node ->
                    node.path("job").path("jobId").asText().equals(blockedJobId.toString())
                            && node.path("attempt").asInt() == 2));
            assertGroupOffset(bootstrap, WorkerRole.RETRY.groupId(),
                    new TopicPartition(TopicProvisioner.RETRY_TOPIC, blockedPartition),
                    blockedPartition == partition(validKey, 3) ? 2 : 1);
            assertEquals(2, mainAttempts.get());
            assertEquals(2, retryAttempts.get());
        } finally {
            retry.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B08: permanent and exhausted failures reach the DLQ before the source commits. */
    @Test
    void permanentAndExhaustedFailuresReachDlqBeforeSourceCompletes() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);
        UUID permanentId = UUID.randomUUID();
        UUID exhaustedId = UUID.randomUUID();
        AtomicInteger permanentRuns = new AtomicInteger();
        ConcurrentLinkedQueue<Integer> exhaustedAttempts = new ConcurrentLinkedQueue<>();

        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 4, 3),
                execution -> handle(execution.job().jobId(), permanentId, permanentRuns,
                        exhaustedAttempts, execution.attempt()));
        KafkaWorker retry = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.RETRY, 2, 3),
                execution -> handle(execution.job().jobId(), permanentId, permanentRuns,
                        exhaustedAttempts, execution.attempt()));
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            retry.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            byte[] permanentKey = JobRecordEncoder.key(permanentId);
            byte[] exhaustedKey = JobRecordEncoder.key(exhaustedId);
            long permanentOffset;
            long exhaustedOffset;
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties(bootstrap))) {
                permanentOffset = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        permanentKey, JobRecordEncoder.main(job(permanentId, 3))))
                        .get(10, TimeUnit.SECONDS).offset();
                exhaustedOffset = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        exhaustedKey, JobRecordEncoder.main(job(exhaustedId, 2))))
                        .get(10, TimeUnit.SECONDS).offset();
            }

            JsonNode permanentDlq = awaitDeadLetter(bootstrap, permanentId);
            assertEquals("PERMANENT_HANDLER_FAILURE", permanentDlq.path("errorCode").asText());
            assertEquals(TopicProvisioner.MAIN_TOPIC,
                    permanentDlq.path("failedSource").path("topic").asText());
            assertEquals(1, permanentRuns.get(), "a permanent failure must not be retried");
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(),
                    new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition(permanentKey, 3)),
                    permanentOffset + 1);

            JsonNode exhaustedDlq = awaitDeadLetter(bootstrap, exhaustedId);
            assertEquals("ATTEMPTS_EXHAUSTED", exhaustedDlq.path("errorCode").asText());
            assertEquals(TopicProvisioner.RETRY_TOPIC,
                    exhaustedDlq.path("failedSource").path("topic").asText());
            assertEquals(List.of(1, 2), List.copyOf(exhaustedAttempts));
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(),
                    new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition(exhaustedKey, 3)),
                    exhaustedOffset + 1);
            awaitCommittedAtLeast(bootstrap, WorkerRole.RETRY.groupId(),
                    new TopicPartition(TopicProvisioner.RETRY_TOPIC, partition(exhaustedKey, 3)), 1);
            Thread.sleep(1_000);
            assertEquals(2, exhaustedAttempts.size(), "no attempt beyond maxAttempts");
        } finally {
            retry.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static Future<Void> handle(UUID jobId, UUID permanentId, AtomicInteger permanentRuns,
                                       ConcurrentLinkedQueue<Integer> exhaustedAttempts, int attempt) {
        if (jobId.equals(permanentId)) {
            permanentRuns.incrementAndGet();
            return Future.failedFuture(new com.example.agenticjobscheduler.execution.PermanentJobFailure());
        }
        exhaustedAttempts.add(attempt);
        return Future.failedFuture(new IllegalStateException("temporary failure"));
    }

    private static JsonNode awaitDeadLetter(String bootstrap, UUID jobId) throws Exception {
        try (KafkaConsumer<byte[], byte[]> consumer =
                     new KafkaConsumer<>(consumerProperties(bootstrap, "assert-" + UUID.randomUUID()))) {
            consumer.subscribe(List.of(TopicProvisioner.DLQ_TOPIC));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    JsonNode node = JSON.readTree(record.value());
                    if (jobId.toString().equals(node.path("jobId").asText())) {
                        return node;
                    }
                }
            }
        }
        throw new AssertionError("timed out waiting for a DLQ record for " + jobId);
    }

    private static JobEnvelope job(UUID id) {
        return job(id, 3);
    }

    private static JobEnvelope job(UUID id, int maxAttempts) {
        return new JobEnvelope(1, id, UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode().put("message", "safe demo"), maxAttempts);
    }

    private static int partition(byte[] key, int partitions) {
        return org.apache.kafka.common.utils.Utils.toPositive(
                org.apache.kafka.common.utils.Utils.murmur2(key)) % partitions;
    }

    private static List<JsonNode> readJsonRecords(String bootstrap, String topic, int expected)
            throws Exception {
        Properties properties = consumerProperties(bootstrap, "assert-" + UUID.randomUUID());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            List<JsonNode> values = new ArrayList<>();
            while (values.size() < expected && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(200));
                records.forEach(record -> {
                    try {
                        values.add(JSON.readTree(record.value()));
                    } catch (Exception failure) {
                        throw new IllegalStateException("Worker emitted invalid JSON", failure);
                    }
                });
            }
            assertEquals(expected, values.size(), "timed out waiting for " + topic + " records");
            return values;
        }
    }

    private static void assertGroupOffset(String bootstrap, String group,
                                          TopicPartition partition, long expected) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(properties)) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                        .get(10, TimeUnit.SECONDS);
                var committed = offsets.get(partition);
                if (committed != null && committed.offset() == expected) {
                    return;
                }
                Thread.sleep(100);
            }
            throw new AssertionError("Consumer group " + group + " did not commit " + partition
                    + " to offset " + expected);
        }
    }

    private static void awaitCommittedAtLeast(String bootstrap, String group,
                                              TopicPartition partition, long expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (committedOffset(bootstrap, group, partition) >= expected) {
                return;
            }
            Thread.sleep(100);
        }
        throw new AssertionError(group + " did not commit " + partition + " to at least " + expected + " (committed=" + committedOffset(bootstrap, group, partition) + ")");
    }

    private static long committedOffset(String bootstrap, String group, TopicPartition partition)
            throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrap);
        try (AdminClient admin = AdminClient.create(properties)) {
            var offsets = admin.listConsumerGroupOffsets(group).partitionsToOffsetAndMetadata()
                    .get(10, TimeUnit.SECONDS);
            var committed = offsets.get(partition);
            return committed == null ? -1 : committed.offset();
        }
    }

    private static void setTopicMaxMessageBytes(String bootstrap, int maxBytes) throws Exception {
        var resource = new ConfigResource(ConfigResource.Type.TOPIC, TopicProvisioner.RETRY_TOPIC);
        var config = List.of(new AlterConfigOp(
                new ConfigEntry("max.message.bytes", Integer.toString(maxBytes)),
                AlterConfigOp.OpType.SET));
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
            admin.incrementalAlterConfigs(Map.of(resource, config)).all()
                    .get(10, TimeUnit.SECONDS);
        }
    }

    private static void awaitCounter(AtomicInteger counter, int expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (counter.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertEquals(expected, counter.get(), "timed out waiting for handler attempt " + expected);
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        return properties;
    }

    private static Properties consumerProperties(String bootstrap, String group) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        return properties;
    }
}
