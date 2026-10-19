package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.example.agenticjobscheduler.execution.ProcessingWindow;
import com.example.agenticjobscheduler.execution.JobExecution;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.time.Duration;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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
import org.testcontainers.DockerClientFactory;
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

    /** B07: retry starts respect per-pod rate/concurrency and failures advance attempts. */
    @Test
    void retryWorkerEnforcesConfiguredRateAndConcurrencyAndAdvancesAttempts() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        List<UUID> jobIds = new ArrayList<>();
        Set<Integer> partitions = new HashSet<>();
        while (jobIds.size() < 3) {
            UUID id = UUID.randomUUID();
            int partition = partition(JobRecordEncoder.key(id), 3);
            if (partitions.add(partition)) {
                jobIds.add(id);
            }
        }
        UUID failingJobId = jobIds.getFirst();
        ConcurrentLinkedQueue<JobExecution> executions = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Long> startTimes = new ConcurrentLinkedQueue<>();
        Map<UUID, Promise<Void>> attemptTwoHandlers = new ConcurrentHashMap<>();
        AtomicInteger starts = new AtomicInteger();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();

        Vertx vertx = Vertx.vertx();
        KafkaWorker retry = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.RETRY, 2, 3, 4),
                execution -> {
                    executions.add(execution);
                    startTimes.add(System.nanoTime());
                    maximumActive.accumulateAndGet(active.incrementAndGet(), Math::max);
                    Future<Void> result;
                    if (execution.attempt() == 2) {
                        Promise<Void> held = Promise.promise();
                        attemptTwoHandlers.put(execution.job().jobId(), held);
                        result = held.future();
                    } else {
                        result = Future.succeededFuture();
                    }
                    result.onComplete(ignored -> active.decrementAndGet());
                    starts.incrementAndGet();
                    return result;
                });
        try {
            retry.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            Map<Integer, Long> inputOffsets = new java.util.HashMap<>();
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (UUID jobId : jobIds) {
                    byte[] key = JobRecordEncoder.key(jobId);
                    int partition = partition(key, 3);
                    SourceRecord source = new SourceRecord(
                            TopicProvisioner.MAIN_TOPIC, partition, 0);
                    JobEnvelope job = job(jobId, 3);
                    var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.RETRY_TOPIC,
                            partition, key, JobRecordEncoder.retry(job, 2, source, source,
                            "HANDLER_FAILURE"))).get(10, TimeUnit.SECONDS);
                    assertEquals(partition, metadata.partition());
                    inputOffsets.put(partition, metadata.offset());
                }
            }

            awaitCounter(starts, 2);
            List<Long> firstStarts = List.copyOf(startTimes);
            assertTrue(startInterval(firstStarts, 0) >= TimeUnit.MILLISECONDS.toNanos(175),
                    "retry starts must be separated by the configured 4-per-second rate");
            assertEquals(2, active.get());
            Thread.sleep(150);
            assertEquals(2, starts.get(),
                    "a third handler must not start while both configured slots are occupied");

            JobExecution firstReleasable = executions.stream()
                    .filter(execution -> !execution.job().jobId().equals(failingJobId))
                    .findFirst().orElseThrow();
            assertTrue(attemptTwoHandlers.get(firstReleasable.job().jobId()).tryComplete());
            awaitCounter(starts, 3);
            assertTrue(maximumActive.get() <= 2);
            assertTrue(startInterval(List.copyOf(startTimes), 1)
                            >= TimeUnit.MILLISECONDS.toNanos(175),
                    "the next retry start must also respect the configured rate");

            assertTrue(attemptTwoHandlers.get(failingJobId)
                    .tryFail(new IllegalStateException("transient retry failure")));
            for (UUID jobId : jobIds) {
                if (!jobId.equals(firstReleasable.job().jobId())
                        && !jobId.equals(failingJobId)) {
                    attemptTwoHandlers.get(jobId).tryComplete();
                }
            }
            awaitCounter(starts, 4);
            awaitNoActiveHandlers(active);

            assertEquals(0, active.get());
            assertEquals(2, maximumActive.get());
            Map<UUID, List<Integer>> attemptsByJob = executions.stream().collect(
                    java.util.stream.Collectors.groupingBy(
                            execution -> execution.job().jobId(),
                            java.util.stream.Collectors.mapping(
                                    JobExecution::attempt, java.util.stream.Collectors.toList())));
            assertEquals(List.of(2, 3), attemptsByJob.get(failingJobId));
            for (UUID jobId : jobIds) {
                if (!jobId.equals(failingJobId)) {
                    assertEquals(List.of(2), attemptsByJob.get(jobId));
                }
            }
            List<Long> allStarts = List.copyOf(startTimes);
            for (int index = 0; index < allStarts.size() - 1; index++) {
                assertTrue(startInterval(allStarts, index) >= TimeUnit.MILLISECONDS.toNanos(175),
                        "retry handler start interval was below the configured rate");
            }
            List<JsonNode> retryRecords = readJsonRecords(
                    bootstrap, TopicProvisioner.RETRY_TOPIC, 4);
            assertTrue(retryRecords.stream().anyMatch(record ->
                    record.path("job").path("jobId").asText().equals(failingJobId.toString())
                            && record.path("attempt").asInt() == 3),
                    "a retryable failure at attempt 2 must publish attempt 3");
            for (var entry : inputOffsets.entrySet()) {
                long expected = entry.getValue() + 1 + (entry.getKey()
                        == partition(JobRecordEncoder.key(failingJobId), 3) ? 1 : 0);
                awaitCommittedAtLeast(bootstrap, WorkerRole.RETRY.groupId(),
                        new TopicPartition(TopicProvisioner.RETRY_TOPIC, entry.getKey()), expected);
            }
        } finally {
            retry.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * H02: a retry handler still sitting behind the rate gate when its owning assignment is
     * revoked must never be dispatched once its turn comes up, and its processing-window
     * reservation must still be released even though it never ran.
     */
    @Test
    void revokedOwnershipInvalidatesRateGateQueuedRetryBeforeItEverDispatches() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 2);

        List<UUID> jobIds = new ArrayList<>();
        Set<Integer> partitions = new HashSet<>();
        while (jobIds.size() < 2) {
            UUID id = UUID.randomUUID();
            int partition = partition(JobRecordEncoder.key(id), 2);
            if (partitions.add(partition)) {
                jobIds.add(id);
            }
        }

        ConcurrentLinkedQueue<UUID> startedJobIds = new ConcurrentLinkedQueue<>();
        AtomicInteger starts = new AtomicInteger();
        ProcessingWindow window = new ProcessingWindow();

        Vertx vertx = Vertx.vertx();
        // concurrency=2 so both distinct-job, distinct-partition retries go RUNNING immediately
        // in their own per-partition dispatcher; startsPerSecond=1 paces the shared retry rate
        // gate so only one of the two can actually be dispatched before the 500ms shutdown
        // timeout forces ownership revocation, well short of the 1000ms pacing interval.
        KafkaWorker retry = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.RETRY, 2, 2, 1),
                execution -> {
                    starts.incrementAndGet();
                    startedJobIds.add(execution.job().jobId());
                    return Promise.<Void>promise().future();
                }, window, 500);
        try {
            retry.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                UUID firstJobId = jobIds.get(0);
                byte[] firstKey = JobRecordEncoder.key(firstJobId);
                int firstPartition = partition(firstKey, 2);
                SourceRecord firstSource = new SourceRecord(
                        TopicProvisioner.MAIN_TOPIC, firstPartition, 0);
                producer.send(new ProducerRecord<>(TopicProvisioner.RETRY_TOPIC,
                        firstPartition, firstKey, JobRecordEncoder.retry(job(firstJobId, 3), 2,
                        firstSource, firstSource, "HANDLER_FAILURE")))
                        .get(10, TimeUnit.SECONDS);
                awaitCounter(starts, 1);
                awaitRetainedRecords(window, 1, 20);

                UUID secondJobId = jobIds.get(1);
                byte[] secondKey = JobRecordEncoder.key(secondJobId);
                int secondPartition = partition(secondKey, 2);
                SourceRecord secondSource = new SourceRecord(
                        TopicProvisioner.MAIN_TOPIC, secondPartition, 0);
                producer.send(new ProducerRecord<>(TopicProvisioner.RETRY_TOPIC,
                        secondPartition, secondKey, JobRecordEncoder.retry(job(secondJobId, 3), 2,
                        secondSource, secondSource, "HANDLER_FAILURE")))
                        .get(10, TimeUnit.SECONDS);
                awaitRetainedRecords(window, 2, 20);
            }

            UUID winner = startedJobIds.peek();
            UUID loser = jobIds.stream().filter(id -> !id.equals(winner)).findFirst().orElseThrow();

            retry.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            // Let the rate gate's already-scheduled ~1-second timer for the loser fire; it must
            // find the owning assignment revoked and skip dispatching rather than wait forever.
            Thread.sleep(1_200);
            assertEquals(1, starts.get(),
                    "the rate-gate-queued loser must never be dispatched once ownership is revoked");
            assertNoHandoffForJob(bootstrap, loser);
            assertEquals(1, window.retainedRecords(),
                    "only the still-running winner's reservation remains; the invalidated loser's "
                            + "reservation must be released even though it never ran");
        } finally {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * H01: shutdown revokes the current assignment, releasing never-started queued work while
     * retaining a running handler's reservation until that stale handler settles.
     */
    @Test
    void revokedAssignmentRetiresQueuedAndSettledHandlerReservations() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 1);

        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        int partition = partition(key, 1);
        ProcessingWindow oldWindow = new ProcessingWindow();
        AtomicInteger oldStarts = new AtomicInteger();
        Promise<Void> oldHandler = Promise.promise();
        ConcurrentLinkedQueue<Promise<Void>> replacementHandlers = new ConcurrentLinkedQueue<>();
        AtomicInteger replacementStarts = new AtomicInteger();

        Vertx vertx = Vertx.vertx();
        KafkaWorker oldOwner = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 1, 1), execution -> {
                    oldStarts.incrementAndGet();
                    return oldHandler.future();
                }, oldWindow, 500);
        ProcessingWindow replacementWindow = new ProcessingWindow();
        KafkaWorker replacement = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 1, 1), execution -> {
                    Promise<Void> gate = Promise.promise();
                    replacementHandlers.add(gate);
                    replacementStarts.incrementAndGet();
                    return gate.future();
                }, replacementWindow);
        try {
            oldOwner.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        partition, key, JobRecordEncoder.main(job(jobId))))
                        .get(10, TimeUnit.SECONDS);
                awaitAtLeast(oldStarts, 1, 20);
                awaitRetainedRecords(oldWindow, 1, 20);

                producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        partition, key, JobRecordEncoder.main(job(jobId))))
                        .get(10, TimeUnit.SECONDS);
                awaitRetainedRecords(oldWindow, 2, 20);
            }

            oldOwner.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            assertEquals(1, oldWindow.retainedRecords(),
                    "revocation must retire the queued successor but retain the running handler");
            assertEquals(1, oldStarts.get(), "the queued successor must never start under the old owner");

            replacement.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            awaitAtLeast(replacementStarts, 1, 20);
            TopicPartition sourcePartition = new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition);
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(), sourcePartition),
                    "the replacement must replay while the old handler is still blocked");

            oldHandler.tryComplete();
            awaitRetainedRecords(oldWindow, 0, 20);
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(), sourcePartition),
                    "settling the revoked handler must release its reservation without committing");

            replacementHandlers.remove().tryComplete();
            awaitAtLeast(replacementStarts, 2, 20);
            replacementHandlers.remove().tryComplete();
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), sourcePartition, 2);
        } finally {
            oldHandler.tryComplete();
            oldOwner.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            replacement.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B13: the main worker applies healthy and slow-window concurrency changes. */
    @Test
    void mainWorkerAdaptsConcurrencyFromMeasuredHandlerWindows() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        AtomicBoolean slowPhase = new AtomicBoolean();
        AtomicBoolean measureReducedLimit = new AtomicBoolean();
        AtomicBoolean reducedConcurrencyObserved = new AtomicBoolean();
        AtomicInteger active = new AtomicInteger();
        AtomicInteger maximumActive = new AtomicInteger();
        AtomicInteger activeAtMeasuredStart = new AtomicInteger();
        AtomicInteger slowCompletions = new AtomicInteger();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 4, 3),
                execution -> {
                    int running = active.incrementAndGet();
                    maximumActive.accumulateAndGet(running, Math::max);
                    if (measureReducedLimit.get() && running <= 2) {
                        activeAtMeasuredStart.set(running);
                        reducedConcurrencyObserved.set(true);
                    }
                    boolean slow = slowPhase.get();
                    Promise<Void> completion = Promise.promise();
                    vertx.setTimer(slow ? 1_100 : 250, ignored -> {
                        active.decrementAndGet();
                        if (slow) {
                            slowCompletions.incrementAndGet();
                        }
                        completion.tryComplete();
                    });
                    return completion.future();
                });
        try {
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 250; index++) {
                    UUID jobId = UUID.randomUUID();
                    byte[] key = JobRecordEncoder.key(jobId);
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            key, JobRecordEncoder.main(job(jobId))))
                            .get(10, TimeUnit.SECONDS);
                }
            }
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

            awaitAtLeast(maximumActive, 5, 12);
            slowPhase.set(true);
            awaitAtLeast(slowCompletions, 20, 8);
            Thread.sleep(10_500);
            assertTrue(slowCompletions.get() >= 20,
                    "the slow evaluation window must contain at least 20 completed attempts");

            measureReducedLimit.set(true);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(8);
            while (!reducedConcurrencyObserved.get() && System.nanoTime() < deadline) {
                Thread.sleep(25);
            }
            assertTrue(reducedConcurrencyObserved.get(),
                    "the worker should resume queued work at the halved concurrency");
                assertTrue(activeAtMeasuredStart.get() >= 1 && activeAtMeasuredStart.get() <= 2,
                    "after slow windows, the worker should start queued work within the reduced limit");
            assertTrue(maximumActive.get() >= 5,
                    "a healthy backlog window should increase the initial concurrency from 4 to 5");
        } finally {
            slowPhase.set(false);
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B14: the worker pauses at its bounded window and resumes after commit release. */
    @Test
    void mainWorkerRetainsAndResumesSameJobBacklogAtWindowWatermark() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        ProcessingWindow window = new ProcessingWindow(6, 128 * 1024, 2, 32 * 1024);
        AtomicInteger handlerStarts = new AtomicInteger();
        Promise<Void> firstHandler = Promise.promise();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 4, 3),
                execution -> handlerStarts.incrementAndGet() == 1
                        ? firstHandler.future() : Future.succeededFuture(),
                window);
        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        byte[] value = JobRecordEncoder.main(job(jobId));
        int partition = partition(key, 3);
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 20; index++) {
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            partition, key, value)).get(10, TimeUnit.SECONDS);
                }
            }

            long pauseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (!window.intakePaused() && System.nanoTime() < pauseDeadline) {
                Thread.sleep(25);
            }
            assertTrue(window.intakePaused(), "the worker should pause at the count watermark");
            int retainedAtPause = window.retainedRecords();
            assertTrue(retainedAtPause >= 4 && retainedAtPause <= 6,
                    "retained work must include the watermark and at most one fetch overshoot");
            assertEquals(1, handlerStarts.get(), "same-job records must remain serialized");
            Thread.sleep(500);
            assertEquals(retainedAtPause, window.retainedRecords(),
                    "the paused worker must not admit another batch");
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition)),
                    "unfinished same-job work must not advance the source offset");

            firstHandler.tryComplete();
            awaitAtLeast(handlerStarts, 20, 30);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(),
                    new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition), 20);
            long releaseDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            while (window.retainedRecords() != 0 && System.nanoTime() < releaseDeadline) {
                Thread.sleep(25);
            }
            assertEquals(0, window.retainedRecords());
            assertFalse(window.intakePaused(), "the worker should resume below both low watermarks");
        } finally {
            firstHandler.tryComplete();
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B15: out-of-order completion must not block an independent partition commit. */
    @Test
    void mainWorkerCommitsIndependentPartitionWhileEarlierOffsetIsRunning() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        List<UUID> samePartitionJobs = new ArrayList<>();
        int samePartition = -1;
        while (samePartitionJobs.size() < 2) {
            UUID id = UUID.randomUUID();
            int candidatePartition = partition(JobRecordEncoder.key(id), 3);
            if (samePartition == -1) {
                samePartition = candidatePartition;
            }
            if (candidatePartition == samePartition) {
                samePartitionJobs.add(id);
            }
        }
        UUID candidateIndependentJobId;
        int candidatePartition;
        do {
            candidateIndependentJobId = UUID.randomUUID();
            candidatePartition = partition(JobRecordEncoder.key(candidateIndependentJobId), 3);
        } while (candidatePartition == samePartition);
        final UUID independentJobId = candidateIndependentJobId;
        final int independentPartition = candidatePartition;

        UUID slowJobId = samePartitionJobs.get(0);
        UUID laterJobId = samePartitionJobs.get(1);
        Promise<Void> slowHandler = Promise.promise();
        AtomicBoolean slowStarted = new AtomicBoolean();
        AtomicBoolean laterCompleted = new AtomicBoolean();
        AtomicBoolean independentCompleted = new AtomicBoolean();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 3, 3),
                execution -> {
                    UUID executedId = execution.job().jobId();
                    if (executedId.equals(slowJobId)) {
                        slowStarted.set(true);
                        return slowHandler.future();
                    }
                    if (executedId.equals(laterJobId)) {
                        laterCompleted.set(true);
                    }
                    if (executedId.equals(independentJobId)) {
                        independentCompleted.set(true);
                    }
                    return Future.succeededFuture();
                });
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                var slowMetadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        samePartition, JobRecordEncoder.key(slowJobId),
                        JobRecordEncoder.main(job(slowJobId)))).get(10, TimeUnit.SECONDS);
                var laterMetadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        samePartition, JobRecordEncoder.key(laterJobId),
                        JobRecordEncoder.main(job(laterJobId)))).get(10, TimeUnit.SECONDS);
                var independentMetadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        independentPartition, JobRecordEncoder.key(independentJobId),
                        JobRecordEncoder.main(job(independentJobId)))).get(10, TimeUnit.SECONDS);
                assertEquals(0, slowMetadata.offset());
                assertEquals(1, laterMetadata.offset());
                assertEquals(0, independentMetadata.offset());
            }

            long completionDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while ((!laterCompleted.get() || !independentCompleted.get() || !slowStarted.get())
                    && System.nanoTime() < completionDeadline) {
                Thread.sleep(25);
            }
            assertTrue(slowStarted.get(), "offset zero should be running behind the test barrier");
            assertTrue(laterCompleted.get(), "the later offset should complete out of order");
            assertTrue(independentCompleted.get(), "the independent partition should complete");

            TopicPartition slowPartition = new TopicPartition(
                    TopicProvisioner.MAIN_TOPIC, samePartition);
            TopicPartition independentTopicPartition = new TopicPartition(
                    TopicProvisioner.MAIN_TOPIC, independentPartition);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), independentTopicPartition, 1);
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(), slowPartition),
                    "the completed later offset cannot pass its unfinished partition prefix");

            slowHandler.tryComplete();
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), slowPartition, 2);
            assertEquals(1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            independentTopicPartition),
                    "completion in the slow partition must not disturb the independent commit");
        } finally {
            slowHandler.tryComplete();
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B19: shutdown cannot complete or release a same-job gate before handler confirmation. */
    @Test
    void workerShutdownWaitsForUnconfirmedHandlerBeforeCommittingSource() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        byte[] value = JobRecordEncoder.main(job(jobId));
        int partition = partition(key, 3);
        AtomicInteger handlerStarts = new AtomicInteger();
        Promise<Void> firstHandler = Promise.promise();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 2, 3),
                execution -> handlerStarts.incrementAndGet() == 1
                        ? firstHandler.future() : Future.succeededFuture());
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 2; index++) {
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            partition, key, value)).get(10, TimeUnit.SECONDS);
                }
            }

            awaitAtLeast(handlerStarts, 1, 20);
            Future<Void> shutdown = main.close();
            Thread.sleep(500);
            assertFalse(shutdown.isComplete(),
                    "shutdown must remain pending while handler completion is unconfirmed");
            assertEquals(1, handlerStarts.get(),
                    "the next same-job record must remain gated behind the running handler");
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition)),
                    "the source prefix must remain uncommitted while the handler is running");

            firstHandler.tryComplete();
            shutdown.toCompletionStage().toCompletableFuture().get(20, TimeUnit.SECONDS);
            assertEquals(2, handlerStarts.get());
            assertEquals(2, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition)));
        } finally {
            firstHandler.tryComplete();
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B19: timeout retry waits until the handler future confirms that cancellation stopped work. */
    @Test
    void timedOutHandlerRetriesAndReleasesGateOnlyAfterStopConfirmation() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        byte[] value = JobRecordEncoder.main(job(jobId, 3));
        int partition = partition(key, 3);
        AtomicInteger handlerStarts = new AtomicInteger();
        Promise<Void> cancellationRequested = Promise.promise();
        Promise<Void> firstHandler = Promise.promise();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 2, 3),
                execution -> {
                    if (execution.source().offset() == 0) {
                        handlerStarts.incrementAndGet();
                        execution.cancellation().cancellationRequested()
                                .onSuccess(ignored -> cancellationRequested.tryComplete());
                        return firstHandler.future();
                    }
                    handlerStarts.incrementAndGet();
                    return Future.succeededFuture();
                }, new ProcessingWindow(), 2_000, 100, 250);
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 2; index++) {
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            partition, key, value)).get(10, TimeUnit.SECONDS);
                }
            }

            cancellationRequested.future().toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            assertEquals(1, handlerStarts.get(),
                    "the same-job successor must stay gated until cancellation is confirmed");
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition)),
                    "the source remains incomplete while the handler future is unresolved");

            firstHandler.tryComplete();
            List<JsonNode> retries = readJsonRecords(bootstrap, TopicProvisioner.RETRY_TOPIC, 1);
            assertEquals(2, retries.getFirst().path("attempt").asInt());
            assertEquals("HANDLER_TIMEOUT", retries.getFirst().path("errorCode").asText());
            awaitAtLeast(handlerStarts, 2, 10);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(),
                    new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition), 2);
            assertEquals(2, handlerStarts.get());
        } finally {
            firstHandler.tryComplete();
            main.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B19 negative case: no cancellation acknowledgment keeps the gate and source offset incomplete. */
    @Test
    void unconfirmedHandlerCancellationKeepsGateAndOffsetIncomplete() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        byte[] value = JobRecordEncoder.main(job(jobId, 3));
        int partition = partition(key, 3);
        AtomicInteger handlerStarts = new AtomicInteger();
        Promise<Void> cancellationRequested = Promise.promise();
        Promise<Void> neverStops = Promise.promise();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 2, 3),
                execution -> {
                    handlerStarts.incrementAndGet();
                    execution.cancellation().cancellationRequested()
                            .onSuccess(ignored -> cancellationRequested.tryComplete());
                    return neverStops.future();
                }, new ProcessingWindow(), 500, 100, 100);
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 2; index++) {
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            partition, key, value)).get(10, TimeUnit.SECONDS);
                }
            }

            cancellationRequested.future().toCompletionStage().toCompletableFuture()
                    .get(10, TimeUnit.SECONDS);
            Thread.sleep(500);
            assertEquals(1, handlerStarts.get(),
                    "an unconfirmed cancellation must retain the same-job gate");
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(),
                            new TopicPartition(TopicProvisioner.MAIN_TOPIC, partition)),
                    "an unconfirmed cancellation must not complete the source record");
        } finally {
            main.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /**
     * B11: a revoked owner's late handler callback cannot dispatch or commit stale work.
     * H01: revoking a partition must also release the revoked owner's window reservations for
     * its never-started queued successor, discarded immediately at revoke.
     */
    @Test
    void workerRebalanceIgnoresStaleHandlerAndReplaysOnNewOwner() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        Map<Integer, UUID> jobsByPartition = new java.util.HashMap<>();
        while (jobsByPartition.size() < 3) {
            UUID jobId = UUID.randomUUID();
            jobsByPartition.putIfAbsent(partition(JobRecordEncoder.key(jobId), 3), jobId);
        }
        Map<UUID, Integer> partitionsByJob = new ConcurrentHashMap<>();
        jobsByPartition.forEach((partition, jobId) -> partitionsByJob.put(jobId, partition));

        AtomicBoolean completeOldAutomatically = new AtomicBoolean();
        AtomicBoolean completeNewAutomatically = new AtomicBoolean();
        AtomicInteger oldStarts = new AtomicInteger();
        AtomicInteger newStarts = new AtomicInteger();
        ConcurrentLinkedQueue<UUID> newStartedJobs = new ConcurrentLinkedQueue<>();
        Map<UUID, AtomicInteger> oldStartsByJob = new ConcurrentHashMap<>();
        Map<UUID, AtomicInteger> newStartsByJob = new ConcurrentHashMap<>();
        Map<UUID, ConcurrentLinkedQueue<Promise<Void>>> oldGates = new ConcurrentHashMap<>();
        Map<UUID, ConcurrentLinkedQueue<Promise<Void>>> newGates = new ConcurrentHashMap<>();

        Vertx oldVertx = Vertx.vertx();
        Vertx newVertx = Vertx.vertx();
        ProcessingWindow oldWindow = new ProcessingWindow(
                1_000, 16L * 1024 * 1024, 2, 2L * 1024 * 1024);
        KafkaWorker oldOwner = new KafkaWorker(oldVertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 3, 3), execution -> {
                    UUID jobId = execution.job().jobId();
                    if (completeOldAutomatically.get()) {
                        return Future.succeededFuture();
                    }
                    oldStarts.incrementAndGet();
                    oldStartsByJob.computeIfAbsent(jobId, ignored -> new AtomicInteger())
                            .incrementAndGet();
                    Promise<Void> gate = Promise.promise();
                    oldGates.computeIfAbsent(jobId, ignored -> new ConcurrentLinkedQueue<>()).add(gate);
                    return gate.future();
                }, oldWindow, 1_000);
        KafkaWorker newOwner = new KafkaWorker(newVertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 3, 3), execution -> {
                    UUID jobId = execution.job().jobId();
                    if (completeNewAutomatically.get()) {
                        return Future.succeededFuture();
                    }
                    newStarts.incrementAndGet();
                    newStartedJobs.add(jobId);
                    newStartsByJob.computeIfAbsent(jobId, ignored -> new AtomicInteger())
                            .incrementAndGet();
                    Promise<Void> gate = Promise.promise();
                    newGates.computeIfAbsent(jobId, ignored -> new ConcurrentLinkedQueue<>()).add(gate);
                    return gate.future();
                });
        try {
            oldOwner.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                int started = 0;
                for (var entry : jobsByPartition.entrySet()) {
                    UUID jobId = entry.getValue();
                    byte[] key = JobRecordEncoder.key(jobId);
                    byte[] value = JobRecordEncoder.main(job(jobId));
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            entry.getKey(), key, value)).get(10, TimeUnit.SECONDS);
                    awaitAtLeast(oldStarts, ++started, 20);
                }
                for (UUID jobId : jobsByPartition.values()) {
                    assertEquals(1, oldStartsByJob.get(jobId).get());
                }
                for (var entry : jobsByPartition.entrySet()) {
                    UUID jobId = entry.getValue();
                    int expectedRetained = oldWindow.retainedRecords() + 1;
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            entry.getKey(), JobRecordEncoder.key(jobId),
                            JobRecordEncoder.main(job(jobId)))).get(10, TimeUnit.SECONDS);
                    awaitRetainedRecords(oldWindow, expectedRetained, 20);
                }
            }
            long retainedDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (oldWindow.retainedRecords() < 6 && System.nanoTime() < retainedDeadline) {
                Thread.sleep(25);
            }
            assertTrue(oldWindow.retainedRecords() >= 6,
                    "the old owner must retain each queued successor before the rebalance");
            int retainedBeforeRebalance = oldWindow.retainedRecords();

            newOwner.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            awaitAtLeast(newStarts, 1, 30);
            UUID replayedJob = newStartedJobs.peek();
            int replayedPartition = partitionsByJob.get(replayedJob);
            TopicPartition topicPartition = new TopicPartition(
                    TopicProvisioner.MAIN_TOPIC, replayedPartition);
            assertEquals(1, newStartsByJob.get(replayedJob).get());

            // H01: a queued successor's reservation must be released on revoke, before the
            // still-running handler settles. The observed post-revoke count accounts for any
            // other queued records discarded in the same consumer-group rebalance.
            long discardDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
            while (oldWindow.retainedRecords() >= retainedBeforeRebalance
                    && System.nanoTime() < discardDeadline) {
                Thread.sleep(25);
            }
            assertTrue(oldWindow.retainedRecords() < retainedBeforeRebalance,
                    "revoking partitions must release their discarded queued successors");

            oldGates.get(replayedJob).peek().tryComplete();
            Thread.sleep(500);
            assertEquals(1, oldStartsByJob.get(replayedJob).get(),
                    "revocation must prevent the old owner from starting its queued successor");
            assertEquals(-1, committedOffset(bootstrap, WorkerRole.MAIN.groupId(), topicPartition),
                    "the stale old-owner callback must not commit while the new owner is blocked");

            newGates.get(replayedJob).peek().tryComplete();
            AtomicInteger replayedStarts = newStartsByJob.get(replayedJob);
            awaitAtLeast(replayedStarts, 2, 20);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), topicPartition, 1);
            var gates = List.copyOf(newGates.get(replayedJob));
            assertEquals(2, gates.size());
            gates.get(1).tryComplete();
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), topicPartition, 2);
        } finally {
            completeOldAutomatically.set(true);
            completeNewAutomatically.set(true);
            oldGates.values().forEach(gates -> gates.forEach(Promise::tryComplete));
            newGates.values().forEach(gates -> gates.forEach(Promise::tryComplete));
            oldOwner.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            newOwner.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            oldVertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            newVertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    /** B18: handler completion and polling recover after a sustained broker outage. */
    @Test
    void workerRecoversCommitsAndPollingAfterKafkaBrokerOutage() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);

        UUID firstJobId = UUID.randomUUID();
        int partition = partition(JobRecordEncoder.key(firstJobId), 3);
        UUID candidateSecondJobId;
        do {
            candidateSecondJobId = UUID.randomUUID();
        } while (partition(JobRecordEncoder.key(candidateSecondJobId), 3) != partition);
        final UUID secondJobId = candidateSecondJobId;

        AtomicInteger firstHandlerRuns = new AtomicInteger();
        AtomicInteger secondHandlerRuns = new AtomicInteger();
        Promise<Void> firstHandler = Promise.promise();
        Vertx vertx = Vertx.vertx();
        KafkaWorker main = new KafkaWorker(vertx,
                new WorkerConfig(bootstrap, WorkerRole.MAIN, 2, 3),
                execution -> {
                    if (execution.job().jobId().equals(firstJobId)) {
                        firstHandlerRuns.incrementAndGet();
                        return firstHandler.future();
                    }
                    if (execution.job().jobId().equals(secondJobId)) {
                        secondHandlerRuns.incrementAndGet();
                    }
                    return Future.succeededFuture();
                });
        try {
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            long firstOffset;
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                firstOffset = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        partition, JobRecordEncoder.key(firstJobId),
                        JobRecordEncoder.main(job(firstJobId))))
                        .get(10, TimeUnit.SECONDS).offset();
            }
            assertEquals(0, firstOffset);
            awaitAtLeast(firstHandlerRuns, 1, 20);

            var docker = DockerClientFactory.instance().client();
            boolean brokerPaused = false;
            try {
                docker.pauseContainerCmd(KAFKA.getContainerId()).exec();
                brokerPaused = true;
                firstHandler.tryComplete();
                Thread.sleep(3_000);
            } finally {
                if (brokerPaused) {
                    docker.unpauseContainerCmd(KAFKA.getContainerId()).exec();
                }
            }

            TopicPartition sourcePartition = new TopicPartition(
                    TopicProvisioner.MAIN_TOPIC, partition);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), sourcePartition, 1);
            assertEquals(1, firstHandlerRuns.get(),
                    "broker recovery must not rerun an already completed handler");

            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                        partition, JobRecordEncoder.key(secondJobId),
                        JobRecordEncoder.main(job(secondJobId))))
                        .get(10, TimeUnit.SECONDS);
                assertEquals(1, metadata.offset());
            }
            awaitAtLeast(secondHandlerRuns, 1, 20);
            awaitCommittedAtLeast(bootstrap, WorkerRole.MAIN.groupId(), sourcePartition, 2);
            assertEquals(1, firstHandlerRuns.get());
            assertEquals(1, secondHandlerRuns.get());
        } finally {
            firstHandler.tryComplete();
            main.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static long startInterval(List<Long> starts, int index) {
        return starts.get(index + 1) - starts.get(index);
    }

    private static void awaitAtLeast(AtomicInteger value, int expected, int timeoutSeconds)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (value.get() < expected && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertTrue(value.get() >= expected,
                "timed out waiting for " + expected + "; observed " + value.get());
    }

    private static void awaitRetainedRecords(ProcessingWindow window, int expected,
                                             int timeoutSeconds) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds);
        while (window.retainedRecords() != expected && System.nanoTime() < deadline) {
            Thread.sleep(25);
        }
        assertEquals(expected, window.retainedRecords(),
                "timed out waiting for " + expected + " retained records");
    }

    private static void awaitNoActiveHandlers(AtomicInteger active) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (active.get() != 0 && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(0, active.get(), "retry worker still has active handlers");
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

    /**
     * Bounded absence check: a revoked, never-dispatched job must neither be re-published as a
     * new retry attempt nor land in the dead-letter topic. Polls briefly rather than waiting the
     * full read-expected-count timeout, since the assertion is that nothing further arrives.
     */
    private static void assertNoHandoffForJob(String bootstrap, UUID jobId) throws Exception {
        try (KafkaConsumer<byte[], byte[]> consumer =
                     new KafkaConsumer<>(consumerProperties(bootstrap, "assert-" + UUID.randomUUID()))) {
            consumer.subscribe(List.of(TopicProvisioner.RETRY_TOPIC, TopicProvisioner.DLQ_TOPIC));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
            while (System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    JsonNode node = JSON.readTree(record.value());
                    if (TopicProvisioner.DLQ_TOPIC.equals(record.topic())) {
                        assertFalse(jobId.toString().equals(node.path("jobId").asText()),
                                "the revoked rate-gate-queued job must never reach the dead-letter topic");
                    } else {
                        boolean republishedAsNextAttempt = jobId.toString()
                                .equals(node.path("job").path("jobId").asText())
                                && node.path("attempt").asInt() == 3;
                        assertFalse(republishedAsNextAttempt,
                                "the revoked rate-gate-queued job must never be re-published as "
                                        + "a new retry attempt");
                    }
                }
            }
        }
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
