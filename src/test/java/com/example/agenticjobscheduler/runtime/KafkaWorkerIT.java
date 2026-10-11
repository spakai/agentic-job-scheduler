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
            main.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            try (KafkaProducer<byte[], byte[]> producer =
                         new KafkaProducer<>(producerProperties(bootstrap))) {
                for (int index = 0; index < 160; index++) {
                    UUID jobId = UUID.randomUUID();
                    byte[] key = JobRecordEncoder.key(jobId);
                    producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                            key, JobRecordEncoder.main(job(jobId))))
                            .get(10, TimeUnit.SECONDS);
                }
            }

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
                assertEquals(2, activeAtMeasuredStart.get(),
                    "after a slow window, the worker should stop starting above the halved limit");
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
