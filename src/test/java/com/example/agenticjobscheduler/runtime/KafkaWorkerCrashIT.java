package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** B09/B20: a real worker process is SIGKILLed mid-handler and its work is replayed. */
@Testcontainers
class KafkaWorkerCrashIT {
    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    /** Child JVM entry point: records a marker, then never completes the handler. */
    public static final class BlockingWorkerMain {
        public static void main(String[] args) throws Exception {
            Path marker = Path.of(args[0]);
            WorkerConfig config = WorkerConfig.fromEnvironment(System.getenv());
            Vertx vertx = Vertx.vertx();
            KafkaWorker worker = new KafkaWorker(vertx, config, execution -> {
                try {
                    Files.writeString(marker, "attempt=" + execution.attempt());
                } catch (java.io.IOException failure) {
                    return Future.failedFuture(failure);
                }
                return io.vertx.core.Promise.<Void>promise().future();
            });
            worker.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            Thread.currentThread().join();
        }
    }

    @Test
    void killedWorkerProcessLeavesSourceUncommittedAndReplacementReplaysIt() throws Exception {
        String bootstrap = KAFKA.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);
        Path marker = Files.createTempFile("l06-crash", ".marker");
        Files.deleteIfExists(marker);

        UUID jobId = UUID.randomUUID();
        byte[] key = JobRecordEncoder.key(jobId);
        JobEnvelope job = new JobEnvelope(1, jobId, UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode().put("message", "crash me"), 3);
        TopicPartition source = new TopicPartition(TopicProvisioner.MAIN_TOPIC,
                org.apache.kafka.common.utils.Utils.toPositive(
                        org.apache.kafka.common.utils.Utils.murmur2(key)) % 3);

        ProcessBuilder builder = new ProcessBuilder(
                Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"),
                BlockingWorkerMain.class.getName(), marker.toString())
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.DISCARD);
        builder.environment().putAll(Map.of(
                "KAFKA_BOOTSTRAP_SERVERS", bootstrap,
                "WORKER_ROLE", "main",
                "WORKER_CONCURRENCY", "4",
                "KAFKA_TOPIC_PARTITIONS", "3"));
        Process child = builder.start();
        Vertx vertx = Vertx.vertx();
        KafkaWorker replacement = null;
        try {
            long offset;
            try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(producerProperties(bootstrap))) {
                offset = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC, key,
                        JobRecordEncoder.main(job))).get(10, TimeUnit.SECONDS).offset();
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (!Files.exists(marker) && System.nanoTime() < deadline) {
                assertTrue(child.isAlive(), "child worker exited before handling the job");
                Thread.sleep(100);
            }
            assertTrue(Files.exists(marker), "child worker never started the handler");
            assertEquals("attempt=1", Files.readString(marker));

            child.destroyForcibly().waitFor(10, TimeUnit.SECONDS);
            assertTrue(!child.isAlive());
            assertTrue(committed(bootstrap, source) <= offset,
                    "the in-flight source record must not be committed by the killed process");

            AtomicInteger replays = new AtomicInteger();
            replacement = new KafkaWorker(vertx,
                    new WorkerConfig(bootstrap, WorkerRole.MAIN, 4, 3), execution -> {
                        assertEquals(jobId, execution.job().jobId());
                        assertEquals(1, execution.attempt());
                        replays.incrementAndGet();
                        return Future.succeededFuture();
                    });
            replacement.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

            deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
            while (committed(bootstrap, source) < offset + 1 && System.nanoTime() < deadline) {
                Thread.sleep(200);
            }
            assertEquals(offset + 1, committed(bootstrap, source));
            assertEquals(1, replays.get(), "the record is replayed once after process loss");
        } finally {
            child.destroyForcibly();
            if (replacement != null) {
                replacement.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            }
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            Files.deleteIfExists(marker);
        }
    }

    private static long committed(String bootstrap, TopicPartition partition) throws Exception {
        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap))) {
            var offsets = admin.listConsumerGroupOffsets(WorkerRole.MAIN.groupId())
                    .partitionsToOffsetAndMetadata().get(10, TimeUnit.SECONDS);
            var value = offsets.get(partition);
            return value == null ? -1 : value.offset();
        }
    }

    private static Properties producerProperties(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        return properties;
    }
}
