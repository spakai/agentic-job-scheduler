package com.example.agenticjobscheduler.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class ProcessingWindowKafkaIT {
    private static final int MESSAGE_COUNT = 8;
    private static final int MAX_POLL_RECORDS = 2;

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test
    void vertxConsumerPausesWhilePollingAndResumesAfterCommitAcknowledgement() throws Exception {
        String topic = "processing-window-" + UUID.randomUUID();
        createTopic(topic);
        publishRecords(topic);

        Vertx vertx = Vertx.vertx();
        KafkaConsumer<byte[], byte[]> consumer = KafkaConsumer.create(
                vertx, consumerConfig(), new ByteArrayDeserializer(), new ByteArrayDeserializer());
        try {
            Promise<Void> completion = Promise.promise();
            vertx.runOnContext(ignored -> {
                try {
                    runPauseScenario(consumer, topic).onComplete(completion);
                } catch (RuntimeException failure) {
                    completion.fail(failure);
                }
            });
            completion.future().toCompletionStage().toCompletableFuture()
                    .get(30, TimeUnit.SECONDS);
        } finally {
            consumer.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private Future<Void> runPauseScenario(KafkaConsumer<byte[], byte[]> consumer, String topic) {
        TopicPartition partition = new TopicPartition(topic, 0);
        Set<TopicPartition> assignment = Set.of(partition);
        var window = new ProcessingWindow(6, 1_000, MAX_POLL_RECORDS, 100);
        var retained = new ArrayList<ProcessingWindow.Reservation>();
        var deliveredOffsets = new ArrayList<Long>();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);

        return consumer.assign(assignment)
                .compose(ignored -> {
                    var controller = new ProcessingWindowKafkaController(
                            Vertx.currentContext(), consumer, window);
                    return controller.partitionsAssigned(assignment)
                            .compose(assigned -> pollUntilWatermark(
                                    consumer, controller, window, retained, deliveredOffsets, deadline))
                            .compose(polled -> consumer.paused())
                            .compose(paused -> {
                                assertTrue(paused.contains(partition));
                                assertEquals(List.of(0L, 1L, 2L, 3L), deliveredOffsets);
                                return pollWhilePaused(consumer, window, 5);
                            })
                            .compose(paused -> consumer.commit(
                                    Map.of(partition, new OffsetAndMetadata(4L, ""))))
                            .compose(committed -> releaseCommitted(controller, retained))
                            .compose(released -> consumer.paused())
                            .compose(paused -> {
                                assertFalse(paused.contains(partition));
                                return pollRemainder(
                                        consumer, controller, window, deliveredOffsets, partition, deadline);
                            })
                            .map(resumed -> {
                                assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), deliveredOffsets);
                                assertEquals(0, window.retainedRecords());
                                return null;
                            });
                });
    }

    private Future<Void> pollUntilWatermark(
            KafkaConsumer<byte[], byte[]> consumer,
            ProcessingWindowKafkaController controller,
            ProcessingWindow window,
            List<ProcessingWindow.Reservation> retained,
            List<Long> offsets,
            long deadline) {
        if (window.retainedRecords() >= 4) {
            return Future.succeededFuture();
        }
        if (System.nanoTime() >= deadline) {
            return Future.failedFuture("Timed out waiting for Kafka records to reach the window watermark");
        }
        return consumer.poll(Duration.ofMillis(250)).compose(batch -> {
            if (batch.isEmpty()) {
                return pollUntilWatermark(consumer, controller, window, retained, offsets, deadline);
            }
            var reservation = controller.reserveFetched(batch.size(), collectRecords(batch, offsets))
                    .orElseThrow();
            retained.add(reservation);
            return controller.synchronize()
                    .compose(ignored -> pollUntilWatermark(
                            consumer, controller, window, retained, offsets, deadline));
        });
    }

    private Future<Void> pollWhilePaused(
            KafkaConsumer<byte[], byte[]> consumer, ProcessingWindow window, int pollsLeft) {
        if (pollsLeft == 0) {
            assertEquals(4, window.retainedRecords());
            return Future.succeededFuture();
        }
        return consumer.poll(Duration.ofMillis(200)).compose(batch -> {
            assertTrue(batch.isEmpty(), "paused Vert.x consumer must not deliver partition records");
            return pollWhilePaused(consumer, window, pollsLeft - 1);
        });
    }

    private Future<Void> pollRemainder(
            KafkaConsumer<byte[], byte[]> consumer,
            ProcessingWindowKafkaController controller,
            ProcessingWindow window,
            List<Long> offsets,
            TopicPartition partition,
            long deadline) {
        if (offsets.size() == MESSAGE_COUNT) {
            return Future.succeededFuture();
        }
        if (System.nanoTime() >= deadline) {
            return Future.failedFuture("Timed out waiting for resumed Kafka records");
        }
        return consumer.poll(Duration.ofMillis(250)).compose(batch -> {
            if (batch.isEmpty()) {
                return pollRemainder(consumer, controller, window, offsets, partition, deadline);
            }
            var reservation = controller.reserveFetched(batch.size(), collectRecords(batch, offsets))
                    .orElseThrow();
            long nextOffset = offsets.get(offsets.size() - 1) + 1;
            return consumer.commit(Map.of(partition, new OffsetAndMetadata(nextOffset, "")))
                    .compose(ignored -> controller.commitAcknowledged(reservation))
                    .compose(ignored -> pollRemainder(
                            consumer, controller, window, offsets, partition, deadline));
        });
    }

    private static long collectRecords(KafkaConsumerRecords<byte[], byte[]> batch, List<Long> offsets) {
        long bytes = 0;
        for (int index = 0; index < batch.size(); index++) {
            var record = batch.recordAt(index);
            offsets.add(record.offset());
            bytes += record.key().length + record.value().length;
        }
        return bytes;
    }

    private static Future<Void> releaseCommitted(
            ProcessingWindowKafkaController controller,
            List<ProcessingWindow.Reservation> reservations) {
        Future<Void> releases = Future.succeededFuture();
        for (var reservation : reservations) {
            releases = releases.compose(ignored -> controller.commitAcknowledged(reservation));
        }
        return releases;
    }

    private static void createTopic(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var admin = AdminClient.create(properties)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void publishRecords(String topic) throws Exception {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (var producer = new KafkaProducer<byte[], byte[]>(properties)) {
            for (int index = 0; index < MESSAGE_COUNT; index++) {
                producer.send(new ProducerRecord<>(topic, new byte[]{(byte) index}, new byte[]{1}))
                        .get(10, TimeUnit.SECONDS);
            }
        }
    }

    private Map<String, String> consumerConfig() {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "processing-window-" + UUID.randomUUID(),
                ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.MAX_POLL_RECORDS_CONFIG, Integer.toString(MAX_POLL_RECORDS));
    }
}
