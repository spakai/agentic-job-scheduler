package com.example.agenticjobscheduler.execution;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
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
class ProcessingWindowKafkaIT {
    private static final int MESSAGE_COUNT = 8;
    private static final int MAX_POLL_RECORDS = 2;

    @Container
    private static final KafkaContainer KAFKA =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test
    void pausesPartitionWhilePollingAndResumesAfterCommitAcknowledgement() throws Exception {
        String topic = "processing-window-" + UUID.randomUUID();
        TopicPartition partition = new TopicPartition(topic, 0);
        createTopic(topic);
        publishRecords(topic);

        var window = new ProcessingWindow(6, 1_000, MAX_POLL_RECORDS, 100);
        var retainedBatches = new ArrayList<ProcessingWindow.Reservation>();
        var deliveredOffsets = new ArrayList<Long>();
        Properties consumerConfig = consumerConfig();
        try (var consumer = new KafkaConsumer<byte[], byte[]>(consumerConfig)) {
            consumer.subscribe(List.of(topic));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);

            while (window.retainedRecords() < 4 && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (records.isEmpty()) {
                    continue;
                }
                long bytes = 0;
                for (var record : records) {
                    deliveredOffsets.add(record.offset());
                    bytes += record.key().length + record.value().length;
                }
                retainedBatches.add(window.tryReserve(records.count(), bytes).orElseThrow());
                if (window.intakePaused()) {
                    consumer.pause(consumer.assignment());
                }
            }

            assertEquals(4, window.retainedRecords(), "window should reach its count watermark");
            assertTrue(window.intakePaused());
            assertTrue(consumer.paused().contains(partition));
            assertEquals(List.of(0L, 1L, 2L, 3L), deliveredOffsets);

            for (int poll = 0; poll < 5; poll++) {
                assertTrue(consumer.poll(Duration.ofMillis(200)).isEmpty(),
                        "paused partition must not deliver prefetched records");
            }
            assertEquals(4, window.retainedRecords(),
                    "polling while paused must not release uncommitted reservations");

            consumer.commitSync(Map.of(partition, new OffsetAndMetadata(4L)));
            for (var reservation : retainedBatches) {
                window.commitAcknowledged(reservation);
            }
            assertFalse(window.intakePaused());
            assertEquals(0, window.retainedRecords());

            consumer.resume(Set.of(partition));
            while (deliveredOffsets.size() < MESSAGE_COUNT && System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (records.isEmpty()) {
                    continue;
                }
                long bytes = 0;
                for (var record : records) {
                    deliveredOffsets.add(record.offset());
                    bytes += record.key().length + record.value().length;
                }
                var reservation = window.tryReserve(records.count(), bytes).orElseThrow();
                long nextOffset = deliveredOffsets.get(deliveredOffsets.size() - 1) + 1;
                consumer.commitSync(Map.of(partition, new OffsetAndMetadata(nextOffset)));
                window.commitAcknowledged(reservation);
            }

            assertEquals(List.of(0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L), deliveredOffsets);
            assertEquals(0, window.retainedRecords());
            assertFalse(window.intakePaused());
        }
    }

    private static void createTopic(String topic) throws Exception {
        Properties config = new Properties();
        config.put("bootstrap.servers", KAFKA.getBootstrapServers());
        try (var admin = AdminClient.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, 1, (short) 1)))
                    .all().get(10, TimeUnit.SECONDS);
        }
    }

    private static void publishRecords(String topic) throws Exception {
        Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        try (var producer = new KafkaProducer<byte[], byte[]>(producerConfig)) {
            for (int index = 0; index < MESSAGE_COUNT; index++) {
                producer.send(new ProducerRecord<>(topic, new byte[]{(byte) index}, new byte[]{1}))
                        .get(10, TimeUnit.SECONDS);
            }
        }
    }

    private Properties consumerConfig() {
        var config = new Properties();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "processing-window-" + UUID.randomUUID());
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, MAX_POLL_RECORDS);
        return config;
    }
}
