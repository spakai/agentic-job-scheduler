package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;

public final class SampleProducerMain {
    static final int DEMO_PARTITION_COUNT = 12;
    static final int DEMO_LOAD_COUNT = 1_200;
    static final int DEMO_RECORDS_PER_PARTITION = DEMO_LOAD_COUNT / DEMO_PARTITION_COUNT;

    private SampleProducerMain() { }

    public static void main(String[] args) throws Exception {
        if (args.length > 1 || (args.length == 1
                && !args[0].equals("--fail")
                && !args[0].equals("--load-1200"))) {
            throw new IllegalArgumentException("Usage: sample-producer [--fail | --load-1200]");
        }
        boolean fail = args.length == 1 && args[0].equals("--fail");
        boolean load1200 = args.length == 1 && args[0].equals("--load-1200");
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "12"));
        if (load1200 && partitions != DEMO_PARTITION_COUNT) {
            throw new IllegalArgumentException("--load-1200 requires KAFKA_TOPIC_PARTITIONS=12");
        }
        TopicProvisioner.ensureTopics(bootstrap, partitions);

        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            if (load1200) {
                publishLoad(producer, fail);
            } else {
                publishOne(producer, fail);
            }
        }
    }

    private static void publishOne(KafkaProducer<byte[], byte[]> producer, boolean fail) throws Exception {
        var job = sampleJob(fail, 1, 1);
        var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                JobRecordEncoder.key(job.jobId()), JobRecordEncoder.main(job)))
                .get(30, TimeUnit.SECONDS);
        System.out.println("Accepted jobId=" + job.jobId() + " topic=" + metadata.topic()
                + " partition=" + metadata.partition() + " offset=" + metadata.offset());
    }

    private static void publishLoad(KafkaProducer<byte[], byte[]> producer, boolean fail) throws Exception {
        AtomicInteger sequence = new AtomicInteger();
        Map<Integer, Integer> acceptedByPartition = new LinkedHashMap<>();
        Map<Integer, Long> firstOffsetByPartition = new LinkedHashMap<>();
        Map<Integer, Long> lastOffsetByPartition = new LinkedHashMap<>();
        List<Integer> sendOrder = new ArrayList<>(DEMO_LOAD_COUNT);
        for (int partition = 0; partition < DEMO_PARTITION_COUNT; partition++) {
            acceptedByPartition.put(partition, 0);
        }
        Random random = new Random(Long.parseLong(System.getenv().getOrDefault(
                "DEMO_RANDOM_SEED", "1200")));
        for (int record = 0; record < DEMO_LOAD_COUNT; record++) {
            sendOrder.add(random.nextInt(DEMO_PARTITION_COUNT));
        }
        for (int partition : sendOrder) {
            UUID jobId = jobIdForPartition(partition, DEMO_PARTITION_COUNT);
            var job = sampleJob(fail,
                    sequence.incrementAndGet(),
                    DEMO_LOAD_COUNT);
            job = new JobEnvelope(job.schemaVersion(), jobId, job.executionId(),
                    job.jobType(), job.payload(), job.remainingAttempts());
            var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                    partition, JobRecordEncoder.key(job.jobId()), JobRecordEncoder.main(job)))
                    .get(30, TimeUnit.SECONDS);
            acceptedByPartition.put(partition, acceptedByPartition.get(partition) + 1);
            firstOffsetByPartition.putIfAbsent(partition, metadata.offset());
            lastOffsetByPartition.put(partition, metadata.offset());
            if (sequence.get() % 100 == 0) {
                System.out.println("Load progress accepted=" + sequence.get()
                        + " total=" + DEMO_LOAD_COUNT);
            }
        }
        System.out.println("Load complete accepted=" + sequence.get()
                + " topic=" + TopicProvisioner.MAIN_TOPIC
                + " partitionCounts=" + acceptedByPartition);
        for (int partition = 0; partition < DEMO_PARTITION_COUNT; partition++) {
            System.out.println("Partition " + partition
                    + " count=" + acceptedByPartition.get(partition)
                    + " firstOffset=" + firstOffsetByPartition.get(partition)
                    + " lastOffset=" + lastOffsetByPartition.get(partition));
        }
    }

    private static JobEnvelope sampleJob(boolean fail, int sequence, int total) {
        long demoDelayMillis = fail ? 0
                : Long.parseLong(System.getenv().getOrDefault(
                        "DEMO_HANDLER_DELAY_MILLIS", "100"));
        String demoRunId = System.getenv().getOrDefault(
                "DEMO_RUN_ID", UUID.randomUUID().toString());
        var payload = JsonNodeFactory.instance.objectNode()
                .put("message", "hello from the sample producer")
                .put("loadSequence", sequence)
                .put("loadTotal", total)
                .put("loadMode", fail ? "failure" : "success")
                .put("demoRunId", demoRunId)
                .put("demoDelayMillis", demoDelayMillis);
        if (fail) {
            payload.put("demoFail", true);
        }
        return new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(),
                "demo.echo", payload, 3);
    }

    static UUID jobIdForPartition(int expectedPartition, int partitionCount) {
        if (expectedPartition < 0 || expectedPartition >= partitionCount) {
            throw new IllegalArgumentException("Invalid demo partition");
        }
        for (int attempt = 0; attempt < 100_000; attempt++) {
            UUID jobId = UUID.randomUUID();
            byte[] key = JobRecordEncoder.key(jobId);
            int actualPartition = org.apache.kafka.common.utils.Utils.toPositive(
                    Utils.murmur2(key)) % partitionCount;
            if (actualPartition == expectedPartition) {
                return jobId;
            }
        }
        throw new IllegalStateException("Could not find a jobId for partition "
                + expectedPartition);
    }
}
