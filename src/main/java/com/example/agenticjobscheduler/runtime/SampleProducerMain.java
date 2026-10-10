package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;

public final class SampleProducerMain {
    private SampleProducerMain() { }

    public static void main(String[] args) throws Exception {
        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "12"));
        TopicProvisioner.ensureTopics(bootstrap, partitions);

        var payload = JsonNodeFactory.instance.objectNode().put("message", "hello from the sample producer");
        var job = new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(),
                "demo.echo", payload, 3);
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        properties.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
        try (KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(properties)) {
            var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                    JobRecordEncoder.key(job.jobId()), JobRecordEncoder.main(job)))
                    .get(30, TimeUnit.SECONDS);
            System.out.println("Accepted jobId=" + job.jobId() + " topic=" + metadata.topic()
                    + " partition=" + metadata.partition() + " offset=" + metadata.offset());
        }
    }
}
