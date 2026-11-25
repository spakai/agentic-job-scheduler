package com.example.agenticjobscheduler.admission;

import com.example.agenticjobscheduler.runtime.TopicProvisioner;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

public final class KafkaJobPublisher implements JobPublisher {
    private final KafkaProducer<byte[], byte[]> producer;
    private final AtomicBoolean ready = new AtomicBoolean(false);

    KafkaJobPublisher(KafkaProducer<byte[], byte[]> producer) {
        this.producer = Objects.requireNonNull(producer, "producer");
    }

    public static KafkaJobPublisher create(Vertx vertx, HttpApiConfig config) {
        Map<String, String> properties = Map.ofEntries(
                Map.entry("bootstrap.servers", config.bootstrapServers()),
                Map.entry("key.serializer",
                        "org.apache.kafka.common.serialization.ByteArraySerializer"),
                Map.entry("value.serializer",
                        "org.apache.kafka.common.serialization.ByteArraySerializer"),
                Map.entry("acks", "all"),
                Map.entry("enable.idempotence", "true"),
                Map.entry("retries", Integer.toString(Integer.MAX_VALUE)),
                Map.entry("linger.ms", "0"),
                Map.entry("request.timeout.ms", Long.toString(
                        Math.max(1, config.deliveryTimeoutMillis() - 1_000))),
                Map.entry("delivery.timeout.ms",
                        Long.toString(config.deliveryTimeoutMillis())),
                Map.entry("max.block.ms", Long.toString(config.deliveryTimeoutMillis())),
                Map.entry("max.in.flight.requests.per.connection", "5"));
        return new KafkaJobPublisher(KafkaProducer.create(vertx, properties,
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer()));
    }

    @Override
    public Future<AdmissionAcknowledgment> publish(AdmissionEnvelope envelope) {
        return producer.send(KafkaProducerRecord.create(
                        TopicProvisioner.MAIN_TOPIC, envelope.key(), envelope.value()))
                .map(metadata -> {
                    ready.set(true);
                    return new AdmissionAcknowledgment(metadata.getPartition(), metadata.getOffset());
                })
                .onFailure(ignored -> ready.set(false));
    }

    @Override
    public boolean ready() {
        return ready.get();
    }

    @Override
    public Future<Void> close() {
        return producer.close();
    }
}
