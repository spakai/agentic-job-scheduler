package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.runtime.TopicProvisioner;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Vertx;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.junit.jupiter.api.Test;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class HttpAdmissionKafkaIT {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @Test
    void httpAcknowledgmentContainsTheExactValidatedMainEnvelope() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);
        HttpApiConfig config = config(bootstrap, availablePort(), 3);
        Vertx vertx = Vertx.vertx();
        HttpApiRuntime runtime = HttpApiRuntime.deploy(vertx, config)
                .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        try {
            HttpResponse<String> response = HttpClient.newHttpClient().send(
                    HttpRequest.newBuilder()
                            .uri(URI.create("http://127.0.0.1:" + config.port() + "/v1/jobs"))
                            .header("content-type", "application/json")
                            .POST(HttpRequest.BodyPublishers.ofString("""
                                    {"jobId":"e2be7c92-05f9-48d3-aeb2-4e695d857151",
                                     "executionId":"5d0d8409-4276-4ec8-8fd0-941ed977ff00",
                                     "jobType":"demo.echo","payload":{"message":"hello"},
                                     "remainingAttempts":4}
                                    """))
                            .build(),
                    HttpResponse.BodyHandlers.ofString());
            assertEquals(202, response.statusCode());
            assertEquals("accepted", JSON.readTree(response.body()).get("status").asText());

            ConsumerRecord<byte[], byte[]> record = nextMainRecord(bootstrap);
            JobEnvelope job = new EnvelopeValidator(Set.of("demo.echo"))
                    .validateMain(record.key(), record.value());
            assertEquals("e2be7c92-05f9-48d3-aeb2-4e695d857151", job.jobId().toString());
            assertEquals("5d0d8409-4276-4ec8-8fd0-941ed977ff00",
                    job.executionId().toString());
            assertEquals(4, job.remainingAttempts());
            assertEquals("hello", job.payload().get("message").asText());
            assertNotNull(record.offset());

            HttpResponse<String> ready = awaitReady(config);
            assertEquals("ready", JSON.readTree(ready.body()).get("status").asText());
        } finally {
            runtime.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void readinessIsReadOnlyAndDetectsPartitionMismatch() throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);
        HttpApiConfig config = config(bootstrap, availablePort(), 4);
        Vertx vertx = Vertx.vertx();
        KafkaReadinessProbe probe = new KafkaReadinessProbe(vertx, config);
        try {
            assertEquals(false, probe.check().toCompletionStage()
                    .toCompletableFuture().get(10, TimeUnit.SECONDS));
            assertEquals(3, topicPartitionCount(bootstrap));
        } finally {
            probe.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }

        KafkaReadinessProbe matchingProbe = new KafkaReadinessProbe(
                vertx, config(bootstrap, availablePort(), 3));
        try {
            assertEquals(true, matchingProbe.check().toCompletionStage()
                    .toCompletableFuture().get(10, TimeUnit.SECONDS));
        } finally {
            matchingProbe.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    private static ConsumerRecord<byte[], byte[]> nextMainRecord(String bootstrap) {
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "http-admission-evidence");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                ByteArrayDeserializer.class.getName());
        try (KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TopicProvisioner.MAIN_TOPIC));
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (System.nanoTime() < deadline) {
                var records = consumer.poll(Duration.ofMillis(250));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
            throw new AssertionError("No HTTP-admitted Kafka record was observed");
        }
    }

    private static int topicPartitionCount(String bootstrap) throws Exception {
        try (var admin = org.apache.kafka.clients.admin.AdminClient.create(
                Map.of("bootstrap.servers", bootstrap))) {
            return admin.describeTopics(List.of(TopicProvisioner.MAIN_TOPIC))
                    .allTopicNames().get(10, TimeUnit.SECONDS)
                    .get(TopicProvisioner.MAIN_TOPIC).partitions().size();
        }
    }

    private static HttpResponse<String> awaitReady(HttpApiConfig config) throws Exception {
        HttpClient client = HttpClient.newHttpClient();
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create("http://127.0.0.1:" + config.port() + "/health/ready"))
                .GET()
                .build();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        HttpResponse<String> latest = null;
        while (System.nanoTime() < deadline) {
            latest = client.send(request, HttpResponse.BodyHandlers.ofString());
            if (latest.statusCode() == 200) {
                return latest;
            }
            Thread.sleep(100);
        }
        assertEquals(200, latest == null ? -1 : latest.statusCode());
        return latest;
    }

    private static HttpApiConfig config(String bootstrap, int port, int partitions) {
        return new HttpApiConfig(
                bootstrap, port, 256 * 1024, 10, 10_000, 9_000, 30_000, partitions,
                Set.of("demo.echo"));
    }

    private static int availablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
