package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.net.ServerSocket;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HttpAdmissionVerticleTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private Vertx vertx;
    private HttpClient client;

    @BeforeEach
    void setUp() {
        vertx = Vertx.vertx();
        client = HttpClient.newHttpClient();
    }

    @AfterEach
    void tearDown() throws Exception {
        vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
    }

    @Test
    void acceptsAValidRequestOnlyAfterPublisherAcknowledges() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher();
        HttpApiConfig config = config(availablePort(), 10);
        deploy(config, publisher);

        HttpResponse<String> response = post(config, """
                {"jobType":"demo.echo","payload":{"message":"hello"}}
                """);
        JsonNode body = JSON.readTree(response.body());

        assertEquals(202, response.statusCode());
        assertEquals("accepted", body.get("status").asText());
        assertNotNull(publisher.envelope.get());
        assertEquals(body.get("jobId").asText(), publisher.envelope.get().jobId().toString());
        assertEquals(1, publisher.calls.get());
    }

    @Test
    void rejectsInvalidMediaAndUnknownFieldsWithoutPublishing() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher();
        HttpApiConfig config = config(availablePort(), 10);
        deploy(config, publisher);

        HttpResponse<String> media = post(config, "{}", "text/plain");
        HttpResponse<String> unknown = post(config, """
                {"jobType":"demo.echo","payload":{},"schemaVersion":1}
                """);

        assertEquals(415, media.statusCode());
        assertEquals(400, unknown.statusCode());
        assertNull(publisher.envelope.get());
        assertEquals(0, publisher.calls.get());
    }

    @Test
    void reportsAmbiguousAcknowledgmentAndCapacityFailure() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher();
        publisher.block = true;
        HttpApiConfig config = config(availablePort(), 1);
        deploy(config, publisher);

        var first = client.sendAsync(request(config, """
                {"jobType":"demo.echo","payload":{}}
                """), HttpResponse.BodyHandlers.ofString());
        Thread.sleep(50);
        HttpResponse<String> overloaded = post(config, """
                {"jobType":"demo.echo","payload":{}}
                """);
        HttpResponse<String> ambiguous = first.get(5, TimeUnit.SECONDS);
        HttpResponse<String> capacityRecovered = post(config, """
                {"jobType":"demo.echo","payload":{}}
                """);

        assertEquals(429, overloaded.statusCode());
        assertEquals(503, ambiguous.statusCode());
        assertEquals(503, capacityRecovered.statusCode());
        assertEquals(2, publisher.calls.get());
    }

    @Test
    void exposesHealthAndMetricsWithoutPayloadLabels() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher();
        HttpApiConfig config = config(availablePort(), 10);
        deploy(config, publisher);

        HttpResponse<String> live = client.send(HttpRequest.newBuilder()
                .uri(URI.create(base(config) + "/health/live")).GET().build(),
                HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> metrics = client.send(HttpRequest.newBuilder()
                .uri(URI.create(base(config) + "/metrics")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(200, live.statusCode());
        assertEquals(200, metrics.statusCode());
        assertEquals(true, metrics.body().contains("http_admission_active_in_flight"));
        assertEquals(false, metrics.body().contains("jobId"));
    }

    @Test
    void readinessRequiresAHealthyProducer() throws Exception {
        CapturingPublisher publisher = new CapturingPublisher();
        publisher.ready = false;
        HttpApiConfig config = config(availablePort(), 10);
        deploy(config, publisher);

        HttpResponse<String> ready = client.send(HttpRequest.newBuilder()
                        .uri(URI.create(base(config) + "/health/ready")).GET().build(),
                HttpResponse.BodyHandlers.ofString());

        assertEquals(503, ready.statusCode());
    }

    private void deploy(HttpApiConfig config, CapturingPublisher publisher) throws Exception {
        ReadinessProbe readiness = new ReadinessProbe() {
            @Override
            public Future<Boolean> check() {
                return Future.succeededFuture(true);
            }

            @Override
            public Future<Void> close() {
                return Future.succeededFuture();
            }
        };
        vertx.deployVerticle(new HttpAdmissionVerticle(config, publisher, readiness))
                .toCompletionStage().toCompletableFuture().get(5, TimeUnit.SECONDS);
    }

    private HttpResponse<String> post(HttpApiConfig config, String body) throws Exception {
        return post(config, body, "application/json");
    }

    private HttpResponse<String> post(HttpApiConfig config, String body, String contentType)
            throws Exception {
        return client.send(request(config, body, contentType), HttpResponse.BodyHandlers.ofString());
    }

    private HttpRequest request(HttpApiConfig config, String body) {
        return request(config, body, "application/json");
    }

    private HttpRequest request(HttpApiConfig config, String body, String contentType) {
        return HttpRequest.newBuilder()
                .uri(URI.create(base(config) + "/v1/jobs"))
                .timeout(Duration.ofSeconds(5))
                .header("content-type", contentType)
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();
    }

    private static String base(HttpApiConfig config) {
        return "http://127.0.0.1:" + config.port();
    }

    private static HttpApiConfig config(int port, int maxInFlight) {
        return new HttpApiConfig(
                "localhost:9092", port, 256 * 1024, maxInFlight, 1_100, 1_000, 5_000, 12,
                Set.of("demo.echo"));
    }

    private static int availablePort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static final class CapturingPublisher implements JobPublisher {
        private final AtomicReference<AdmissionEnvelope> envelope = new AtomicReference<>();
        private final AtomicInteger calls = new AtomicInteger();
        private final Promise<AdmissionAcknowledgment> blockedSend = Promise.promise();
        private boolean block;
        private boolean ready = true;

        @Override
        public Future<AdmissionAcknowledgment> publish(AdmissionEnvelope candidate) {
            calls.incrementAndGet();
            envelope.set(candidate);
            return block
                    ? blockedSend.future()
                    : Future.succeededFuture(new AdmissionAcknowledgment(0, 1));
        }

        @Override
        public boolean ready() {
            return ready;
        }

        @Override
        public Future<Void> close() {
            blockedSend.tryComplete(new AdmissionAcknowledgment(0, 1));
            return Future.succeededFuture();
        }
    }
}
