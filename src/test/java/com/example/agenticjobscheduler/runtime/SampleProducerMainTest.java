package com.example.agenticjobscheduler.runtime;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleProducerMainTest {
    private static final ObjectMapper JSON = new ObjectMapper();

    @Test
    void demoJobsStartWithOneRemainingAttempt() {
        assertEquals(1, SampleProducerMain.sampleJob(false, 1, 1).remainingAttempts());
    }

    @Test
    void randomizedPartitionCountsRetain1200TotalWithoutFixedPerPartitionCounts() {
        int[] counts = new int[SampleProducerMain.DEMO_PARTITION_COUNT];
        Random random = new Random(1200);
        for (int record = 0; record < SampleProducerMain.DEMO_LOAD_COUNT; record++) {
            int partition = random.nextInt(counts.length);
            counts[partition]++;
        }
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
        boolean notAll100 = false;
        for (int count : counts) {
            if (count != 100) {
                notAll100 = true;
            }
        }
        assertTrue(notAll100);
    }

    @Test
    void transportDefaultsToDirectKafkaAndAcceptsHttp() {
        assertEquals(SampleProducerMain.Transport.DIRECT_KAFKA,
                SampleProducerMain.parseOptions(new String[] {"--load-1200"}).transport());
        assertEquals(SampleProducerMain.Transport.HTTP,
                SampleProducerMain.parseOptions(new String[] {
                        "--load-1200", "--transport", "http"
                }).transport());
        assertEquals(SampleProducerMain.Transport.HTTP,
                SampleProducerMain.parseOptions(new String[] {
                        "--transport=http"
                }).transport());
    }

    @Test
    void transportRejectsAmbiguousAndUnsupportedOptions() {
        assertThrows(IllegalArgumentException.class, () ->
                SampleProducerMain.parseOptions(new String[] {"--transport"}));
        assertThrows(IllegalArgumentException.class, () ->
                SampleProducerMain.parseOptions(new String[] {"--transport", "database"}));
        assertThrows(IllegalArgumentException.class, () ->
                SampleProducerMain.parseOptions(new String[] {"--fail", "--load-1200"}));
    }

    @Test
    void httpRequestUsesTheCanonicalAdmissionContract() throws Exception {
        var job = SampleProducerMain.sampleJob(false, 42, 1200);
        var body = JSON.readTree(SampleProducerMain.admissionRequestBody(job));

        assertEquals(job.jobId().toString(), body.get("jobId").asText());
        assertEquals(job.executionId().toString(), body.get("executionId").asText());
        assertEquals("demo.echo", body.get("jobType").asText());
        assertEquals(42, body.at("/payload/loadSequence").asInt());
        assertEquals(1, body.get("remainingAttempts").asInt());
        assertTrue(body.get("schemaVersion") == null);
    }

    @Test
    void httpPublisherPostsAndRequiresMatchingAcceptedResponse() throws Exception {
        var job = SampleProducerMain.sampleJob(false, 7, 1200);
        var requestBody = new AtomicReference<String>();
        var contentType = new AtomicReference<String>();
        var method = new AtomicReference<String>();
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/jobs", exchange -> {
            requestBody.set(new String(exchange.getRequestBody().readAllBytes(),
                    StandardCharsets.UTF_8));
            contentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
            method.set(exchange.getRequestMethod());
            respond(exchange, 202, """
                    {"status":"accepted","jobId":"%s","executionId":"%s"}
                    """.formatted(job.jobId(), job.executionId()));
        });
        server.start();
        try (HttpClient client = HttpClient.newHttpClient()) {
            SampleProducerMain.submitHttp(client, job,
                    "http://127.0.0.1:" + server.getAddress().getPort() + "/", 5_000);
        } finally {
            server.stop(0);
        }

        assertEquals("POST", method.get());
        assertEquals("application/json", contentType.get());
        var body = JSON.readTree(requestBody.get());
        assertEquals(job.jobId().toString(), body.get("jobId").asText());
        assertEquals(job.executionId().toString(), body.get("executionId").asText());
    }

    @Test
    void httpPublisherFailsOnNonAcceptedResponse() throws Exception {
        var job = SampleProducerMain.sampleJob(false, 8, 1200);
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/v1/jobs", exchange -> respond(exchange, 503,
                "{\"error\":{\"code\":\"KAFKA_UNAVAILABLE\",\"message\":\"unavailable\"}}"));
        server.start();
        try (HttpClient client = HttpClient.newHttpClient()) {
            IOException failure = assertThrows(IOException.class, () ->
                    SampleProducerMain.submitHttp(client, job,
                            "http://127.0.0.1:" + server.getAddress().getPort(), 5_000));
            assertEquals("HTTP admission failed status=503 errorCode=KAFKA_UNAVAILABLE",
                    failure.getMessage());
        } finally {
            server.stop(0);
        }
    }

    @Test
    void loadKeysRouteExactly100RecordsToEachOfTwelvePartitions() {
        int[] counts = new int[SampleProducerMain.DEMO_PARTITION_COUNT];
        for (int partition = 0; partition < counts.length; partition++) {
            for (int record = 0; record < SampleProducerMain.DEMO_RECORDS_PER_PARTITION; record++) {
                UUID jobId = SampleProducerMain.jobIdForPartition(
                        partition, SampleProducerMain.DEMO_PARTITION_COUNT);
                byte[] key = JobRecordEncoder.key(jobId);
                int routedPartition = Utils.toPositive(Utils.murmur2(key))
                        % SampleProducerMain.DEMO_PARTITION_COUNT;
                assertEquals(partition, routedPartition);
                counts[routedPartition]++;
            }
        }

        for (int count : counts) {
            assertEquals(100, count);
        }
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
    }

    private static void respond(HttpExchange exchange, int status, String body)
            throws IOException {
        byte[] response = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, response.length);
        exchange.getResponseBody().write(response);
        exchange.close();
    }
}
