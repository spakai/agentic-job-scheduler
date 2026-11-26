package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
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
    private static final ObjectMapper JSON = new ObjectMapper();

    private SampleProducerMain() { }

    public static void main(String[] args) throws Exception {
        Options options = parseOptions(args);
        if (options.transport() == Transport.HTTP) {
            if (options.loadCount() > 0) {
                publishLoadHttp(options.fail(), options.loadCount());
            } else {
                publishOneHttp(options.fail());
            }
            return;
        }

        String bootstrap = System.getenv().getOrDefault("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092");
        int partitions = Integer.parseInt(System.getenv().getOrDefault("KAFKA_TOPIC_PARTITIONS", "12"));
        if (options.loadCount() > 0 && partitions != DEMO_PARTITION_COUNT) {
            throw new IllegalArgumentException("Load demos require KAFKA_TOPIC_PARTITIONS=12");
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
            if (options.loadCount() > 0) {
                publishLoad(producer, options.fail(), options.loadCount());
            } else {
                publishOne(producer, options.fail());
            }
        }
    }

    static Options parseOptions(String[] args) {
        boolean fail = false;
        int loadCount = 0;
        Transport transport = Transport.DIRECT_KAFKA;
        for (int index = 0; index < args.length; index++) {
            String argument = args[index];
            switch (argument) {
                case "--fail" -> fail = true;
                case "--load-1200" -> {
                    if (loadCount != 0) {
                        throw new IllegalArgumentException("Specify only one load option");
                    }
                    loadCount = DEMO_LOAD_COUNT;
                }
                case "--load-count" -> {
                    if (loadCount != 0) {
                        throw new IllegalArgumentException("Specify only one load option");
                    }
                    if (index + 1 >= args.length) {
                        throw new IllegalArgumentException("--load-count requires a value");
                    }
                    loadCount = parseLoadCount(args[++index]);
                }
                case "--transport" -> {
                    if (index + 1 >= args.length) {
                        throw new IllegalArgumentException("--transport requires a value");
                    }
                    transport = parseTransport(args[++index]);
                }
                default -> {
                    if (argument.startsWith("--transport=")) {
                        transport = parseTransport(argument.substring("--transport=".length()));
                    } else {
                        throw new IllegalArgumentException("Usage: sample-producer "
                                + "[--fail | --load-1200 | --load-count 1..1200] "
                                + "[--transport direct-kafka|http]");
                    }
                }
            }
        }
        if (fail && loadCount > 0) {
            throw new IllegalArgumentException("--fail and load options are mutually exclusive");
        }
        return new Options(fail, loadCount, transport);
    }

    private static int parseLoadCount(String value) {
        try {
            int count = Integer.parseInt(value);
            if (count < 1 || count > DEMO_LOAD_COUNT) {
                throw new IllegalArgumentException("--load-count must be between 1 and "
                        + DEMO_LOAD_COUNT);
            }
            return count;
        } catch (NumberFormatException failure) {
            throw new IllegalArgumentException("--load-count must be an integer", failure);
        }
    }

    private static Transport parseTransport(String value) {
        return switch (value) {
            case "direct-kafka" -> Transport.DIRECT_KAFKA;
            case "http" -> Transport.HTTP;
            default -> throw new IllegalArgumentException(
                    "Unsupported transport " + value + "; expected direct-kafka or http");
        };
    }

    private static void publishOne(KafkaProducer<byte[], byte[]> producer, boolean fail) throws Exception {
        var job = sampleJob(fail, 1, 1);
        var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                JobRecordEncoder.key(job.jobId()), JobRecordEncoder.main(job)))
                .get(30, TimeUnit.SECONDS);
        System.out.println("Accepted jobId=" + job.jobId() + " topic=" + metadata.topic()
                + " partition=" + metadata.partition() + " offset=" + metadata.offset());
    }

    private static void publishLoad(KafkaProducer<byte[], byte[]> producer,
            boolean fail, int loadCount) throws Exception {
        AtomicInteger sequence = new AtomicInteger();
        Map<Integer, Integer> acceptedByPartition = new LinkedHashMap<>();
        Map<Integer, Long> firstOffsetByPartition = new LinkedHashMap<>();
        Map<Integer, Long> lastOffsetByPartition = new LinkedHashMap<>();
        for (int partition = 0; partition < DEMO_PARTITION_COUNT; partition++) {
            acceptedByPartition.put(partition, 0);
        }
        for (int partition : loadPartitionOrder(loadCount)) {
            UUID jobId = jobIdForPartition(partition, DEMO_PARTITION_COUNT);
            var job = sampleJob(fail,
                    sequence.incrementAndGet(),
                    loadCount);
            job = new JobEnvelope(job.schemaVersion(), jobId, job.executionId(),
                    job.jobType(), job.payload(), job.remainingAttempts());
            var metadata = producer.send(new ProducerRecord<>(TopicProvisioner.MAIN_TOPIC,
                    partition, JobRecordEncoder.key(job.jobId()), JobRecordEncoder.main(job)))
                    .get(30, TimeUnit.SECONDS);
            acceptedByPartition.put(partition, acceptedByPartition.get(partition) + 1);
            firstOffsetByPartition.putIfAbsent(partition, metadata.offset());
            lastOffsetByPartition.put(partition, metadata.offset());
            if (sequence.get() % 100 == 0 || sequence.get() == loadCount) {
                System.out.println("Load progress accepted=" + sequence.get()
                        + " total=" + loadCount);
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

    private static void publishOneHttp(boolean fail) throws Exception {
        JobEnvelope job = sampleJob(fail, 1, 1);
        try (HttpClient client = HttpClient.newHttpClient()) {
            submitHttp(client, job);
        }
        System.out.println("Accepted jobId=" + job.jobId()
                + " transport=http status=202");
    }

    private static void publishLoadHttp(boolean fail, int loadCount) throws Exception {
        AtomicInteger sequence = new AtomicInteger();
        Map<Integer, Integer> acceptedByPartition = new LinkedHashMap<>();
        for (int partition = 0; partition < DEMO_PARTITION_COUNT; partition++) {
            acceptedByPartition.put(partition, 0);
        }
        try (HttpClient client = HttpClient.newHttpClient()) {
            for (int partition : loadPartitionOrder(loadCount)) {
                UUID jobId = jobIdForPartition(partition, DEMO_PARTITION_COUNT);
                JobEnvelope job = sampleJob(fail,
                        sequence.incrementAndGet(),
                        loadCount);
                job = new JobEnvelope(job.schemaVersion(), jobId, job.executionId(),
                        job.jobType(), job.payload(), job.remainingAttempts());
                submitHttp(client, job);
                acceptedByPartition.put(partition, acceptedByPartition.get(partition) + 1);
                if (sequence.get() % 100 == 0 || sequence.get() == loadCount) {
                    System.out.println("Load progress accepted=" + sequence.get()
                            + " total=" + loadCount + " transport=http");
                }
            }
        }
        System.out.println("Load complete accepted=" + sequence.get()
                + " transport=http topic=" + TopicProvisioner.MAIN_TOPIC
                + " partitionCounts=" + acceptedByPartition);
    }

    private static void submitHttp(HttpClient client, JobEnvelope job)
            throws IOException, InterruptedException {
        String baseUrl = System.getenv().getOrDefault(
                "DEMO_HTTP_BASE_URL", "http://http-api:8080");
        long timeoutMillis = Long.parseLong(System.getenv().getOrDefault(
                "DEMO_HTTP_TIMEOUT_MILLIS", "15000"));
        submitHttp(client, job, baseUrl, timeoutMillis);
    }

    static void submitHttp(HttpClient client, JobEnvelope job,
            String baseUrl, long timeoutMillis) throws IOException, InterruptedException {
        String normalizedBaseUrl = baseUrl.endsWith("/")
                ? baseUrl.substring(0, baseUrl.length() - 1)
                : baseUrl;
        HttpRequest request = HttpRequest.newBuilder()
                .uri(URI.create(normalizedBaseUrl + "/v1/jobs"))
                .timeout(Duration.ofMillis(timeoutMillis))
                .header("content-type", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(admissionRequestBody(job)))
                .build();
        HttpResponse<String> response = client.send(
                request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() != 202) {
            throw new IOException("HTTP admission failed status=" + response.statusCode()
                    + " errorCode=" + responseErrorCode(response.body()));
        }
        var responseBody = JSON.readTree(response.body());
        if (!"accepted".equals(responseBody.path("status").asText())
                || !job.jobId().toString().equals(responseBody.path("jobId").asText())
                || !job.executionId().toString().equals(
                        responseBody.path("executionId").asText())) {
            throw new IOException("HTTP admission returned an inconsistent 202 response");
        }
    }

    static String admissionRequestBody(JobEnvelope job) {
        var request = JsonNodeFactory.instance.objectNode()
                .put("jobId", job.jobId().toString())
                .put("executionId", job.executionId().toString())
                .put("jobType", job.jobType());
        request.set("payload", job.payload());
        request.put("remainingAttempts", job.remainingAttempts());
        try {
            return JSON.writeValueAsString(request);
        } catch (IOException exception) {
            throw new IllegalStateException("Could not encode HTTP admission request", exception);
        }
    }

    private static String responseErrorCode(String responseBody) {
        try {
            return JSON.readTree(responseBody).path("error").path("code").asText("none");
        } catch (IOException ignored) {
            return "unparseable";
        }
    }

    static List<Integer> loadPartitionOrder(int loadCount) {
        if (loadCount < 1 || loadCount > DEMO_LOAD_COUNT) {
            throw new IllegalArgumentException("Load count must be between 1 and "
                    + DEMO_LOAD_COUNT);
        }
        List<Integer> sendOrder = new ArrayList<>(loadCount);
        Random random = new Random(Long.parseLong(System.getenv().getOrDefault(
                "DEMO_RANDOM_SEED", "1200")));
        for (int record = 0; record < loadCount; record++) {
            sendOrder.add(random.nextInt(DEMO_PARTITION_COUNT));
        }
        return sendOrder;
    }

    static JobEnvelope sampleJob(boolean fail, int sequence, int total) {
        long demoDelayMillis = fail ? 0
                : Long.parseLong(System.getenv().getOrDefault(
                        "DEMO_HANDLER_DELAY_MILLIS", "100"));
        String demoRunId = System.getenv().getOrDefault(
                "DEMO_RUN_ID", UUID.randomUUID().toString());
        String externalResponse = externalResponse(sequence, System.getenv().getOrDefault(
                "DEMO_EXTERNAL_RESPONSE", "none"));
        var payload = JsonNodeFactory.instance.objectNode()
                .put("message", "hello from the sample producer")
                .put("loadSequence", sequence)
                .put("loadTotal", total)
                .put("loadMode", fail ? "failure" : "success")
                .put("demoRunId", demoRunId)
                .put("demoDelayMillis", demoDelayMillis)
                .put("demoExternalResponse", externalResponse);
        if (fail) {
            payload.put("demoFail", true);
        }
        return new JobEnvelope(1, UUID.randomUUID(), UUID.randomUUID(),
                "demo.echo", payload, 1);
    }

    static String externalResponse(int sequence, String configured) {
        return switch (configured) {
            case "none" -> "none";
            case "rest-503", "rest-timeout", "database-timeout" -> configured;
            case "mixed" -> switch (Math.floorMod(sequence - 1, 3)) {
                case 0 -> "rest-503";
                case 1 -> "rest-timeout";
                default -> "database-timeout";
            };
            default -> throw new IllegalArgumentException(
                    "Unsupported DEMO_EXTERNAL_RESPONSE " + configured);
        };
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

    enum Transport {
        DIRECT_KAFKA,
        HTTP
    }

    record Options(boolean fail, int loadCount, Transport transport) {
    }
}
