package com.example.agenticjobscheduler.runtime;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewPartitions;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.common.errors.TopicExistsException;

/** Idempotently creates the three application topics before a worker starts. */
public final class TopicProvisioner {
    public static final String MAIN_TOPIC = "jobs.main.v1";
    public static final String RETRY_TOPIC = "jobs.retry.v1";
    public static final String DLQ_TOPIC = "jobs.dlq.v1";

    private TopicProvisioner() { }

    public static void ensureTopics(String bootstrapServers, int partitions) throws Exception {
        if (bootstrapServers == null || bootstrapServers.isBlank() || partitions < 1) {
            throw new IllegalArgumentException("Invalid Kafka topic configuration");
        }
        Properties properties = new Properties();
        properties.put("bootstrap.servers", bootstrapServers);
        try (AdminClient admin = AdminClient.create(properties)) {
            List<String> names = List.of(MAIN_TOPIC, RETRY_TOPIC, DLQ_TOPIC);
            var create = List.of(
                    new NewTopic(MAIN_TOPIC, partitions, (short) 1)
                            .configs(Map.of("max.message.bytes", "1048576")),
                    new NewTopic(RETRY_TOPIC, partitions, (short) 1)
                            .configs(Map.of("max.message.bytes", "1048576")),
                    new NewTopic(DLQ_TOPIC, 3, (short) 1)
                            .configs(Map.of("max.message.bytes", "1048576")));
            try {
                admin.createTopics(create).all().get(Duration.ofSeconds(30).toMillis(),
                        java.util.concurrent.TimeUnit.MILLISECONDS);
            } catch (ExecutionException failure) {
                if (!(failure.getCause() instanceof TopicExistsException)) {
                    throw failure;
                }
            }

            var descriptions = admin.describeTopics(names).allTopicNames()
                    .get(Duration.ofSeconds(30).toMillis(),
                            java.util.concurrent.TimeUnit.MILLISECONDS);
            for (String name : names) {
                int existing = descriptions.get(name).partitions().size();
                int required = name.equals(DLQ_TOPIC) ? 3 : partitions;
                if (existing < required) {
                    admin.createPartitions(Map.of(name, NewPartitions.increaseTo(required)))
                            .all().get(Duration.ofSeconds(30).toMillis(),
                                    java.util.concurrent.TimeUnit.MILLISECONDS);
                }
            }
        }
    }
}
