package com.example.agenticjobscheduler.admission;

import com.example.agenticjobscheduler.runtime.TopicProvisioner;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.AdminClient;

public final class KafkaReadinessProbe implements ReadinessProbe {
    private final Vertx vertx;
    private final String bootstrapServers;
    private final int expectedPartitions;

    public KafkaReadinessProbe(Vertx vertx, HttpApiConfig config) {
        this.vertx = vertx;
        bootstrapServers = config.bootstrapServers();
        expectedPartitions = config.topicPartitions();
    }

    @Override
    public Future<Boolean> check() {
        return vertx.executeBlocking(() -> {
            Properties properties = new Properties();
            properties.put("bootstrap.servers", bootstrapServers);
            properties.put("request.timeout.ms", "3000");
            properties.put("default.api.timeout.ms", "3000");
            try (AdminClient admin = AdminClient.create(properties)) {
                var topics = admin.describeTopics(List.of(TopicProvisioner.MAIN_TOPIC))
                        .allTopicNames().get(3, TimeUnit.SECONDS);
                var topic = topics.get(TopicProvisioner.MAIN_TOPIC);
                return topic != null && topic.partitions().size() == expectedPartitions;
            } catch (RuntimeException | java.util.concurrent.ExecutionException
                    | java.util.concurrent.TimeoutException
                    | java.lang.InterruptedException failure) {
                if (failure instanceof InterruptedException) {
                    Thread.currentThread().interrupt();
                }
                return false;
            }
        }, false);
    }

    @Override
    public Future<Void> close() {
        return Future.succeededFuture();
    }
}
