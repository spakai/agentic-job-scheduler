package com.example.agenticjobscheduler.runtime;

import io.vertx.core.Vertx;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class WorkerMain {
    private static final Logger LOG = Logger.getLogger(WorkerMain.class.getName());

    private WorkerMain() { }

    public static void main(String[] args) throws Exception {
        WorkerConfig config = WorkerConfig.fromEnvironment(System.getenv());
        TopicProvisioner.ensureTopics(config.bootstrapServers(), config.topicPartitions());
        Vertx vertx = Vertx.vertx();
        KafkaWorker worker = new KafkaWorker(vertx, config, new DemoJobHandler());
        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                worker.close().toCompletionStage().toCompletableFuture()
                        .get(35, TimeUnit.SECONDS);
            } catch (Exception failure) {
                LOG.log(Level.SEVERE, "Worker did not close cleanly", failure);
            } finally {
                vertx.close().toCompletionStage().toCompletableFuture()
                        .orTimeout(10, TimeUnit.SECONDS).exceptionally(failure -> {
                            LOG.log(Level.SEVERE, "Vert.x did not close cleanly", failure);
                            return null;
                        }).join();
                stopped.countDown();
            }
        }, "worker-shutdown"));
        worker.start().toCompletionStage().toCompletableFuture()
                .get(30, TimeUnit.SECONDS);
        LOG.info(() -> "Started " + config.role() + " worker, group="
                + config.role().groupId() + ", concurrency=" + config.concurrency());
        stopped.await();
    }
}
