package com.example.agenticjobscheduler.runtime;

import io.vertx.core.Future;
import io.vertx.core.VerticleBase;
import java.util.Objects;

/** Owns one Kafka consumer for one worker role. */
final class ConsumerVerticle extends VerticleBase {
    private final WorkerConfig config;
    private final ExecutionVerticlePool workers;
    private KafkaWorker consumer;

    ConsumerVerticle(WorkerConfig config, ExecutionVerticlePool workers) {
        this.config = Objects.requireNonNull(config, "config");
        this.workers = Objects.requireNonNull(workers, "workers");
    }

    @Override
    public Future<?> start() {
        consumer = new KafkaWorker(vertx, config, workers);
        return consumer.start();
    }

    @Override
    public Future<?> stop() {
        return consumer.close();
    }
}
