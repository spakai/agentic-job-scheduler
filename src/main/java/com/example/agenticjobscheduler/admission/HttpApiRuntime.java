package com.example.agenticjobscheduler.admission;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import java.util.Objects;

public final class HttpApiRuntime {
    private final Vertx vertx;
    private final String deploymentId;
    private Future<Void> closing;

    private HttpApiRuntime(Vertx vertx, String deploymentId) {
        this.vertx = vertx;
        this.deploymentId = deploymentId;
    }

    public static Future<HttpApiRuntime> deploy(Vertx vertx, HttpApiConfig config) {
        Objects.requireNonNull(vertx, "vertx");
        Objects.requireNonNull(config, "config");
        JobPublisher publisher = KafkaJobPublisher.create(vertx, config);
        ReadinessProbe readiness = new KafkaReadinessProbe(vertx, config);
        return vertx.deployVerticle(new HttpAdmissionVerticle(config, publisher, readiness))
                .map(id -> new HttpApiRuntime(vertx, id))
                .recover(failure -> Future.all(publisher.close(), readiness.close())
                        .compose(ignored -> Future.failedFuture(failure)));
    }

    public synchronized Future<Void> close() {
        if (closing == null) {
            closing = vertx.undeploy(deploymentId);
        }
        return closing;
    }
}
