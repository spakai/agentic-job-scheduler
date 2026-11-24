package com.example.agenticjobscheduler.admission;

import io.vertx.core.Vertx;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HttpApiMain {
    private static final Logger LOG = Logger.getLogger(HttpApiMain.class.getName());

    private HttpApiMain() { }

    public static void main(String[] args) throws Exception {
        HttpApiConfig config = HttpApiConfig.fromEnvironment(System.getenv());
        Vertx vertx = Vertx.vertx();
        HttpApiRuntime runtime;
        try {
            runtime = HttpApiRuntime.deploy(vertx, config)
                    .toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        } catch (Exception failure) {
            vertx.close().toCompletionStage().toCompletableFuture()
                    .orTimeout(10, TimeUnit.SECONDS).exceptionally(closeFailure -> {
                        failure.addSuppressed(closeFailure);
                        return null;
                    }).join();
            throw failure;
        }

        CountDownLatch stopped = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                runtime.close().toCompletionStage().toCompletableFuture()
                        .get(config.gracefulShutdownMillis() + 5_000, TimeUnit.MILLISECONDS);
            } catch (Exception failure) {
                LOG.log(Level.SEVERE, "HTTP admission runtime did not close cleanly", failure);
            } finally {
                vertx.close().toCompletionStage().toCompletableFuture()
                        .orTimeout(10, TimeUnit.SECONDS).exceptionally(failure -> null).join();
                stopped.countDown();
            }
        }, "http-admission-shutdown"));
        LOG.info(() -> "Started HTTP admission API on port " + config.port());
        stopped.await();
    }
}
