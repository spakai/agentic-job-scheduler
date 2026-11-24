package com.example.agenticjobscheduler.admission;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.http.HttpServer;
import io.vertx.core.http.HttpServerOptions;
import io.vertx.core.http.HttpServerRequest;
import io.vertx.core.http.HttpServerResponse;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

public final class HttpAdmissionVerticle extends AbstractVerticle {
    private static final Logger LOG = Logger.getLogger(HttpAdmissionVerticle.class.getName());
    private static final ObjectMapper JSON = new ObjectMapper();

    private final HttpApiConfig config;
    private final AdmissionEnvelopeMapper mapper;
    private final JobPublisher publisher;
    private final ReadinessProbe readiness;
    private final AdmissionMetrics metrics;
    private final AdmissionLimiter limiter;
    private HttpServer server;
    private volatile boolean ready;

    public HttpAdmissionVerticle(HttpApiConfig config, JobPublisher publisher,
                                 ReadinessProbe readiness) {
        this.config = Objects.requireNonNull(config, "config");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        this.readiness = Objects.requireNonNull(readiness, "readiness");
        this.mapper = new AdmissionEnvelopeMapper(
                new com.example.agenticjobscheduler.messaging.EnvelopeValidator(
                        config.registeredJobTypes()));
        this.metrics = new AdmissionMetrics();
        this.limiter = new AdmissionLimiter(config.maxInFlight());
    }

    @Override
    public void start(Promise<Void> startPromise) {
        server = vertx.createHttpServer(new HttpServerOptions()
                        .setHost("0.0.0.0")
                        .setPort(config.port()))
                .requestHandler(this::handle);
        server.listen(config.port())
                .<Void>mapEmpty()
                .onComplete(startPromise);
    }

    @Override
    public void stop(Promise<Void> stopPromise) {
        limiter.stopAdmission();
        Future<Void> serverClose = server == null
                ? Future.succeededFuture()
                : server.shutdown(Duration.ofMillis(config.gracefulShutdownMillis()));
        serverClose.eventually(publisher::close)
                .eventually(readiness::close)
                .onComplete(stopPromise);
    }

    private void handle(HttpServerRequest request) {
        if (request.method() == HttpMethod.GET && "/health/live".equals(request.path())) {
            json(request.response(), 200, JSON.createObjectNode().put("status", "live"));
            return;
        }
        if (request.method() == HttpMethod.GET && "/health/ready".equals(request.path())) {
            readiness.check().onSuccess(isReady -> {
                boolean producerReady = publisher.ready();
                ready = isReady && producerReady;
                json(request.response(), ready ? 200 : 503,
                        JSON.createObjectNode().put("status", ready ? "ready" : "unavailable"));
            });
            return;
        }
        if (request.method() == HttpMethod.GET && "/metrics".equals(request.path())) {
            request.response()
                    .putHeader("content-type", "text/plain; version=0.0.4; charset=utf-8")
                    .end(metrics.prometheus(limiter.active(), ready));
            return;
        }
        if (!"/v1/jobs".equals(request.path())) {
            error(request.response(), 404, "NOT_FOUND", "Endpoint not found");
            return;
        }
        if (request.method() != HttpMethod.POST) {
            error(request.response(), 405, "METHOD_NOT_ALLOWED", "POST is required");
            return;
        }
        submit(request);
    }

    private void submit(HttpServerRequest request) {
        metrics.request();
        AdmissionLimiter.Permit permit = limiter.tryAcquire();
        if (permit == null) {
            metrics.overloaded();
            drainAndReject(request, 429, "ADMISSION_OVERLOADED", "Admission capacity is full");
            return;
        }

        String contentType = request.getHeader("content-type");
        if (contentType == null || !contentType.split(";", 2)[0].trim()
                .equalsIgnoreCase("application/json")) {
            permit.release();
            metrics.invalid();
            drainAndReject(request, 415, "UNSUPPORTED_MEDIA_TYPE",
                    "Content-Type must be application/json");
            return;
        }

        Buffer body = Buffer.buffer();
        AtomicBoolean rejected = new AtomicBoolean();
        long readTimer = vertx.setTimer(config.acknowledgmentDeadlineMillis(), ignored -> {
            if (rejected.compareAndSet(false, true)) {
                permit.release();
                metrics.invalid();
                error(request.response(), 408, "REQUEST_TIMEOUT",
                        "Request body was not received in time");
            }
        });
        request.handler(chunk -> {
            if (rejected.get()) {
                return;
            }
            if ((long) body.length() + chunk.length() > config.maxBodyBytes()) {
                rejected.set(true);
                vertx.cancelTimer(readTimer);
                permit.release();
                metrics.invalid();
                error(request.response(), 413, "REQUEST_TOO_LARGE",
                        "Request body is too large");
                return;
            }
            body.appendBuffer(chunk);
        });
        request.endHandler(ignored -> {
            vertx.cancelTimer(readTimer);
            if (rejected.get()) {
                return;
            }
            process(request, body, permit);
        });
        request.exceptionHandler(failure -> {
            if (rejected.compareAndSet(false, true)) {
                vertx.cancelTimer(readTimer);
                permit.release();
                metrics.unexpected();
                error(request.response(), 500, "INTERNAL_ERROR", "Request could not be read");
            }
        });
    }

    private void process(HttpServerRequest request, Buffer body, AdmissionLimiter.Permit permit) {
        long validationStart = System.nanoTime();
        AdmissionEnvelope envelope;
        try {
            AdmissionRequest decoded = AdmissionRequestDecoder.decode(body.getBytes());
            envelope = mapper.map(decoded);
            metrics.validationLatency(System.nanoTime() - validationStart);
            metrics.bytes((long) body.length() + envelope.value().length);
        } catch (AdmissionException failure) {
            permit.release();
            metrics.invalid();
            error(request.response(), failure.status(), failure.code(), failure.getMessage());
            return;
        } catch (RuntimeException failure) {
            permit.release();
            metrics.unexpected();
            safeLog(failure);
            error(request.response(), 500, "INTERNAL_ERROR", "Request could not be processed");
            return;
        }

        long acknowledgmentStart = System.nanoTime();
        Future<AdmissionAcknowledgment> send;
        try {
            send = publisher.publish(envelope);
        } catch (RuntimeException failure) {
            permit.release();
            metrics.kafkaUnavailable();
            error(request.response(), 503, "KAFKA_UNAVAILABLE",
                    "Kafka did not acknowledge the job");
            return;
        }

        AtomicBoolean responseSent = new AtomicBoolean();
        long timer = vertx.setTimer(config.acknowledgmentDeadlineMillis(), ignored -> {
            if (responseSent.compareAndSet(false, true)) {
                permit.release();
                metrics.kafkaUnavailable();
                error(request.response(), 503, "KAFKA_UNAVAILABLE",
                        "Kafka acknowledgment is ambiguous or unavailable");
            }
        });
        send.onComplete(result -> {
            vertx.cancelTimer(timer);
            permit.release();
            metrics.acknowledgmentLatency(System.nanoTime() - acknowledgmentStart);
            if (responseSent.compareAndSet(false, true)) {
                if (result.succeeded()) {
                    metrics.accepted();
                    ObjectNode response = JSON.createObjectNode()
                            .put("status", "accepted")
                            .put("jobId", envelope.jobId().toString())
                            .put("executionId", envelope.executionId().toString())
                            .put("schemaVersion", 1)
                            .put("remainingAttempts", envelope.remainingAttempts());
                    json(request.response(), 202, response);
                } else {
                    metrics.kafkaUnavailable();
                    error(request.response(), 503, "KAFKA_UNAVAILABLE",
                            "Kafka did not acknowledge the job");
                }
            }
        });
    }

    private static void drainAndReject(HttpServerRequest request, int status,
                                       String code, String message) {
        request.handler(ignored -> { });
        error(request.response(), status, code, message);
    }

    private static void error(HttpServerResponse response, int status, String code,
                              String message) {
        if (response.ended()) {
            return;
        }
        ObjectNode error = JSON.createObjectNode()
                .put("code", code)
                .put("message", message);
        json(response, status, JSON.createObjectNode().set("error", error));
    }

    private static void json(HttpServerResponse response, int status, ObjectNode body) {
        if (response.ended()) {
            return;
        }
        try {
            response.setStatusCode(status)
                    .putHeader("content-type", "application/json; charset=utf-8")
                    .end(Buffer.buffer(JSON.writeValueAsBytes(body)));
        } catch (JsonProcessingException failure) {
            response.setStatusCode(500).end();
        }
    }

    private static void safeLog(RuntimeException failure) {
        LOG.log(Level.WARNING, "HTTP admission internal failure: {0}",
                failure.getClass().getSimpleName());
    }
}
