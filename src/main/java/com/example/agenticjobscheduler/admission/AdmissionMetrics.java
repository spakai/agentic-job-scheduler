package com.example.agenticjobscheduler.admission;

import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

public final class AdmissionMetrics {
    private final LongAdder requests = new LongAdder();
    private final LongAdder accepted = new LongAdder();
    private final LongAdder invalid = new LongAdder();
    private final LongAdder overloaded = new LongAdder();
    private final LongAdder kafkaUnavailable = new LongAdder();
    private final LongAdder unexpected = new LongAdder();
    private final AtomicLong bytes = new AtomicLong();
    private final AtomicLong validationNanos = new AtomicLong();
    private final AtomicLong acknowledgmentNanos = new AtomicLong();

    public void request() {
        requests.increment();
    }

    public void bytes(long value) {
        bytes.addAndGet(value);
    }

    public void validationLatency(long nanos) {
        validationNanos.addAndGet(nanos);
    }

    public void acknowledgmentLatency(long nanos) {
        acknowledgmentNanos.addAndGet(nanos);
    }

    public void accepted() {
        accepted.increment();
    }

    public void invalid() {
        invalid.increment();
    }

    public void overloaded() {
        overloaded.increment();
    }

    public void kafkaUnavailable() {
        kafkaUnavailable.increment();
    }

    public void unexpected() {
        unexpected.increment();
    }

    public String prometheus(int active, boolean ready) {
        return """
                # HELP http_admission_requests_total HTTP admission requests by bounded outcome.
                # TYPE http_admission_requests_total counter
                http_admission_requests_total{outcome="all"} %d
                http_admission_requests_total{outcome="accepted"} %d
                http_admission_requests_total{outcome="invalid"} %d
                http_admission_requests_total{outcome="overloaded"} %d
                http_admission_requests_total{outcome="kafka_unavailable"} %d
                http_admission_requests_total{outcome="unexpected"} %d
                # HELP http_admission_active_in_flight Current in-flight admission requests.
                # TYPE http_admission_active_in_flight gauge
                http_admission_active_in_flight %d
                # HELP http_admission_bytes_total Validated request and envelope bytes.
                # TYPE http_admission_bytes_total counter
                http_admission_bytes_total %d
                # HELP http_admission_validation_seconds_total Aggregate validation time.
                # TYPE http_admission_validation_seconds_total counter
                http_admission_validation_seconds_total %.9f
                # HELP http_admission_ack_seconds_total Aggregate broker acknowledgment time.
                # TYPE http_admission_ack_seconds_total counter
                http_admission_ack_seconds_total %.9f
                # HELP http_admission_ready Readiness state.
                # TYPE http_admission_ready gauge
                http_admission_ready %d
                """.formatted(
                        requests.sum(), accepted.sum(), invalid.sum(), overloaded.sum(),
                        kafkaUnavailable.sum(), unexpected.sum(), active, bytes.get(),
                        validationNanos.get() / 1_000_000_000.0,
                        acknowledgmentNanos.get() / 1_000_000_000.0,
                        ready ? 1 : 0);
    }
}
