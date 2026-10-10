package com.example.agenticjobscheduler.execution;

/**
 * Small, deterministic limiter state for the L04 adaptive main-thread budget.
 *
 * <p>This is intentionally a library-level utility: the full Kafka worker will
 * combine it with queue admission and partition fairness, but the logic here is
 * expressed in terms of the spec's signals and bounds.
 */
public final class AdaptiveLimiter {
    private static final int DEFAULT_MIN = 1;
    private static final int DEFAULT_MAX = 128;
    private static final int DEFAULT_INITIAL = 16;

    private final int minimum;
    private final int maximum;
    private int limit;
    private long samples;

    public AdaptiveLimiter() {
        this(DEFAULT_MIN, DEFAULT_MAX, DEFAULT_INITIAL);
    }

    public AdaptiveLimiter(int minimum, int maximum, int initialLimit) {
        if (minimum < 1 || maximum < minimum) {
            throw new IllegalArgumentException("Invalid limiter bounds");
        }
        if (initialLimit < minimum || initialLimit > maximum) {
            throw new IllegalArgumentException("Initial limit is outside bounds");
        }
        this.minimum = minimum;
        this.maximum = maximum;
        this.limit = initialLimit;
    }

    public int currentLimit() {
        return limit;
    }

    public int applyWindow(boolean backlogPresent, long p95LatencyMs, double transientFailureRate,
            boolean throttled, boolean timedOut) {
        if (p95LatencyMs < 0) {
            throw new IllegalArgumentException("Latency must be non-negative");
        }
        if (transientFailureRate < 0.0 || transientFailureRate > 1.0) {
            throw new IllegalArgumentException("Failure rate must be between 0 and 1");
        }
        samples++;
        boolean slow = p95LatencyMs > 1000L;
        boolean unstable = transientFailureRate >= 0.10 || throttled || timedOut;
        if (slow || unstable) {
            int decreased = Math.floorDiv(limit, 2);
            limit = Math.max(minimum, decreased);
            return limit;
        }
        if (backlogPresent && samples >= 20) {
            int increased = limit + 1;
            limit = Math.min(maximum, increased);
        }
        return limit;
    }

    public long samples() {
        return samples;
    }
}
