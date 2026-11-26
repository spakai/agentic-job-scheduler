package com.example.agenticjobscheduler.execution;

import java.util.Objects;

/**
 * Small, deterministic limiter state for the L04 adaptive main-thread budget.
 *
 * <p>This is intentionally a library-level utility: the full Kafka worker will
 * combine it with queue admission and partition fairness, but the logic here is
 * expressed in terms of the spec's signals and bounds.
 */
public final class AdaptiveLimiter {
    public enum DecisionReason {
        EXTERNAL_PRESSURE,
        THROTTLE,
        HANDLER_TIMEOUT,
        HIGH_LATENCY,
        TRANSIENT_FAILURES,
        RECOVERY_HOLD,
        INCREASE,
        MAXIMUM,
        INSUFFICIENT_BACKLOG,
        INSUFFICIENT_SAMPLES
    }

    public record Policy(int minimum, int maximum, long evaluationPeriodMillis,
                         long targetP95LatencyMillis, long sampleFloor,
                         double transientFailureThreshold, int recoveryWindows,
                         boolean slowLatencyEnabled, boolean externalPressureEnabled) {
        public static final Policy DEFAULT = new Policy(
                1, 128, 5_000, 1_000, 20, 0.10, 2, true, true);

        public Policy(int minimum, int maximum, long evaluationPeriodMillis,
                      long targetP95LatencyMillis, long sampleFloor,
                      double transientFailureThreshold, int recoveryWindows) {
            this(minimum, maximum, evaluationPeriodMillis, targetP95LatencyMillis,
                    sampleFloor, transientFailureThreshold, recoveryWindows, true, true);
        }

        public Policy {
            if (minimum < 1 || maximum < minimum
                    || evaluationPeriodMillis < 1 || targetP95LatencyMillis < 1 || sampleFloor < 1
                    || !Double.isFinite(transientFailureThreshold)
                    || transientFailureThreshold < 0.0 || transientFailureThreshold > 1.0
                    || recoveryWindows < 1) {
                throw new IllegalArgumentException("Invalid adaptive limiter policy");
            }
        }
    }

    public record WindowDecision(int limitBefore, int limitAfter, DecisionReason reason,
                                 int qualifyingHealthyWindows, boolean minimumClamp) { }

    private static final int DEFAULT_MIN = 1;
    private static final int DEFAULT_MAX = 128;
    private static final int DEFAULT_INITIAL = 16;

    private final int minimum;
    private final int maximum;
    private final long evaluationPeriodMillis;
    private final long targetP95LatencyMillis;
    private final long sampleFloor;
    private final double transientFailureThreshold;
    private final int recoveryWindows;
    private final boolean slowLatencyEnabled;
    private final boolean externalPressureEnabled;
    private int limit;
    private int qualifyingHealthyWindows;
    private boolean recovering;

    public AdaptiveLimiter() {
        this(DEFAULT_MIN, DEFAULT_MAX, DEFAULT_INITIAL, Policy.DEFAULT);
    }

    public AdaptiveLimiter(int minimum, int maximum, int initialLimit) {
        this(minimum, maximum, initialLimit, Policy.DEFAULT);
    }

    public AdaptiveLimiter(int minimum, int maximum, int initialLimit,
                           long evaluationPeriodMillis, long targetP95LatencyMillis,
                           long sampleFloor, double transientFailureThreshold,
                           int recoveryWindows) {
        this(minimum, maximum, initialLimit, new Policy(
                minimum, maximum, evaluationPeriodMillis, targetP95LatencyMillis, sampleFloor,
                transientFailureThreshold, recoveryWindows));
    }

    public AdaptiveLimiter(int minimum, int maximum, int initialLimit, Policy policy) {
        Objects.requireNonNull(policy, "policy");
        if (minimum < 1 || maximum < minimum) {
            throw new IllegalArgumentException("Invalid limiter bounds");
        }
        if (initialLimit < minimum || initialLimit > maximum) {
            throw new IllegalArgumentException("Initial limit is outside bounds");
        }
        this.minimum = minimum;
        this.maximum = maximum;
        this.evaluationPeriodMillis = policy.evaluationPeriodMillis();
        this.targetP95LatencyMillis = policy.targetP95LatencyMillis();
        this.sampleFloor = policy.sampleFloor();
        this.transientFailureThreshold = policy.transientFailureThreshold();
        this.recoveryWindows = policy.recoveryWindows();
        this.slowLatencyEnabled = policy.slowLatencyEnabled();
        this.externalPressureEnabled = policy.externalPressureEnabled();
        this.limit = initialLimit;
    }

    public int currentLimit() {
        return limit;
    }

    public long evaluationPeriodMillis() {
        return evaluationPeriodMillis;
    }

    public int applyWindow(boolean backlogPresent, long p95LatencyMs, double transientFailureRate,
            boolean throttled, boolean timedOut) {
        return applyWindowDecision(backlogPresent, p95LatencyMs, transientFailureRate,
                throttled, timedOut, sampleFloor, false).limitAfter();
    }

    public int applyWindow(boolean backlogPresent, long p95LatencyMs, double transientFailureRate,
            boolean throttled, boolean timedOut, long completedAttempts) {
        return applyWindowDecision(backlogPresent, p95LatencyMs, transientFailureRate,
                throttled, timedOut, completedAttempts, false).limitAfter();
    }

    public int applyWindow(boolean backlogPresent, long p95LatencyMs, double transientFailureRate,
            boolean throttled, boolean timedOut, long completedAttempts,
            boolean externalPressure) {
        return applyWindowDecision(backlogPresent, p95LatencyMs, transientFailureRate,
                throttled, timedOut, completedAttempts, externalPressure).limitAfter();
    }

    public WindowDecision applyWindowDecision(
            boolean backlogPresent, long p95LatencyMs, double transientFailureRate,
            boolean throttled, boolean timedOut, long completedAttempts,
            boolean externalPressure) {
        if (p95LatencyMs < 0) {
            throw new IllegalArgumentException("Latency must be non-negative");
        }
        if (!Double.isFinite(transientFailureRate)
                || transientFailureRate < 0.0 || transientFailureRate > 1.0) {
            throw new IllegalArgumentException("Failure rate must be between 0 and 1");
        }
        if (completedAttempts < 0) {
            throw new IllegalArgumentException("Completed attempts must be non-negative");
        }

        boolean observedExternalPressure = externalPressureEnabled && externalPressure;
        boolean explicitPressure = observedExternalPressure || throttled || timedOut;
        boolean sufficientSamples = completedAttempts >= sampleFloor;
        boolean slow = slowLatencyEnabled && sufficientSamples
                && p95LatencyMs > targetP95LatencyMillis;
        boolean unstable = sufficientSamples
                && transientFailureRate >= transientFailureThreshold;
        int limitBefore = limit;
        if (explicitPressure || slow || unstable) {
            limit = Math.max(minimum, Math.floorDiv(limit, 2));
            qualifyingHealthyWindows = 0;
            recovering = true;
            DecisionReason reason = observedExternalPressure
                    ? DecisionReason.EXTERNAL_PRESSURE
                    : throttled ? DecisionReason.THROTTLE
                    : timedOut ? DecisionReason.HANDLER_TIMEOUT
                    : slow ? DecisionReason.HIGH_LATENCY
                    : DecisionReason.TRANSIENT_FAILURES;
            return new WindowDecision(
                    limitBefore, limit, reason, qualifyingHealthyWindows,
                    limitBefore == minimum);
        }

        if (!sufficientSamples) {
            qualifyingHealthyWindows = 0;
            return new WindowDecision(
                    limit, limit, DecisionReason.INSUFFICIENT_SAMPLES, 0, false);
        }
        if (!backlogPresent) {
            qualifyingHealthyWindows = 0;
            return new WindowDecision(
                    limit, limit, DecisionReason.INSUFFICIENT_BACKLOG, 0, false);
        }

        if (!recovering) {
            if (limit < maximum) {
                limit++;
                return new WindowDecision(
                        limitBefore, limit, DecisionReason.INCREASE,
                        qualifyingHealthyWindows, false);
            }
            return new WindowDecision(
                    limit, limit, DecisionReason.MAXIMUM, qualifyingHealthyWindows, false);
        }
        qualifyingHealthyWindows++;
        if (qualifyingHealthyWindows >= recoveryWindows) {
            recovering = false;
            limit = Math.min(maximum, limit + 1);
            return new WindowDecision(
                    limitBefore, limit, DecisionReason.INCREASE,
                    qualifyingHealthyWindows, false);
        }
        return new WindowDecision(
                limitBefore, limit, DecisionReason.RECOVERY_HOLD,
                qualifyingHealthyWindows, false);
    }
}
