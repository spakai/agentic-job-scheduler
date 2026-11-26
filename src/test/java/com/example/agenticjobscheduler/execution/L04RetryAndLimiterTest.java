package com.example.agenticjobscheduler.execution;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class L04RetryAndLimiterTest {
    @Test
    void routesOnlyPositivePostFailureBudgetsToRetry() {
        assertEquals(FailureClassifier.FailureType.RETRYABLE, FailureClassifier.classify(2));
        assertEquals(FailureClassifier.FailureType.RETRYABLE, FailureClassifier.classify(1));
        assertEquals(FailureClassifier.FailureType.EXHAUSTED, FailureClassifier.classify(0));
        assertThrows(IllegalArgumentException.class, () -> FailureClassifier.classify(-1));
        assertThrows(IllegalArgumentException.class, () -> FailureClassifier.classify(100));
    }

    @Test
    void adaptiveLimiterDecreasesOnSlowOrUnstableWindows() {
        var limiter = new AdaptiveLimiter();
        assertEquals(16, limiter.currentLimit());

        int afterSlow = limiter.applyWindow(true, 1500L, 0.00, false, false);
        assertEquals(8, afterSlow);
        assertEquals(8, limiter.currentLimit());

        var second = new AdaptiveLimiter();
        int afterFailure = second.applyWindow(true, 500L, 0.15, false, false);
        assertEquals(8, afterFailure);
        assertEquals(8, second.currentLimit());
    }

    @Test
    void adaptiveLimiterIncreasesOnlyWithHealthyBacklog() {
        var limiter = new AdaptiveLimiter(1, 32, 8);
        assertEquals(9, limiter.applyWindow(true, 200L, 0.00, false, false));
        assertEquals(9, limiter.currentLimit());

        var capped = new AdaptiveLimiter(1, 8, 8);
        assertEquals(8, capped.applyWindow(true, 200L, 0.00, false, false));
    }

    @Test
    void adaptiveLimiterHoldsInsufficientWindowsExceptForThrottleSignals() {
        var limiter = new AdaptiveLimiter();
        assertEquals(16, limiter.applyWindow(true, 1_500L, 0.20, false, false, 19));
        assertEquals(8, limiter.applyWindow(true, 1_500L, 0.20, false, false, 20));
        assertEquals(8, limiter.applyWindow(true, 500L, 0.00, false, false));
        assertEquals(4, limiter.applyWindow(true, 500L, 0.00, true, false, 0));
    }

    @Test
    void explicitPressureBypassesSampleFloorAndHalvesOncePerWindow() {
        var limiter = new AdaptiveLimiter();

        assertEquals(8, limiter.applyWindow(true, 0, 0, false, false, 0, true));
        assertEquals(4, limiter.applyWindow(true, 0, 0, false, false, 0, true));
        assertEquals(8, new AdaptiveLimiter().applyWindow(
                true, 0, 0, false, true, 0, false));
    }

    @Test
    void sustainedPressureStopsAtTheConfiguredMinimum() {
        var limiter = new AdaptiveLimiter();

        assertEquals(8, limiter.applyWindow(false, 0, 0, false, false, 0, true));
        assertEquals(4, limiter.applyWindow(false, 0, 0, false, false, 0, true));
        assertEquals(2, limiter.applyWindow(false, 0, 0, false, false, 0, true));
        assertEquals(1, limiter.applyWindow(false, 0, 0, false, false, 0, true));
        assertEquals(1, limiter.applyWindow(false, 0, 0, false, false, 0, true));
    }

    @Test
    void recoveryRequiresTwoQualifyingHealthyWindowsAfterReduction() {
        var limiter = new AdaptiveLimiter();
        assertEquals(8, limiter.applyWindow(true, 0, 0, false, false, 0, true));

        assertEquals(8, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(9, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(10, limiter.applyWindow(true, 500, 0, false, false));

        assertEquals(5, limiter.applyWindow(true, 0, 0, false, false, 0, true));
        assertEquals(5, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(6, limiter.applyWindow(true, 500, 0, false, false));
    }

    @Test
    void recoveryWindowsMustBeConsecutive() {
        var limiter = new AdaptiveLimiter();
        assertEquals(8, limiter.applyWindow(true, 0, 0, false, false, 0, true));

        assertEquals(8, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(8, limiter.applyWindow(false, 500, 0, false, false));
        assertEquals(8, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(9, limiter.applyWindow(true, 500, 0, false, false));
    }

    @Test
    void healthyRecoveryStopsAtMaximumAndDoesNotOccurWithoutBacklog() {
        var limiter = new AdaptiveLimiter(1, 3, 2);
        assertEquals(3, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(3, limiter.applyWindow(true, 500, 0, false, false));

        assertEquals(1, limiter.applyWindow(true, 0, 0, false, false, 0, true));
        assertEquals(1, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(2, limiter.applyWindow(true, 500, 0, false, false));
        assertEquals(2, limiter.applyWindow(false, 500, 0, false, false));
    }

    @Test
    void rejectsInvalidAdaptivePolicyValues() {
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 0, 1_000, 20, 0.10, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 5_000, 0, 20, 0.10, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 5_000, 1_000, 0, 0.10, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 5_000, 1_000, 20, -0.01, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 5_000, 1_000, 20, Double.NaN, 2));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter(1, 8, 4, 5_000, 1_000, 20, 0.10, 0));
        assertThrows(IllegalArgumentException.class,
                () -> new AdaptiveLimiter().applyWindow(true, 500, Double.NaN, false, false));
    }

    @Test
    void slowLatencyAndExternalPressureTriggersCanBeDisabledIndependently() {
        var p95Disabled = new AdaptiveLimiter(1, 128, 16, new AdaptiveLimiter.Policy(
                1, 128, 5_000, 1_000, 20, 0.10, 2, false, true));
        assertEquals(16, p95Disabled.applyWindow(false, 2_000, 0, false, false));
        assertEquals(8, p95Disabled.applyWindow(
                false, 2_000, 0, false, false, 0, true));

        var externalDisabled = new AdaptiveLimiter(1, 128, 16, new AdaptiveLimiter.Policy(
                1, 128, 5_000, 1_000, 20, 0.10, 2, true, false));
        assertEquals(16, externalDisabled.applyWindow(
                false, 500, 0, false, false, 0, true));
        assertEquals(8, externalDisabled.applyWindow(
                false, 2_000, 0, false, false));
    }
}
