package com.example.agenticjobscheduler.execution;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class L04RetryAndLimiterTest {
    @Test
    void classifiesRetryableFailuresBeforeAttemptLimit() {
        assertEquals(FailureClassifier.FailureType.RETRYABLE,
                FailureClassifier.classify(1, 3, new IllegalStateException("transient")));
        assertEquals(FailureClassifier.FailureType.RETRYABLE,
                FailureClassifier.classify(2, 3, new IllegalStateException("retry me")));
    }

    @Test
    void classifiesExhaustedAndPermanentFailures() {
        assertEquals(FailureClassifier.FailureType.EXHAUSTED,
                FailureClassifier.classify(3, 3, new IllegalStateException("no more retries")));
        assertEquals(FailureClassifier.FailureType.PERMANENT,
                FailureClassifier.classify(3, 3, null));
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
        for (int i = 0; i < 19; i++) {
            assertEquals(8, limiter.applyWindow(true, 200L, 0.00, false, false));
        }
        assertEquals(9, limiter.applyWindow(true, 200L, 0.00, false, false));
        assertEquals(9, limiter.currentLimit());

        var capped = new AdaptiveLimiter(1, 8, 8);
        for (int i = 0; i < 19; i++) {
            assertEquals(8, capped.applyWindow(true, 200L, 0.00, false, false));
        }
        assertEquals(8, capped.applyWindow(true, 200L, 0.00, false, false));
    }

    @Test
    void adaptiveLimiterHoldsInsufficientWindowsExceptForThrottleSignals() {
        var limiter = new AdaptiveLimiter();
        assertEquals(16, limiter.applyWindow(true, 1_500L, 0.20, false, false, 19));
        assertEquals(8, limiter.applyWindow(true, 1_500L, 0.20, false, false, 20));
        assertEquals(4, limiter.applyWindow(true, 500L, 0.00, true, false, 0));
    }
}
