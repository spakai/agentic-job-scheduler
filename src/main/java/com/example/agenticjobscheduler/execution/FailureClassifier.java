package com.example.agenticjobscheduler.execution;

/**
 * L04 failure classification for retry/DLQ handoff decisions.
 *
 * <p>Spec 002 distinguishes retryable failures (retry topic), exhausted retries
 * (DLQ after the maximum logical attempt), and permanent failures (DLQ without retry).
 */
public final class FailureClassifier {
    public enum FailureType { RETRYABLE, EXHAUSTED, PERMANENT }

    private FailureClassifier() { }

    public static FailureType classify(int attempt, int maxAttempts, Throwable failure) {
        if (failure == null) {
            return FailureType.PERMANENT;
        }
        if (failure instanceof PermanentJobFailure) {
            return FailureType.PERMANENT;
        }
        if (attempt >= maxAttempts) {
            return FailureType.EXHAUSTED;
        }
        return FailureType.RETRYABLE;
    }

    public static boolean shouldRetry(int attempt, int maxAttempts, Throwable failure) {
        return classify(attempt, maxAttempts, failure) == FailureType.RETRYABLE;
    }
}
