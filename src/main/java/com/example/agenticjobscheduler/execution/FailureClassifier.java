package com.example.agenticjobscheduler.execution;

/** Routing after a handler failure; the supplied budget has already been decremented. */
public final class FailureClassifier {
    public enum FailureType { RETRYABLE, EXHAUSTED }

    private FailureClassifier() { }

    public static FailureType classify(int remainingAttempts) {
        if (remainingAttempts < 0 || remainingAttempts > 99) {
            throw new IllegalArgumentException("Invalid post-failure remaining attempts");
        }
        return remainingAttempts == 0 ? FailureType.EXHAUSTED : FailureType.RETRYABLE;
    }
}
