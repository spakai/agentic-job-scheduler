package com.example.agenticjobscheduler.execution;

import java.util.Objects;
import java.util.UUID;

/** Sanitized, bounded-cardinality pressure observation for one handler invocation. */
public record ExternalPressureEvent(
        UUID jobId,
        UUID executionId,
        long observedAtNanos,
        ExternalDependencyClass dependencyClass,
        String dependencyName,
        ExternalPressureCategory category) {
    public ExternalPressureEvent {
        Objects.requireNonNull(jobId, "jobId");
        Objects.requireNonNull(executionId, "executionId");
        Objects.requireNonNull(dependencyClass, "dependencyClass");
        Objects.requireNonNull(dependencyName, "dependencyName");
        Objects.requireNonNull(category, "category");
    }
}
