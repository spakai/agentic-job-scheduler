package com.example.agenticjobscheduler.execution;

import java.util.Objects;

/** Maps reviewed database-client outcomes to pressure without inspecting SQL or errors. */
public final class DatabasePressureAdapter {
    public enum Unavailable {
        SERVER_UNAVAILABLE,
        OVERLOAD,
        ADMISSION_REJECTED
    }

    public enum Timeout {
        CONNECTION_ACQUISITION,
        STATEMENT,
        TRANSACTION,
        DRIVER_OPERATION
    }

    private final ExternalPressureReporter reporter;
    private final String dependencyName;

    public DatabasePressureAdapter(
            ExternalPressureRegistry registry,
            ExternalPressureReporter reporter,
            String dependencyName) {
        Objects.requireNonNull(registry, "registry");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.dependencyName = registry.requireRegistered(dependencyName);
    }

    public boolean observeUnavailable(Unavailable unavailable) {
        Objects.requireNonNull(unavailable, "unavailable");
        return report(ExternalPressureCategory.UNAVAILABLE);
    }

    public boolean observeTimeout(Timeout timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return report(ExternalPressureCategory.TIMEOUT);
    }

    private boolean report(ExternalPressureCategory category) {
        return reporter.report(category, ExternalDependencyClass.DATABASE, dependencyName);
    }
}
