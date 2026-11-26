package com.example.agenticjobscheduler.execution;

import java.util.Objects;

/** Maps reviewed REST outcomes to pressure without inspecting failure content. */
public final class RestPressureAdapter {
    public enum Timeout {
        CONNECT,
        RESPONSE_HEADER,
        READ,
        TOTAL_REQUEST
    }

    private final ExternalPressureReporter reporter;
    private final String dependencyName;

    public RestPressureAdapter(
            ExternalPressureRegistry registry,
            ExternalPressureReporter reporter,
            String dependencyName) {
        Objects.requireNonNull(registry, "registry");
        this.reporter = Objects.requireNonNull(reporter, "reporter");
        this.dependencyName = registry.requireRegistered(dependencyName);
    }

    public boolean observeStatus(int statusCode) {
        if (statusCode == 503) {
            return report(ExternalPressureCategory.UNAVAILABLE);
        }
        return false;
    }

    public boolean observeTimeout(Timeout timeout) {
        Objects.requireNonNull(timeout, "timeout");
        return report(ExternalPressureCategory.TIMEOUT);
    }

    private boolean report(ExternalPressureCategory category) {
        return reporter.report(category, ExternalDependencyClass.REST, dependencyName);
    }
}
