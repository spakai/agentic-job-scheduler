package com.example.agenticjobscheduler.execution;

/** Reports one explicit pressure observation for the current invocation. */
@FunctionalInterface
public interface ExternalPressureReporter {
    ExternalPressureReporter NONE = (category, dependencyClass, dependencyName) -> false;

    /**
     * @return {@code true} if this invocation accepted the observation, or
     * {@code false} when the same category/dependency was already reported
     */
    boolean report(ExternalPressureCategory category,
                   ExternalDependencyClass dependencyClass,
                   String dependencyName);
}
