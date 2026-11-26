package com.example.agenticjobscheduler.execution;

import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.function.LongSupplier;

/** Bounds pressure reports to one event per category and dependency per invocation. */
public final class InvocationPressureReporter implements ExternalPressureReporter {
    private final ExternalPressureRegistry registry;
    private final UUID jobId;
    private final UUID executionId;
    private final LongSupplier monotonicNanos;
    private final Consumer<ExternalPressureEvent> observer;
    private final Set<SignalKey> acceptedSignals = ConcurrentHashMap.newKeySet();
    private final AtomicInteger duplicateCount = new AtomicInteger();

    public InvocationPressureReporter(
            ExternalPressureRegistry registry, JobExecution execution,
            Consumer<ExternalPressureEvent> observer) {
        this(registry, execution, System::nanoTime, observer);
    }

    InvocationPressureReporter(
            ExternalPressureRegistry registry, JobExecution execution,
            LongSupplier monotonicNanos, Consumer<ExternalPressureEvent> observer) {
        this.registry = Objects.requireNonNull(registry, "registry");
        Objects.requireNonNull(execution, "execution");
        this.jobId = execution.job().jobId();
        this.executionId = execution.job().executionId();
        this.monotonicNanos = Objects.requireNonNull(monotonicNanos, "monotonicNanos");
        this.observer = Objects.requireNonNull(observer, "observer");
    }

    @Override
    public boolean report(
            ExternalPressureCategory category,
            ExternalDependencyClass dependencyClass,
            String dependencyName) {
        Objects.requireNonNull(category, "category");
        Objects.requireNonNull(dependencyClass, "dependencyClass");
        String registeredName = registry.requireRegistered(dependencyName);
        SignalKey key = new SignalKey(dependencyClass, registeredName, category);
        if (!acceptedSignals.add(key)) {
            duplicateCount.incrementAndGet();
            return false;
        }
        observer.accept(new ExternalPressureEvent(
                jobId, executionId, monotonicNanos.getAsLong(), dependencyClass,
                registeredName, category));
        return true;
    }

    public int acceptedSignalCount() {
        return acceptedSignals.size();
    }

    public int duplicateSuppressedCount() {
        return duplicateCount.get();
    }

    private record SignalKey(
            ExternalDependencyClass dependencyClass,
            String dependencyName,
            ExternalPressureCategory category) { }
}
