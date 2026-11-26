package com.example.agenticjobscheduler.execution;

import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Pattern;

/** Fixed allow-list of safe logical labels used for pressure observability. */
public final class ExternalPressureRegistry {
    private static final Pattern LOGICAL_NAME =
            Pattern.compile("[A-Za-z0-9][A-Za-z0-9._-]{0,63}");
    private final Set<String> dependencyNames;

    public ExternalPressureRegistry(Collection<String> dependencyNames) {
        Objects.requireNonNull(dependencyNames, "dependencyNames");
        LinkedHashSet<String> names = new LinkedHashSet<>();
        for (String name : dependencyNames) {
            Objects.requireNonNull(name, "dependency name");
            if (!LOGICAL_NAME.matcher(name).matches()) {
                throw new IllegalArgumentException("Invalid logical dependency name");
            }
            names.add(name);
        }
        this.dependencyNames = Set.copyOf(names);
    }

    public String requireRegistered(String dependencyName) {
        if (!dependencyNames.contains(dependencyName)) {
            throw new IllegalArgumentException("Unregistered logical dependency name");
        }
        return dependencyName;
    }
}
