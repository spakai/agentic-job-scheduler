package com.example.agenticjobscheduler.admission;

import io.vertx.core.Future;

public interface ReadinessProbe {
    Future<Boolean> check();

    Future<Void> close();
}
