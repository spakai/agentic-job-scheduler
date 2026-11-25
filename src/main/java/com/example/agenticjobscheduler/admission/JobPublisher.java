package com.example.agenticjobscheduler.admission;

import io.vertx.core.Future;

public interface JobPublisher {
    Future<AdmissionAcknowledgment> publish(AdmissionEnvelope envelope);

    boolean ready();

    Future<Void> close();
}
