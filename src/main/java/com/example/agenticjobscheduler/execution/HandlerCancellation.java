package com.example.agenticjobscheduler.execution;

import io.vertx.core.Future;
import io.vertx.core.Promise;

/** A cancellation request; completion of the handler's future confirms that work has stopped. */
public final class HandlerCancellation {
    private final Promise<Void> requested = Promise.promise();

    public Future<Void> cancellationRequested() {
        return requested.future();
    }

    public boolean isCancellationRequested() {
        return requested.future().isComplete();
    }

    public boolean requestCancellation() {
        return requested.tryComplete();
    }
}