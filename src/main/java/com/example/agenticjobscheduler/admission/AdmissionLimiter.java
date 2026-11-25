package com.example.agenticjobscheduler.admission;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public final class AdmissionLimiter {
    private final int capacity;
    private final AtomicInteger active = new AtomicInteger();
    private final AtomicBoolean accepting = new AtomicBoolean(true);

    public AdmissionLimiter(int capacity) {
        if (capacity < 1) {
            throw new IllegalArgumentException("capacity must be positive");
        }
        this.capacity = capacity;
    }

    public Permit tryAcquire() {
        while (accepting.get()) {
            int observed = active.get();
            if (observed >= capacity) {
                return null;
            }
            if (active.compareAndSet(observed, observed + 1)) {
                return new Permit();
            }
        }
        return null;
    }

    public void stopAdmission() {
        accepting.set(false);
    }

    public int active() {
        return active.get();
    }

    public final class Permit {
        private final AtomicBoolean released = new AtomicBoolean();

        private Permit() { }

        public void release() {
            if (released.compareAndSet(false, true)) {
                active.decrementAndGet();
            }
        }
    }
}
