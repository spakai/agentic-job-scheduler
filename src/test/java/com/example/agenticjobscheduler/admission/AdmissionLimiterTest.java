package com.example.agenticjobscheduler.admission;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class AdmissionLimiterTest {
    @Test
    void rejectsAtCapacityAndReleasesExactlyOnce() {
        AdmissionLimiter limiter = new AdmissionLimiter(1);
        AdmissionLimiter.Permit permit = limiter.tryAcquire();

        assertNotNull(permit);
        assertNull(limiter.tryAcquire());
        permit.release();
        assertEquals(0, limiter.active());
        assertNotNull(limiter.tryAcquire());
        permit.release();
        assertEquals(1, limiter.active());
    }

    @Test
    void stopAdmissionRejectsNewWorkButDoesNotDiscardActivePermit() {
        AdmissionLimiter limiter = new AdmissionLimiter(1);
        AdmissionLimiter.Permit permit = limiter.tryAcquire();
        limiter.stopAdmission();

        assertNull(limiter.tryAcquire());
        assertEquals(1, limiter.active());
        permit.release();
        assertEquals(0, limiter.active());
    }
}
