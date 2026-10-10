package com.example.agenticjobscheduler;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.fail;

/** Temporary CI validation fixture; removed before the PR is ready for review. */
class CiFailureProbeTest {
    @Test
    void intentionalFailureVerifiesCiAndReportUpload() {
        fail("CI01 controlled failure: verify failed job and retained test reports");
    }
}
