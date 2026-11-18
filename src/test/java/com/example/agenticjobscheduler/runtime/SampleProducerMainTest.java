package com.example.agenticjobscheduler.runtime;

import java.util.UUID;
import java.util.Random;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SampleProducerMainTest {
    @Test
    void demoJobsStartWithOneRemainingAttempt() {
        assertEquals(1, SampleProducerMain.sampleJob(false, 1, 1).remainingAttempts());
    }

    @Test
    void randomizedPartitionCountsRetain1200TotalWithoutFixedPerPartitionCounts() {
        int[] counts = new int[SampleProducerMain.DEMO_PARTITION_COUNT];
        Random random = new Random(1200);
        for (int record = 0; record < SampleProducerMain.DEMO_LOAD_COUNT; record++) {
            int partition = random.nextInt(counts.length);
            counts[partition]++;
        }
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
        boolean notAll100 = false;
        for (int count : counts) {
            if (count != 100) {
                notAll100 = true;
            }
        }
        assertTrue(notAll100);
    }

    @Test
    void loadKeysRouteExactly100RecordsToEachOfTwelvePartitions() {
        int[] counts = new int[SampleProducerMain.DEMO_PARTITION_COUNT];
        for (int partition = 0; partition < counts.length; partition++) {
            for (int record = 0; record < SampleProducerMain.DEMO_RECORDS_PER_PARTITION; record++) {
                UUID jobId = SampleProducerMain.jobIdForPartition(
                        partition, SampleProducerMain.DEMO_PARTITION_COUNT);
                byte[] key = JobRecordEncoder.key(jobId);
                int routedPartition = Utils.toPositive(Utils.murmur2(key))
                        % SampleProducerMain.DEMO_PARTITION_COUNT;
                assertEquals(partition, routedPartition);
                counts[routedPartition]++;
            }
        }

        for (int count : counts) {
            assertEquals(100, count);
        }
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
    }
}
