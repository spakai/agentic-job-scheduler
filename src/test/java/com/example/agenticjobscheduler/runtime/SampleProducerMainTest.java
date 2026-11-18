package com.example.agenticjobscheduler.runtime;

import java.util.UUID;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SampleProducerMainTest {
    @Test
    void loadKeysRouteExactly300RecordsToEachOfFourPartitions() {
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

        assertEquals(300, counts[0]);
        assertEquals(300, counts[1]);
        assertEquals(300, counts[2]);
        assertEquals(300, counts[3]);
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
    }
}
