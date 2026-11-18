package com.example.agenticjobscheduler.runtime;

import java.util.UUID;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class SampleProducerMainTest {
    @Test
    void randomizedSendOrderRetainsExactly100RecordsPerPartition() {
        int[] counts = new int[SampleProducerMain.DEMO_PARTITION_COUNT];
        List<Integer> sendOrder = new ArrayList<>(SampleProducerMain.DEMO_LOAD_COUNT);
        for (int partition = 0; partition < counts.length; partition++) {
            for (int record = 0; record < SampleProducerMain.DEMO_RECORDS_PER_PARTITION; record++) {
                sendOrder.add(partition);
            }
        }
        Collections.shuffle(sendOrder, new Random(1200));
        for (int partition : sendOrder) {
            counts[partition]++;
        }
        for (int count : counts) {
            assertEquals(100, count);
        }
        assertEquals(SampleProducerMain.DEMO_LOAD_COUNT,
                java.util.Arrays.stream(counts).sum());
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
