package com.example.agenticjobscheduler.runtime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.ProcessingWindow;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ConsumerGroupDescription;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.ConsumerGroupState;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.utils.Utils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Spec 005 C12: observable continuity of retained lanes across partial revocation. */
@Testcontainers
class KafkaWorkerCooperativeIT {
    @Container
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("apache/kafka-native:3.8.0"));

    @ParameterizedTest
    @EnumSource(WorkerRole.class)
    void retainedPartitionsProgressWithoutReplayingAcrossRepeatedJoinsAndLeaves(WorkerRole role)
            throws Exception {
        String bootstrap = kafka.getBootstrapServers();
        TopicProvisioner.ensureTopics(bootstrap, 3);
        Map<Integer, UUID> keys = keysForEachPartition();
        ControlledHandler originalHandler = new ControlledHandler();
        ProcessingWindow originalWindow = window();
        Vertx originalVertx = Vertx.vertx();
        KafkaWorker original = worker(originalVertx, bootstrap, role, originalHandler, originalWindow);

        try (AdminClient admin = AdminClient.create(Map.of("bootstrap.servers", bootstrap));
             KafkaProducer<byte[], byte[]> producer = new KafkaProducer<>(Map.of(
                     "bootstrap.servers", bootstrap, "acks", "all",
                     "key.serializer", ByteArraySerializer.class.getName(),
                     "value.serializer", ByteArraySerializer.class.getName()))) {
            original.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
            ConsumerGroupDescription initial = awaitGroup(admin, role, 1);
            String originalMember = initial.members().iterator().next().consumerId();

            // Repeating the cycle also exercises incremental assignment when partitions return.
            for (int cycle = 0; cycle < 2; cycle++) {
                long headOffset = cycle * 2L;
                for (int partition = 0; partition < 3; partition++) {
                    publish(producer, role, partition, keys.get(partition), headOffset);
                    SourceRecord head = source(role, partition, headOffset);
                    await("original handler head " + head, () -> originalHandler.starts(head) == 1);
                    publish(producer, role, partition, keys.get(partition), headOffset + 1);
                }
                await("all heads and same-job successors retained",
                        () -> originalWindow.retainedRecords() == 6);

                ControlledHandler newcomerHandler = new ControlledHandler();
                ProcessingWindow newcomerWindow = window();
                Vertx newcomerVertx = Vertx.vertx();
                KafkaWorker newcomer = worker(newcomerVertx, bootstrap, role,
                        newcomerHandler, newcomerWindow);
                try {
                    newcomer.start().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
                    ConsumerGroupDescription joined = awaitGroup(admin, role, 2);
                    Set<Integer> retained = new HashSet<>();
                    joined.members().stream()
                            .filter(member -> member.consumerId().equals(originalMember))
                            .flatMap(member -> member.assignment().topicPartitions().stream())
                            .forEach(partition -> retained.add(partition.partition()));
                    assertFalse(retained.isEmpty(), "the original member must retain partitions");
                    assertTrue(retained.size() < 3, "the join must actually move a partition");
                    Set<Integer> moved = new HashSet<>(Set.of(0, 1, 2));
                    moved.removeAll(retained);
                    for (int partition : moved) {
                        SourceRecord head = source(role, partition, headOffset);
                        await("replacement replay starts " + head,
                                () -> newcomerHandler.starts(head) == 1);
                    }

                    // Replacement heads remain blocked. Release the ORIGINAL retained futures:
                    // a full revoke/recreate would ignore these and strand or replay their lanes.
                    for (int partition : retained) {
                        SourceRecord head = source(role, partition, headOffset);
                        assertEquals(1, originalHandler.starts(head), "retained head must not replay");
                        assertEquals(0, originalHandler.starts(source(role, partition, headOffset + 1)),
                                "same-job successor must wait for its original head");
                        originalHandler.release(head);
                    }
                    for (int partition : retained) {
                        awaitOffset(admin, role, partition, headOffset + 2);
                        assertEquals(1, originalHandler.starts(source(role, partition, headOffset)),
                                "retained handler must not restart across the partial revoke");
                        assertEquals(1, originalHandler.starts(source(role, partition, headOffset + 1)),
                                "retained queued successor must progress exactly once");
                        assertEquals(0, newcomerHandler.starts(source(role, partition, headOffset)),
                                "unaffected work must stay with the original member");
                    }

                    // Old moved handlers settle late. Their callbacks cannot commit while the
                    // replacement's replay is unfinished. Window retirement is the barrier.
                    for (int partition : moved) {
                        originalHandler.release(source(role, partition, headOffset));
                    }
                    await("original reservations retire", () -> originalWindow.retainedRecords() == 0);
                    for (int partition : moved) {
                        assertEquals(cycle == 0 ? -1 : headOffset,
                                committedOffset(admin, role, partition),
                                "stale old completion must not commit replacement's blocked head");
                        assertEquals(0, originalHandler.starts(source(role, partition, headOffset + 1)),
                                "revoked queued successor must never start on old owner");
                        newcomerHandler.release(source(role, partition, headOffset));
                        awaitOffset(admin, role, partition, headOffset + 2);
                        assertEquals(1, newcomerHandler.starts(source(role, partition, headOffset + 1)));
                    }
                    await("newcomer reservations retire", () -> newcomerWindow.retainedRecords() == 0);
                } finally {
                    newcomerHandler.releaseAll();
                    try {
                        newcomer.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
                    } finally {
                        newcomerVertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
                    }
                }
                ConsumerGroupDescription departed = awaitGroup(admin, role, 1);
                assertEquals(originalMember, departed.members().iterator().next().consumerId());
                assertEquals(0, originalWindow.retainedRecords());
                assertEquals(0, originalWindow.retainedBytes());
            }
        } finally {
            originalHandler.releaseAll();
            try {
                original.close().toCompletionStage().toCompletableFuture().get(35, TimeUnit.SECONDS);
            } finally {
                originalVertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
            }
        }
    }

    private static KafkaWorker worker(Vertx vertx, String bootstrap, WorkerRole role,
                                      ControlledHandler handler, ProcessingWindow window) {
        // A long test handler timeout keeps the membership assertions independent of timeout
        // cancellation; the test's bounded waits and explicit promises control settlement.
        return new KafkaWorker(vertx, new WorkerConfig(bootstrap, role, 3, 3, 100),
                handler::execute, window, 1_000, 120_000, 5_000);
    }

    private static ProcessingWindow window() {
        return new ProcessingWindow(1_000, 16L * 1024 * 1024, 2, 2L * 1024 * 1024);
    }

    private static Map<Integer, UUID> keysForEachPartition() {
        Map<Integer, UUID> result = new HashMap<>();
        for (long candidate = 1; result.size() < 3; candidate++) {
            UUID jobId = new UUID(0, candidate);
            int partition = Utils.toPositive(Utils.murmur2(JobRecordEncoder.key(jobId))) % 3;
            result.putIfAbsent(partition, jobId);
        }
        return result;
    }

    private static void publish(KafkaProducer<byte[], byte[]> producer, WorkerRole role,
                                int partition, UUID jobId, long expectedOffset) throws Exception {
        JobEnvelope job = new JobEnvelope(1, jobId, UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode(), 3);
        SourceRecord original = new SourceRecord(TopicProvisioner.MAIN_TOPIC, partition, expectedOffset);
        byte[] value = role == WorkerRole.MAIN ? JobRecordEncoder.main(job)
                : JobRecordEncoder.retry(job.afterFailure(), 2, original, original, "HANDLER_FAILURE");
        var metadata = producer.send(new ProducerRecord<>(role.topic(), partition,
                JobRecordEncoder.key(jobId), value)).get(10, TimeUnit.SECONDS);
        assertEquals(expectedOffset, metadata.offset());
    }

    private static SourceRecord source(WorkerRole role, int partition, long offset) {
        return new SourceRecord(role.topic(), partition, offset);
    }

    private static ConsumerGroupDescription awaitGroup(AdminClient admin, WorkerRole role, int members)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        ConsumerGroupDescription latest = null;
        while (System.nanoTime() < deadline) {
            latest = admin.describeConsumerGroups(Set.of(role.groupId())).all()
                    .get(5, TimeUnit.SECONDS).get(role.groupId());
            if (latest.state() == ConsumerGroupState.STABLE && latest.members().size() == members
                    && latest.members().stream().allMatch(member -> !member.assignment().topicPartitions().isEmpty())) {
                Set<TopicPartition> assigned = new HashSet<>();
                int count = 0;
                for (var member : latest.members()) {
                    assigned.addAll(member.assignment().topicPartitions());
                    count += member.assignment().topicPartitions().size();
                }
                if (assigned.size() == 3 && count == 3) {
                    assertEquals("cooperative-sticky", latest.partitionAssignor(),
                            "verify the broker negotiated cooperative, not eager Range assignment");
                    return latest;
                }
            }
            Thread.sleep(25);
        }
        throw new AssertionError("Group did not stabilize with " + members + " members: " + latest);
    }

    private static void awaitOffset(AdminClient admin, WorkerRole role, int partition, long expected)
            throws Exception {
        await("committed offset " + partition + "=" + expected,
                () -> committedOffset(admin, role, partition) == expected);
    }

    private static long committedOffset(AdminClient admin, WorkerRole role, int partition) throws Exception {
        var offset = admin.listConsumerGroupOffsets(role.groupId()).partitionsToOffsetAndMetadata()
                .get(5, TimeUnit.SECONDS).get(new TopicPartition(role.topic(), partition));
        return offset == null ? -1 : offset.offset();
    }

    private static void await(String description, Check condition) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        while (System.nanoTime() < deadline) {
            if (condition.satisfied()) return;
            Thread.sleep(25);
        }
        throw new AssertionError("Timed out: " + description);
    }

    @FunctionalInterface
    private interface Check {
        boolean satisfied() throws Exception;
    }

    private static final class ControlledHandler {
        private final Map<SourceRecord, AtomicInteger> starts = new ConcurrentHashMap<>();
        private final Map<SourceRecord, Promise<Void>> gates = new ConcurrentHashMap<>();
        private final AtomicBoolean released = new AtomicBoolean();

        Future<Void> execute(JobExecution execution) {
            SourceRecord source = execution.source();
            Promise<Void> gate = source.offset() % 2 == 0
                    ? gates.computeIfAbsent(source, ignored -> Promise.promise()) : null;
            starts.computeIfAbsent(source, ignored -> new AtomicInteger()).incrementAndGet();
            if (gate == null) return Future.succeededFuture();
            if (released.get()) gate.tryComplete();
            return gate.future();
        }

        int starts(SourceRecord source) {
            AtomicInteger count = starts.get(source);
            return count == null ? 0 : count.get();
        }

        void release(SourceRecord source) {
            gates.get(source).tryComplete();
        }

        void releaseAll() {
            released.set(true);
            gates.values().forEach(Promise::tryComplete);
        }
    }
}
