package com.example.agenticjobscheduler.runtime;

import com.example.agenticjobscheduler.execution.AdaptiveLimiter;
import com.example.agenticjobscheduler.execution.FailureClassifier;
import com.example.agenticjobscheduler.execution.HandlerCancellation;
import com.example.agenticjobscheduler.execution.ImmediateJobDispatcher;
import com.example.agenticjobscheduler.execution.JobExecution;
import com.example.agenticjobscheduler.execution.JobHandler;
import com.example.agenticjobscheduler.execution.OffsetCommitTracker;
import com.example.agenticjobscheduler.execution.ProcessingWindow;
import com.example.agenticjobscheduler.execution.ProcessingWindowKafkaController;
import com.example.agenticjobscheduler.messaging.EnvelopeValidationException;
import com.example.agenticjobscheduler.messaging.EnvelopeValidator;
import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.RetryEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import io.vertx.core.Context;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Vertx;
import io.vertx.kafka.client.common.TopicPartition;
import io.vertx.kafka.client.consumer.KafkaConsumer;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.consumer.KafkaConsumerRecords;
import io.vertx.kafka.client.consumer.OffsetAndMetadata;
import io.vertx.kafka.client.producer.KafkaProducer;
import io.vertx.kafka.client.producer.KafkaProducerRecord;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.clients.consumer.NoOffsetForPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;

/**
 * One Kafka consumer group member. All delivery, dispatch, and commit state is
 * confined to its Vert.x owner context; Kafka commits are globally serialized.
 */
public final class KafkaWorker {
    private static final Logger LOG = Logger.getLogger(KafkaWorker.class.getName());
    private static final long POLL_MILLIS = 250;
    private static final long HANDOFF_RETRY_MILLIS = 1_000;
    private static final int MAX_RETAINED_RECORDS = 1_000;
    private static final long MAX_RETAINED_BYTES = 16L * 1024 * 1024;
    private static final int MAX_FETCH_RECORDS = 2;
    private static final long MAX_FETCH_BYTES = 2L * 1024 * 1024;
    private static final long DEFAULT_SHUTDOWN_TIMEOUT_MILLIS = 30_000;
    private static final long DEFAULT_HANDLER_TIMEOUT_MILLIS = 30_000;
    private static final long DEFAULT_CANCELLATION_REPORT_MILLIS = 5_000;

    private final Vertx vertx;
    private final WorkerConfig config;
    private final JobHandler handler;
    private final long shutdownTimeoutMillis;
    private final long handlerTimeoutMillis;
    private final long cancellationReportMillis;
    private final EnvelopeValidator validator = new EnvelopeValidator(Set.of("demo.echo"));
    private final ProcessingWindow window;
    private final AdaptiveLimiter mainLimiter;
    private final ArrayDeque<CommitRequest> commitQueue = new ArrayDeque<>();
    private final Map<TopicPartition, PartitionOwner> owners = new HashMap<>();
    private final List<Long> handlerLatencies = new ArrayList<>();
    private final RateGate retryRateGate;
    private final Promise<Void> started = Promise.promise();
    private final Promise<Void> closed = Promise.promise();
    private KafkaConsumer<byte[], byte[]> consumer;
    private KafkaProducer<byte[], byte[]> producer;
    private ProcessingWindowKafkaController windowController;
    private Context context;
    private KafkaConsumerRecords<byte[], byte[]> pendingBatch;
    private Map<String, Integer> partitionCounts;
    private boolean commitInFlight;
    private boolean stopping;
    private boolean closeStarted;
    private boolean startRequested;
    private boolean clientsClosing;
    private long periodicId = -1;
    private long shutdownTimerId = -1;
    private int completedAttempts;
    private int failedAttempts;

    public KafkaWorker(Vertx vertx, WorkerConfig config, JobHandler handler) {
        this(vertx, config, handler, new ProcessingWindow(
                MAX_RETAINED_RECORDS, MAX_RETAINED_BYTES, MAX_FETCH_RECORDS, MAX_FETCH_BYTES),
            DEFAULT_SHUTDOWN_TIMEOUT_MILLIS, DEFAULT_HANDLER_TIMEOUT_MILLIS,
            DEFAULT_CANCELLATION_REPORT_MILLIS);
    }

    KafkaWorker(Vertx vertx, WorkerConfig config, JobHandler handler, ProcessingWindow window) {
        this(vertx, config, handler, window, DEFAULT_SHUTDOWN_TIMEOUT_MILLIS);
    }

    KafkaWorker(Vertx vertx, WorkerConfig config, JobHandler handler, ProcessingWindow window,
                long shutdownTimeoutMillis) {
        this(vertx, config, handler, window, shutdownTimeoutMillis,
                DEFAULT_HANDLER_TIMEOUT_MILLIS, DEFAULT_CANCELLATION_REPORT_MILLIS);
    }

    KafkaWorker(Vertx vertx, WorkerConfig config, JobHandler handler, ProcessingWindow window,
                long shutdownTimeoutMillis, long handlerTimeoutMillis, long cancellationReportMillis) {
        this.vertx = Objects.requireNonNull(vertx, "vertx");
        this.config = Objects.requireNonNull(config, "config");
        this.handler = Objects.requireNonNull(handler, "handler");
        this.window = Objects.requireNonNull(window, "window");
        if (shutdownTimeoutMillis < 1 || handlerTimeoutMillis < 1 || cancellationReportMillis < 1) {
            throw new IllegalArgumentException("Worker timeouts must be positive");
        }
        this.shutdownTimeoutMillis = shutdownTimeoutMillis;
        this.handlerTimeoutMillis = handlerTimeoutMillis;
        this.cancellationReportMillis = cancellationReportMillis;
        this.mainLimiter = new AdaptiveLimiter(1, 128, config.role() == WorkerRole.MAIN
                ? config.concurrency() : 16);
        this.retryRateGate = new RateGate(
                vertx, config.retryStartsPerSecond(), config.concurrency());
    }

    public synchronized Future<Void> start() {
        if (startRequested || closeStarted) {
            return Future.failedFuture("Worker has already been started");
        }
        startRequested = true;
        vertx.runOnContext(ignored -> {
            context = Vertx.currentContext();
            vertx.executeBlocking(this::consumerProperties, false)
                    .onComplete(properties -> {
                        if (stopping) {
                            started.tryFail("Worker stopped during startup");
                            closeClients();
                            return;
                        }
                        if (properties.failed()) {
                            started.tryFail(properties.cause());
                            failWorker("Could not determine Kafka consumer offset policy",
                                    properties.cause());
                            return;
                        }
                        initializeConsumer(properties.result());
                    });
        });
        return started.future();
    }

    private void initializeConsumer(Map<String, String> consumerProperties) {
        consumer = KafkaConsumer.create(vertx, consumerProperties,
                new org.apache.kafka.common.serialization.ByteArrayDeserializer(),
                new org.apache.kafka.common.serialization.ByteArrayDeserializer());
        producer = KafkaProducer.create(vertx, producerProperties(),
                new org.apache.kafka.common.serialization.ByteArraySerializer(),
                new org.apache.kafka.common.serialization.ByteArraySerializer());
        producer.exceptionHandler(failure ->
                LOG.log(Level.WARNING, "Kafka producer reported an asynchronous error", failure));
        windowController = new ProcessingWindowKafkaController(context, consumer, window);
        consumer.partitionsAssignedHandler(this::partitionsAssigned);
        consumer.partitionsRevokedHandler(this::partitionsRevoked);
        loadPartitionCounts()
                .compose(ignoredCounts -> consumer.subscribe(config.role().topic()))
                .onComplete(result -> {
                    if (stopping) {
                        started.tryFail("Worker stopped during startup");
                        closeClients();
                        return;
                    }
                    if (result.failed()) {
                        started.tryFail(result.cause());
                        failWorker("Could not initialize Kafka worker", result.cause());
                        return;
                    }
                    periodicId = vertx.setPeriodic(5_000, ignoredTick ->
                            context.runOnContext(ignoredContext -> evaluateMainLimit()));
                    started.tryComplete();
                    pollNext();
                });
    }

    /** Stops intake, waits for active handler/handoff callbacks, then closes clients. */
    public synchronized Future<Void> close() {
        if (closeStarted) {
            return closed.future();
        }
        closeStarted = true;
        if (!startRequested) {
            closed.tryComplete();
            return closed.future();
        }
        vertx.runOnContext(ignored -> {
            stopping = true;
            if (periodicId != -1) {
                vertx.cancelTimer(periodicId);
            }
            if (consumer == null) {
                closeClients();
                return;
            }
            pendingBatch = null;
            Set<TopicPartition> assigned = Set.copyOf(owners.keySet());
            Future<Void> pause = assigned.isEmpty()
                    ? Future.succeededFuture() : consumer.pause(assigned);
            pause.onComplete(paused -> {
                if (paused.failed()) {
                    LOG.log(Level.WARNING, "Kafka pause failed during worker close", paused.cause());
                }
                owners.values().forEach(this::enqueueCommit);
                maybeFinishClose();
            });
                shutdownTimerId = vertx.setTimer(shutdownTimeoutMillis,
                    ignoredTimer -> context.runOnContext(ignoredContext -> closeClients()));
        });
        return closed.future();
    }

    private Future<Void> loadPartitionCounts() {
        return consumer.partitionsFor(TopicProvisioner.MAIN_TOPIC).compose(main ->
                consumer.partitionsFor(TopicProvisioner.RETRY_TOPIC).compose(retry ->
                        consumer.partitionsFor(TopicProvisioner.DLQ_TOPIC).map(dlq -> {
                            partitionCounts = Map.of(
                                    TopicProvisioner.MAIN_TOPIC, main.size(),
                                    TopicProvisioner.RETRY_TOPIC, retry.size(),
                                    TopicProvisioner.DLQ_TOPIC, dlq.size());
                            if (partitionCounts.values().stream().anyMatch(count -> count < 1)) {
                                throw new IllegalStateException("Kafka topics must have partitions");
                            }
                            return null;
                        })));
    }

    private void partitionsAssigned(Set<TopicPartition> assignment) {
        if (stopping) {
            if (!assignment.isEmpty()) {
                consumer.pause(assignment).onFailure(failure ->
                        LOG.log(Level.WARNING, "Could not pause assignment during shutdown", failure));
            }
            return;
        }
        for (TopicPartition partition : assignment) {
            if (!partition.getTopic().equals(config.role().topic())) {
                continue;
            }
            if (!owners.containsKey(partition)) {
                owners.put(partition, new PartitionOwner(partition));
            }
        }
        resizeDispatchers();
        windowController.partitionsAssigned(assignment).onFailure(failure ->
                failWorker("Could not apply processing-window assignment", failure));
    }

    private void partitionsRevoked(Set<TopicPartition> revoked) {
        for (TopicPartition partition : revoked) {
            PartitionOwner owner = owners.remove(partition);
            if (owner != null) {
                discardQueuedWork(owner);
                enqueueCommit(owner);
                owner.dispatcher.revokeOwnership();
            }
        }
        windowController.partitionsRevoked(revoked);
        resizeDispatchers();
    }

    private void pollNext() {
        if (stopping || consumer == null) {
            return;
        }
        consumer.poll(Duration.ofMillis(POLL_MILLIS)).onComplete(result -> {
            if (stopping) {
                return;
            }
            if (result.failed()) {
                if (result.cause() instanceof NoOffsetForPartitionException missing) {
                    initializeMissingOffsets(missing);
                    return;
                }
                if (result.cause() instanceof OffsetOutOfRangeException) {
                    failWorker("Kafka offset is outside the retained range; refusing to reset", result.cause());
                    return;
                }
                LOG.log(Level.WARNING, "Kafka poll failed; retaining uncommitted work", result.cause());
                vertx.setTimer(HANDOFF_RETRY_MILLIS, ignored -> pollNext());
                return;
            }
            KafkaConsumerRecords<byte[], byte[]> batch = result.result();
            if (!batch.isEmpty()) {
                if (pendingBatch != null) {
                    failWorker("A second Kafka batch arrived before the retained batch was admitted",
                            new IllegalStateException("Processing-window fetch bound violated"));
                    return;
                }
                Optional<ProcessingWindow.Reservation> reservation = windowController
                        .reserveFetched(batch.size(), batchBytes(batch));
                if (reservation.isEmpty()) {
                    pendingBatch = batch;
                    windowController.synchronize().onComplete(sync -> {
                        if (sync.failed()) {
                            failWorker("Could not pause Kafka intake at the processing watermark", sync.cause());
                        } else {
                            pollNext();
                        }
                    });
                    return;
                }
                processBatch(batch, reservation.get());
                windowController.synchronize().onComplete(sync -> {
                    if (sync.failed()) {
                        failWorker("Could not synchronize Kafka intake with the processing window", sync.cause());
                    } else {
                        pollNext();
                    }
                });
                return;
            }
            admitPendingBatch();
            pollNext();
        });
    }

    private void initializeMissingOffsets(NoOffsetForPartitionException missing) {
        // Kafka distinguishes absent positions from out-of-range committed positions.
        // Do not reset an entire assignment: other partitions can have valid progress.
        Set<TopicPartition> partitions = missing.partitions().stream()
                .map(partition -> new TopicPartition(partition.topic(), partition.partition()))
                .filter(owners::containsKey)
                .collect(java.util.stream.Collectors.toSet());
        if (partitions.isEmpty()) {
            // Empty seekToBeginning means ALL assigned partitions in Kafka.
            pollNext();
            return;
        }
        LOG.info(() -> "Initializing partitions without committed offsets at earliest: " + partitions);
        consumer.seekToBeginning(partitions).onComplete(result -> {
            if (stopping) {
                return;
            }
            if (result.failed()) {
                failWorker("Could not initialize missing Kafka offsets", result.cause());
            } else {
                // No other poll is issued while this seek is pending. This seeks locally;
                // offsets are committed only after normal execution/handoff completion.
                pollNext();
            }
        });
    }

    private void admitPendingBatch() {
        if (pendingBatch == null) {
            return;
        }
        KafkaConsumerRecords<byte[], byte[]> batch = pendingBatch;
        Optional<ProcessingWindow.Reservation> reservation =
                windowController.reserveFetched(batch.size(), batchBytes(batch));
        if (reservation.isEmpty()) {
            return;
        }
        pendingBatch = null;
        processBatch(batch, reservation.get());
        windowController.synchronize().onFailure(failure ->
                failWorker("Could not resume Kafka intake after window admission", failure));
    }

    private void processBatch(KafkaConsumerRecords<byte[], byte[]> batch,
                              ProcessingWindow.Reservation reservation) {
        ReservationGroup group = new ReservationGroup(reservation, batch.size());
        for (int index = 0; index < batch.size(); index++) {
            KafkaConsumerRecord<byte[], byte[]> record = batch.recordAt(index);
            TopicPartition partition = new TopicPartition(record.topic(), record.partition());
            PartitionOwner owner = owners.get(partition);
            if (owner == null) {
                failWorker("Kafka delivered a record without a current partition owner",
                        new IllegalStateException(partition.toString()));
                return;
            }
            SourceRecord source = new SourceRecord(record.topic(), record.partition(), record.offset());
            owner.reservations.put(record.offset(), group);
            processRecord(owner, record, source);
        }
        resizeDispatchers();
    }

    private void processRecord(PartitionOwner owner, KafkaConsumerRecord<byte[], byte[]> record,
                               SourceRecord source) {
        try {
            JobEnvelope job;
            int attempt;
            SourceRecord originalSource;
            if (config.role() == WorkerRole.MAIN) {
                job = validator.validateMain(record.key(), record.value());
                attempt = 1;
                originalSource = source;
            } else {
                RetryEnvelope retry = validator.validateRetry(record.key(), record.value());
                job = retry.job();
                attempt = retry.attempt();
                originalSource = retry.originalSource();
                boolean validFailureSource = retry.attempt() == 2
                        ? retry.failedSource().topic().equals(TopicProvisioner.MAIN_TOPIC)
                        : retry.failedSource().topic().equals(TopicProvisioner.RETRY_TOPIC);
                if (!validFailureSource
                        || !retry.originalSource().topic().equals(TopicProvisioner.MAIN_TOPIC)
                        || retry.originalSource().partition()
                        >= partitionCounts.get(TopicProvisioner.MAIN_TOPIC)) {
                    throw new InvalidRoutingException(job, attempt, originalSource);
                }
            }
            int partitionCount = partitionCounts.get(config.role().topic());
            int expectedPartition = Utils.toPositive(Utils.murmur2(record.key())) % partitionCount;
            if (record.partition() != expectedPartition) {
                throw new InvalidRoutingException(job, attempt, originalSource);
            }
            JobExecution execution = new JobExecution(source, job, attempt, new HandlerCancellation());
            ImmediateJobDispatcher.Submission submission = owner.dispatcher.submit(execution);
            submission.handlerStopped().onComplete(stopped -> {
                if (stopped.failed()) {
                    failWorker("Job handler completion signal failed", stopped.cause());
                    return;
                }
                if (stopped.result() == ImmediateJobDispatcher.HandlerResult.SUCCESS) {
                    resizeDispatchers();
                    enqueueCommit(owner);
                    return;
                }
                resizeDispatchers();
                publishFailureHandoff(owner, submission, execution, originalSource);
            });
        } catch (EnvelopeValidationException | InvalidRoutingException invalid) {
            String code = invalid instanceof EnvelopeValidationException validation
                    ? validation.code().name() : "WRONG_PARTITION";
            byte[] deadLetter = invalid instanceof InvalidRoutingException routing
                    ? JobRecordEncoder.deadLetter(
                            routing.job, routing.attempt, source, routing.originalSource, code)
                    : JobRecordEncoder.invalidDeadLetter(source, record.key(), record.value(), code);
            Future<Void> handoff = owner.dispatcher.submitTerminal(source, () -> sendUntilAcknowledged(
                    KafkaProducerRecord.create(TopicProvisioner.DLQ_TOPIC,
                            invalid instanceof InvalidRoutingException routing
                                    ? JobRecordEncoder.key(routing.job.jobId()) : null,
                            deadLetter)));
            handoff.onSuccess(ignored -> enqueueCommit(owner))
                    .onFailure(failure -> failWorker("Invalid record DLQ handoff failed", failure));
        } catch (RuntimeException failure) {
            failWorker("Could not dispatch a Kafka record; it remains uncommitted", failure);
        }
    }

    private void publishFailureHandoff(PartitionOwner owner,
                                       ImmediateJobDispatcher.Submission submission,
                                       JobExecution execution, SourceRecord originalSource) {
        Throwable failure = submission.handlerFailure();
        JobEnvelope failedJob = execution.job().afterFailure();
        FailureClassifier.FailureType failureType =
                FailureClassifier.classify(failedJob.remainingAttempts());
        boolean retryable = failureType == FailureClassifier.FailureType.RETRYABLE;
        String errorCode = failure instanceof HandlerTimeoutException ? "HANDLER_TIMEOUT"
                : "HANDLER_FAILURE";
        if (failureType == FailureClassifier.FailureType.EXHAUSTED) {
            errorCode = "ATTEMPTS_EXHAUSTED";
        }
        String topic = retryable ? TopicProvisioner.RETRY_TOPIC : TopicProvisioner.DLQ_TOPIC;
        byte[] value = retryable
                ? JobRecordEncoder.retry(failedJob, execution.attempt() + 1,
                        execution.source(), originalSource, errorCode)
                : JobRecordEncoder.deadLetter(failedJob, execution.attempt(),
                        execution.source(), originalSource, errorCode);
        sendUntilAcknowledged(KafkaProducerRecord.create(topic,
                JobRecordEncoder.key(execution.job().jobId()), value))
                .onSuccess(ignored -> {
                    OffsetCommitTracker.Completion completion = retryable
                            ? OffsetCommitTracker.Completion.RETRY_ACKNOWLEDGED
                            : OffsetCommitTracker.Completion.DLQ_ACKNOWLEDGED;
                    owner.dispatcher.handoffAcknowledged(submission, completion);
                    enqueueCommit(owner);
                })
                .onFailure(failureCause -> failWorker("Retry/DLQ publication failed", failureCause));
    }

    private Future<Void> sendUntilAcknowledged(KafkaProducerRecord<byte[], byte[]> record) {
        Promise<Void> acknowledged = Promise.promise();
        sendAttempt(record, acknowledged);
        return acknowledged.future();
    }

    private void sendAttempt(KafkaProducerRecord<byte[], byte[]> record, Promise<Void> acknowledged) {
        if (clientsClosing) {
            acknowledged.tryFail("Worker stopped before Kafka handoff acknowledgment");
            return;
        }
        producer.send(record)
                .onSuccess(ignored -> acknowledged.tryComplete())
                .otherwise(failure -> {
                    LOG.log(Level.WARNING, "Kafka handoff send failed; retrying identical record",
                            failure);
                    vertx.setTimer(HANDOFF_RETRY_MILLIS,
                            ignored -> sendAttempt(record, acknowledged));
                    return null;
                });
    }

    private void enqueueCommit(PartitionOwner owner) {
        Optional<OffsetCommitTracker.CommitBatch> ready = owner.dispatcher.beginCommit();
        if (ready.isPresent()) {
            commitQueue.addLast(new CommitRequest(owner, ready.get()));
        }
        pumpCommitQueue();
    }

    private void pumpCommitQueue() {
        if (commitInFlight || commitQueue.isEmpty() || clientsClosing) {
            return;
        }
        CommitRequest request = commitQueue.removeFirst();
        commitInFlight = true;
        Map<TopicPartition, OffsetAndMetadata> commitOffsets = new HashMap<>();
        request.batch.offsets().forEach((partition, offset) ->
                commitOffsets.put(new TopicPartition(partition.topic(), partition.partition()),
                        new OffsetAndMetadata(offset, "")));
        consumer.commit(commitOffsets)
                .onComplete(result -> {
                    commitInFlight = false;
                    if (result.succeeded()) {
                        request.owner.dispatcher.commitSucceeded(request.batch);
                        releaseCommitted(request.owner, request.batch.offsets());
                        // Completions that arrived while this commit was in flight were not batched.
                        if (!request.owner.dispatcher.isOwnershipRevoked()) {
                            enqueueCommit(request.owner);
                        }
                    } else {
                        request.owner.dispatcher.commitFailed(request.batch);
                        LOG.log(Level.WARNING, "Kafka offset commit failed; completed work remains retained",
                                result.cause());
                        if (!request.owner.dispatcher.isOwnershipRevoked()) {
                            vertx.setTimer(HANDOFF_RETRY_MILLIS,
                                    ignored -> enqueueCommit(request.owner));
                        }
                    }
                    pumpCommitQueue();
                    maybeFinishClose();
                });
    }

    private void releaseCommitted(PartitionOwner owner, Map<OffsetCommitTracker.Partition, Long> offsets) {
        offsets.forEach((partition, nextOffset) -> {
            var iterator = owner.reservations.headMap(nextOffset, false).entrySet().iterator();
            while (iterator.hasNext()) {
                ReservationGroup group = iterator.next().getValue();
                iterator.remove();
                releaseGroup(group, "Could not release commit-acknowledged processing reservation");
            }
        });
        admitPendingBatch();
        maybeFinishClose();
    }

    /**
     * Releases a reservation group's claim on the processing window once the last record in the
     * group has either been committed or discarded without ever running.
     */
    private void releaseGroup(ReservationGroup group, String failureMessage) {
        group.remaining--;
        if (group.remaining == 0) {
            windowController.commitAcknowledged(group.reservation)
                    .onFailure(failure -> failWorker(failureMessage, failure));
        }
    }

    /**
     * Releases the processing-window reservation for a single record that will never be
     * committed through the normal Kafka-commit path: either it was discarded while still
     * queued (never started), or its handler settled after the owning assignment was revoked, so
     * the dispatcher's own completion signal was silently ignored (see
     * {@link ImmediateJobDispatcher.Submission#handlerStopped()}).
     */
    private void releaseDiscardedReservation(PartitionOwner owner, SourceRecord source) {
        ReservationGroup group = owner.reservations.remove(source.offset());
        if (group != null) {
            releaseGroup(group, "Could not release retired-assignment processing reservation");
        }
        admitPendingBatch();
        maybeFinishClose();
    }

    /** Releases reservations for submissions discarded while still queued at revoke time. */
    private void discardQueuedWork(PartitionOwner owner) {
        for (SourceRecord discarded : owner.dispatcher.queuedSources(context)) {
            releaseDiscardedReservation(owner, discarded);
        }
    }

    private void resizeDispatchers() {
        if (owners.isEmpty()) {
            return;
        }
        List<PartitionOwner> ordered = owners.values().stream()
                .sorted(Comparator.comparing(owner -> owner.partition.toString()))
                .toList();
        int total = config.role() == WorkerRole.MAIN
                ? mainLimiter.currentLimit() : config.concurrency();
        Map<PartitionOwner, Integer> capacities = new HashMap<>();
        int active = 0;
        for (PartitionOwner owner : ordered) {
            int running = owner.dispatcher.activeHandlers();
            capacities.put(owner, running);
            active += running;
        }
        int available = Math.max(0, total - active);
        List<PartitionOwner> runnable = ordered.stream()
                .filter(owner -> owner.dispatcher.runnableHeads(context) > 0)
                .toList();
        List<PartitionOwner> recipients = runnable.isEmpty() ? ordered : runnable;
        int perRecipient = recipients.isEmpty() ? 0 : available / recipients.size();
        int remainder = recipients.isEmpty() ? 0 : available % recipients.size();
        for (int index = 0; index < recipients.size(); index++) {
            PartitionOwner owner = recipients.get(index);
            int increment = perRecipient + (index < remainder ? 1 : 0);
            capacities.compute(owner, (ignored, value) -> value + increment);
        }
        for (PartitionOwner owner : ordered) {
            owner.dispatcher.setCapacity(0);
        }
        for (PartitionOwner owner : ordered) {
            owner.dispatcher.setCapacity(capacities.get(owner));
        }
    }

    private void evaluateMainLimit() {
        if (config.role() != WorkerRole.MAIN) {
            return;
        }
        int sampleCount = completedAttempts;
        long p95Millis = 0;
        double failureRate = 0;
        if (sampleCount > 0) {
            List<Long> sorted = handlerLatencies.stream().sorted().toList();
            int index = Math.max(0, (int) Math.ceil(sorted.size() * 0.95) - 1);
            long p95Nanos = sorted.get(index);
            p95Millis = p95Nanos / 1_000_000
                    + (p95Nanos % 1_000_000 == 0 ? 0 : 1);
            failureRate = (double) failedAttempts / sampleCount;
        }
        int updated = mainLimiter.applyWindow(
                owners.values().stream().anyMatch(owner -> owner.dispatcher.queuedRecords(context) > 0),
                p95Millis, failureRate,
                false, false, sampleCount);
        completedAttempts = 0;
        failedAttempts = 0;
        handlerLatencies.clear();
        if (updated != mainLimiter.currentLimit()) {
            throw new IllegalStateException("Adaptive limiter returned an inconsistent limit");
        }
        resizeDispatchers();
    }

    private Future<Void> executeHandler(PartitionOwner owner, JobExecution execution) {
        long startedAt = System.nanoTime();
        Supplier<Future<Void>> operation = () -> executeHandlerWithTimeout(execution);
        Future<Void> outcome = config.role() == WorkerRole.RETRY
                ? retryRateGate.submit(
                        () -> !owner.dispatcher.isOwnershipRevoked(),
                        () -> releaseDiscardedReservation(owner, execution.source()),
                        operation)
                : operation.get();
        if (outcome != null) {
            outcome.onComplete(result -> context.runOnContext(ignored -> {
                if (owner.dispatcher.isOwnershipRevoked()) {
                    // The assignment was revoked while this handler was running (or while it sat
                    // behind the retry rate gate). The dispatcher's own completion signal is
                    // silently ignored in that case, so nothing else will ever release this
                    // record's processing-window reservation.
                    releaseDiscardedReservation(owner, execution.source());
                }
                if (result.failed() && result.cause()
                        instanceof com.example.agenticjobscheduler.execution.PermanentJobFailure) {
                    return;
                }
                completedAttempts++;
                if (result.failed()) {
                    failedAttempts++;
                }
                handlerLatencies.add(System.nanoTime() - startedAt);
            }));
        }
        return outcome;
    }

    private Future<Void> executeHandlerWithTimeout(JobExecution execution) {
        Future<Void> handlerCompletion;
        try {
            handlerCompletion = handler.execute(execution);
        } catch (RuntimeException failure) {
            return Future.failedFuture(failure);
        }
        if (handlerCompletion == null) {
            return null;
        }

        Promise<Void> settled = Promise.promise();
        boolean[] timedOut = {false};
        long[] cancellationReportTimer = {-1};
        long timeoutTimer = vertx.setTimer(handlerTimeoutMillis, ignored ->
                context.runOnContext(ignoredContext -> {
                    if (handlerCompletion.isComplete() || settled.future().isComplete()) {
                        return;
                    }
                    timedOut[0] = true;
                    execution.cancellation().requestCancellation();
                    LOG.log(Level.WARNING, "Handler timed out; cancellation requested for job {0}",
                            execution.job().jobId());
                    cancellationReportTimer[0] = vertx.setTimer(cancellationReportMillis,
                            ignoredGrace -> context.runOnContext(graceContext -> {
                                if (!handlerCompletion.isComplete()) {
                                    LOG.log(Level.SEVERE,
                                            "Handler cancellation is unconfirmed; retaining job gate and source record for {0}",
                                            execution.job().jobId());
                                }
                            }));
                }));
        handlerCompletion.onComplete(result -> context.runOnContext(ignoredContext -> {
            vertx.cancelTimer(timeoutTimer);
            if (cancellationReportTimer[0] != -1) {
                vertx.cancelTimer(cancellationReportTimer[0]);
            }
            if (timedOut[0]) {
                settled.tryFail(new HandlerTimeoutException());
            } else if (result.succeeded()) {
                settled.tryComplete();
            } else {
                settled.tryFail(result.cause());
            }
        }));
        return settled.future();
    }

    private static long batchBytes(KafkaConsumerRecords<byte[], byte[]> batch) {
        long bytes = 0;
        for (int index = 0; index < batch.size(); index++) {
            KafkaConsumerRecord<byte[], byte[]> record = batch.recordAt(index);
            bytes += record.key() == null ? 0 : record.key().length;
            bytes += record.value() == null ? 0 : record.value().length;
        }
        return bytes;
    }

    private Map<String, String> consumerProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("bootstrap.servers", config.bootstrapServers());
        properties.put("group.id", config.role().groupId());
        // Keep unaffected partitions active across membership changes. Range-first
        // defaults select eager rebalancing even when cooperative is also supported.
        properties.put("group.protocol", "classic");
        properties.put("partition.assignment.strategy",
                "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
        properties.put("enable.auto.commit", "false");
        properties.put("max.poll.records", Integer.toString(MAX_FETCH_RECORDS));
        properties.put("fetch.max.bytes", Long.toString(MAX_FETCH_BYTES));
        properties.put("max.partition.fetch.bytes", Long.toString(MAX_FETCH_BYTES));
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");

        try {
            if (initialOffsetPolicyUsesEarliest(config.role().groupId())) {
                properties.put("auto.offset.reset", "earliest");
            } else {
                properties.put("auto.offset.reset", "none");
            }
        } catch (ExecutionException | InterruptedException | TimeoutException failure) {
            if (failure instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new IllegalStateException(
                    "Could not determine committed Kafka offsets for group "
                            + config.role().groupId() + "; refusing to start with an offset reset policy",
                    failure);
        }
        return properties;
    }

    private boolean initialOffsetPolicyUsesEarliest(String groupId)
            throws ExecutionException, InterruptedException, TimeoutException {
        try (AdminClient admin = AdminClient.create(
                Map.of("bootstrap.servers", config.bootstrapServers()))) {
            var offsets = admin.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata()
                    .get(30, TimeUnit.SECONDS);
            if (offsets.isEmpty()) {
                return true;
            }

            Map<org.apache.kafka.common.TopicPartition, OffsetSpec> beginningRequests = new HashMap<>();
            Map<org.apache.kafka.common.TopicPartition, OffsetSpec> endRequests = new HashMap<>();
            offsets.keySet().forEach(partition -> {
                beginningRequests.put(partition, OffsetSpec.earliest());
                endRequests.put(partition, OffsetSpec.latest());
            });
            var beginnings = admin.listOffsets(beginningRequests).all().get(30, TimeUnit.SECONDS);
            var ends = admin.listOffsets(endRequests).all().get(30, TimeUnit.SECONDS);
            for (var committed : offsets.entrySet()) {
                var partition = committed.getKey();
                long offset = committed.getValue().offset();
                long beginning = beginnings.get(partition).offset();
                long end = ends.get(partition).offset();
                if (offset < beginning || offset > end) {
                    throw new IllegalStateException("Kafka group " + groupId + " has committed offset "
                            + offset + " for " + partition + " outside the available range ["
                            + beginning + ", " + end + "]");
                }
            }
            return false;
        }
    }

    private Map<String, String> producerProperties() {
        return Map.ofEntries(
                Map.entry("bootstrap.servers", config.bootstrapServers()),
                Map.entry("acks", "all"),
                Map.entry("enable.idempotence", "true"),
                Map.entry("retries", Integer.toString(Integer.MAX_VALUE)),
                Map.entry("max.in.flight.requests.per.connection", "5"),
                Map.entry("max.request.size", "1048576"),
                Map.entry("buffer.memory", "33554432"),
                Map.entry("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"),
                Map.entry("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer"));
    }

    private void failWorker(String message, Throwable failure) {
        LOG.log(Level.SEVERE, message, failure);
        if (!started.future().isComplete()) {
            started.tryFail(failure);
        }
        if (!stopping) {
            close();
        }
    }

    private void closeClients() {
        if (clientsClosing) {
            return;
        }
        clientsClosing = true;
        stopping = true;
        owners.values().forEach(owner -> {
            if (!owner.dispatcher.isOwnershipRevoked()) {
                discardQueuedWork(owner);
                owner.dispatcher.revokeOwnership();
            }
        });
        if (shutdownTimerId != -1) {
            vertx.cancelTimer(shutdownTimerId);
        }
        Future<Void> producerClose = producer == null ? Future.succeededFuture() : producer.close();
        Future<Void> consumerClose = consumer == null ? Future.succeededFuture() : consumer.close();
        Future.all(producerClose, consumerClose).onComplete(result -> {
            consumer = null;
            producer = null;
            if (result.succeeded()) {
                closed.tryComplete();
            } else {
                closed.tryFail(result.cause());
            }
        });
    }

    private void maybeFinishClose() {
        if (!stopping || clientsClosing || window.retainedRecords() != 0
                || commitInFlight || !commitQueue.isEmpty()) {
            return;
        }
        closeClients();
    }

    private final class PartitionOwner {
        private final TopicPartition partition;
        private final ImmediateJobDispatcher dispatcher;
        private final TreeMap<Long, ReservationGroup> reservations = new TreeMap<>();

        private PartitionOwner(TopicPartition partition) {
            this.partition = partition;
            dispatcher = new ImmediateJobDispatcher(context, partition.getTopic(),
                    Set.of(partition.getPartition()), 1, MAX_RETAINED_RECORDS,
                    execution -> KafkaWorker.this.executeHandler(this, execution));
            dispatcher.setCapacity(0);
        }
    }

    private static final class ReservationGroup {
        private final ProcessingWindow.Reservation reservation;
        private int remaining;

        private ReservationGroup(ProcessingWindow.Reservation reservation, int remaining) {
            this.reservation = reservation;
            this.remaining = remaining;
        }
    }

    private record CommitRequest(PartitionOwner owner, OffsetCommitTracker.CommitBatch batch) { }

    private static final class InvalidRoutingException extends RuntimeException {
        private final JobEnvelope job;
        private final int attempt;
        private final SourceRecord originalSource;

        private InvalidRoutingException(JobEnvelope job, int attempt, SourceRecord originalSource) {
            super("WRONG_PARTITION");
            this.job = job;
            this.attempt = attempt;
            this.originalSource = originalSource;
        }
    }

    private static final class HandlerTimeoutException extends RuntimeException {
        private HandlerTimeoutException() {
            super("HANDLER_TIMEOUT");
        }
    }

    private static final class RateGate {
        private final Vertx vertx;
        private final long intervalNanos;
        private final int queueLimit;
        private final ArrayDeque<RateRequest> waiting = new ArrayDeque<>();
        private boolean timerScheduled;
        private long lastStartNanos;

        private RateGate(Vertx vertx, int startsPerSecond, int queueLimit) {
            this.vertx = vertx;
            this.intervalNanos = Math.max(1L, TimeUnit.SECONDS.toNanos(1) / startsPerSecond);
            this.queueLimit = queueLimit;
        }

        private Future<Void> submit(Supplier<Boolean> isValid, Runnable onInvalidated,
                                    Supplier<Future<Void>> operation) {
            if (waiting.size() >= queueLimit) {
                return Future.failedFuture("Retry handler start queue is full");
            }
            Promise<Void> result = Promise.promise();
            waiting.addLast(new RateRequest(isValid, onInvalidated, operation, result));
            pump();
            return result.future();
        }

        private void pump() {
            if (timerScheduled || waiting.isEmpty()) {
                return;
            }
            long elapsedNanos = System.nanoTime() - lastStartNanos;
            long remainingNanos = lastStartNanos == 0
                    ? 0 : Math.max(0, intervalNanos - elapsedNanos);
            long delay = TimeUnit.NANOSECONDS.toMillis(remainingNanos)
                    + (remainingNanos % 1_000_000 == 0 ? 0 : 1);
            if (delay > 0) {
                timerScheduled = true;
                vertx.setTimer(delay, ignored -> {
                    timerScheduled = false;
                    startNext();
                });
                return;
            }
            startNext();
        }

        private void startNext() {
            RateRequest next = null;
            while (next == null) {
                if (waiting.isEmpty()) {
                    return;
                }
                RateRequest candidate = waiting.removeFirst();
                if (candidate.isValid.get()) {
                    next = candidate;
                } else {
                    // The owner was revoked while this request sat behind the rate gate: never
                    // invoke the operation and never settle its promise (no handler starts, no
                    // outcome is fabricated); just let the caller release what it held for this.
                    candidate.onInvalidated.run();
                }
            }
            final RateRequest request = next;
            lastStartNanos = System.nanoTime();
            Future<Void> operation;
            try {
                operation = request.operation.get();
            } catch (RuntimeException failure) {
                operation = Future.failedFuture(failure);
            }
            if (operation == null) {
                return;
            }
            operation.onComplete(result -> {
                if (result.succeeded()) {
                    request.result.tryComplete();
                } else {
                    request.result.tryFail(result.cause());
                }
            });
            pump();
        }
    }

    private record RateRequest(Supplier<Boolean> isValid, Runnable onInvalidated,
                               Supplier<Future<Void>> operation, Promise<Void> result) { }
}
