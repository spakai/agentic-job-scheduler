package com.example.agenticjobscheduler.execution;

import static org.junit.jupiter.api.Assertions.*;

import com.example.agenticjobscheduler.messaging.JobEnvelope;
import com.example.agenticjobscheduler.messaging.SourceRecord;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

class ExternalPressureSignalTest {
    private static final String REST = "inventory-rest";
    private static final String DATABASE = "orders-db";
    private static final ExternalPressureRegistry REGISTRY =
            new ExternalPressureRegistry(List.of(REST, DATABASE));

    @Test
    void mapsOnlyRest503ToUnavailable() {
        Fixture fixture = fixture();
        RestPressureAdapter rest = fixture.rest();

        assertTrue(rest.observeStatus(503));
        assertFalse(rest.observeStatus(502));
        assertFalse(rest.observeStatus(404));
        assertEquals(List.of(new ExternalPressureEvent(
                fixture.execution().job().jobId(),
                fixture.execution().job().executionId(),
                101L,
                ExternalDependencyClass.REST,
                REST,
                ExternalPressureCategory.UNAVAILABLE)), fixture.events());
    }

    @Test
    void mapsEveryReviewedRestDeadlineAndPreservesWrappingBoundary() {
        Fixture fixture = fixture();
        RestPressureAdapter rest = fixture.rest();

        for (RestPressureAdapter.Timeout timeout : RestPressureAdapter.Timeout.values()) {
            rest.observeTimeout(timeout);
        }

        RuntimeException wrapper = new RuntimeException(
                "transport wrapper", new IllegalStateException("ignored timeout text"));
        assertNotNull(wrapper.getCause());
        assertEquals(1, fixture.events().size());
        assertEquals(ExternalPressureCategory.TIMEOUT, fixture.events().getFirst().category());
        assertEquals(3, fixture.reporter().duplicateSuppressedCount());
    }

    @Test
    void mapsReviewedDatabaseAvailabilityAndDeadlineOutcomes() {
        Fixture fixture = fixture();
        DatabasePressureAdapter database = fixture.database();

        assertTrue(database.observeUnavailable(DatabasePressureAdapter.Unavailable.OVERLOAD));
        assertTrue(database.observeTimeout(DatabasePressureAdapter.Timeout.STATEMENT));
        assertEquals(Set.of(
                ExternalPressureCategory.UNAVAILABLE,
                ExternalPressureCategory.TIMEOUT),
                Set.copyOf(fixture.events().stream()
                        .map(ExternalPressureEvent::category)
                        .toList()));
        assertTrue(fixture.events().stream()
                .allMatch(event -> event.dependencyClass() == ExternalDependencyClass.DATABASE));
    }

    @Test
    void deduplicatesPerInvocationCategoryAndDependency() {
        Fixture fixture = fixture();

        fixture.rest().observeStatus(503);
        fixture.rest().observeStatus(503);
        fixture.rest().observeTimeout(RestPressureAdapter.Timeout.READ);
        fixture.rest().observeTimeout(RestPressureAdapter.Timeout.CONNECT);
        fixture.database().observeTimeout(DatabasePressureAdapter.Timeout.STATEMENT);

        assertEquals(3, fixture.reporter().acceptedSignalCount());
        assertEquals(2, fixture.reporter().duplicateSuppressedCount());
        assertEquals(3, fixture.events().size());
    }

    @Test
    void neverInfersPressureFromExceptionTextOrUnreviewedOutcomes() {
        Fixture fixture = fixture();
        RuntimeException messageContainingStatus = new RuntimeException("503");
        RuntimeException constraintFailure = new RuntimeException("constraint violation");

        assertNotNull(messageContainingStatus);
        assertNotNull(constraintFailure);
        assertFalse(fixture.rest().observeStatus(500));
        assertFalse(fixture.rest().observeStatus(429));
        assertTrue(fixture.events().isEmpty());
    }

    @Test
    void rejectsUnregisteredOrUnsafeDependencyLabelsAtAdapterConstruction() {
        Fixture fixture = fixture();

        assertThrows(IllegalArgumentException.class, () ->
                new RestPressureAdapter(REGISTRY, fixture.reporter(), "https://inventory.example"));
        assertThrows(IllegalArgumentException.class, () ->
                new RestPressureAdapter(REGISTRY, fixture.reporter(), "unregistered"));
        assertThrows(IllegalArgumentException.class, () ->
                new ExternalPressureRegistry(List.of("contains space")));
    }

    @Test
    void reporterAwareHandlerOverloadPreservesExistingLambdas() {
        JobHandler original = execution -> io.vertx.core.Future.succeededFuture();
        JobExecution execution = fixture().execution();

        assertTrue(original.execute(execution, ExternalPressureReporter.NONE).succeeded());
    }

    private static Fixture fixture() {
        JobEnvelope job = new JobEnvelope(
                1, UUID.randomUUID(), UUID.randomUUID(), "demo.echo",
                JsonNodeFactory.instance.objectNode(), 2);
        JobExecution execution = new JobExecution(
                new SourceRecord("jobs.main.v1", 0, 0), job, 1);
        List<ExternalPressureEvent> events = new ArrayList<>();
        AtomicLong clock = new AtomicLong(100L);
        InvocationPressureReporter reporter = new InvocationPressureReporter(
                REGISTRY, execution, clock::incrementAndGet, events::add);
        return new Fixture(
                execution,
                reporter,
                events,
                new RestPressureAdapter(REGISTRY, reporter, REST),
                new DatabasePressureAdapter(REGISTRY, reporter, DATABASE));
    }

    private record Fixture(
            JobExecution execution,
            InvocationPressureReporter reporter,
            List<ExternalPressureEvent> events,
            RestPressureAdapter rest,
            DatabasePressureAdapter database) { }
}
