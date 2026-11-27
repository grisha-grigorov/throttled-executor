package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.lang.reflect.Field;
import java.net.SocketTimeoutException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the basics of the execution recorder: one {@link TaskRecord} per completed
 * task, with everything a production troubleshooter needs — and, just as deliberately,
 * with nothing that would pin the application's objects. Failures are rendered to strings
 * at completion time; the record graph never holds a {@code Throwable}, a job, or a
 * descriptor.
 */
class ExecutionRecorderTests {

    /** Depth-first search for a node snapshot by executor id. */
    static ExecutorNodeSnapshot find(List<ExecutorNodeSnapshot> nodes, String executorId) {
        for (ExecutorNodeSnapshot node : nodes) {
            if (node.executorId().equals(executorId)) {
                return node;
            }
            final ExecutorNodeSnapshot inChildren = find(node.children(), executorId);
            if (inChildren != null) {
                return inChildren;
            }
        }
        return null;
    }

    static ExecutorNodeSnapshot node(ExecutionRecorder recorder, ThrottledExecutor executor) {
        final ExecutorNodeSnapshot node = find(recorder.snapshot().roots(), executor.executorId);
        assertThat(node).as("a node for %s", executor.executorId).isNotNull();
        return node;
    }

    @Test
    @DisplayName("a completed task produces one record with name, sequence, backend and timings")
    void aCompletedTaskProducesOneRecordWithNameSequenceBackendAndTimings() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("RecBasics");
        executor.addListener(recorder);

        executor.submitThrottled(
                List.of(new ThrottledTask<>((Runnable) () -> sleep(20), 4321L, "NamedTask")),
                new MaxConcurrencyPolicy(4));
        executor.waitForCompletion();

        final ExecutorNodeSnapshot node = node(recorder, executor);
        assertThat(node.records()).hasSize(1);

        final TaskRecord record = node.records().get(0);
        assertThat(record.name()).isEqualTo("NamedTask");
        assertThat(record.sizeEstimate()).isEqualTo(4321L);
        assertThat(record.sequence()).isPositive();
        assertThat(record.backend()).isEqualTo(executor.backend().name());
        assertThat(record.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(record.submittedAtEpochMillis()).isPositive();
        assertThat(record.queuedNanos()).isGreaterThanOrEqualTo(0L);
        assertThat(record.durationNanos())
                .as("the body slept 20 ms")
                .isGreaterThanOrEqualTo(TimeUnit.MILLISECONDS.toNanos(15));
        assertThat(record.completionThread()).isNotBlank();
        assertThat(record.failure()).isNull();

        assertThat(node.submitted()).isEqualTo(1);
        assertThat(node.statusCounts().get(TaskStatus.COMPLETED)).isEqualTo(1L);
    }

    @Test
    @DisplayName("a failed task renders the failure as a string — class and message of both top and root cause")
    void aFailedTaskRendersFailureAsStringNeverTheThrowable() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("RecFailure");
        executor.addListener(recorder);

        final CompletableFuture<Void> future = executor.submit((Runnable) () -> {
            throw new IllegalStateException("pump offline", new SocketTimeoutException("no reply in 5s"));
        });
        assertThat(future).failsWithin(java.time.Duration.ofSeconds(10));
        swallowAggregate(executor);

        final TaskRecord record = node(recorder, executor).records().get(0);
        assertThat(record.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(record.failure())
                .contains("IllegalStateException").contains("pump offline")
                .contains("SocketTimeoutException").contains("no reply in 5s");
    }

    /**
     * The structural no-pinning guarantee: nothing in the snapshot graph can hold a
     * {@code Throwable} (whose stack frames and fields pin application objects), a job, a
     * descriptor or an executor. Checked by reflection over the snapshot types so a future
     * field can't sneak one in.
     */
    @Test
    @DisplayName("the snapshot graph retains no Throwable, descriptor or executor anywhere")
    void theSnapshotGraphRetainsNoThrowableAnywhere() {
        for (Class<?> type : List.of(TaskRecord.class, ExecutorNodeSnapshot.class, RecorderSnapshot.class)) {
            for (Field field : type.getDeclaredFields()) {
                assertThat(Throwable.class.isAssignableFrom(field.getType()))
                        .as("%s.%s must not be a Throwable", type.getSimpleName(), field.getName())
                        .isFalse();
                assertThat(field.getType().getName())
                        .as("%s.%s must not reference core executor/descriptor types", type.getSimpleName(), field.getName())
                        .isNotIn("io.github.grishagrigorov.executors.ThrottledExecutor",
                                "io.github.grishagrigorov.executors.spi.TaskDescriptor");
            }
        }
    }

    @Test
    @DisplayName("a rejected submission is recorded with no duration and no histogram entry")
    void rejectedRecordsHaveNoDurationAndNoHistogramEntry() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("RecRejected");
        executor.addListener(recorder);
        executor.close();

        final CompletableFuture<Void> rejected = executor.submit((Runnable) () -> { });
        assertThat(rejected.isCompletedExceptionally()).isTrue();

        final ExecutorNodeSnapshot node = node(recorder, executor);
        assertThat(node.records()).hasSize(1);
        assertThat(node.records().get(0).status()).isEqualTo(TaskStatus.REJECTED);
        assertThat(node.records().get(0).durationNanos()).isEqualTo(-1L);
        assertThat(node.statusCounts().get(TaskStatus.REJECTED)).isEqualTo(1L);

        long histogramTotal = 0;
        for (long bucket : node.histogram()) {
            histogramTotal += bucket;
        }
        assertThat(histogramTotal).as("a body that never ran has no duration to bucket").isZero();
    }

    /**
     * Core's own root-cause util has no cycle guard; the recorder must not import that
     * risk — a cyclic cause chain must render (depth-capped), not hang a worker.
     */
    @Test
    @DisplayName("a cyclic cause chain does not hang the recorder")
    void aCyclicCauseChainDoesNotHangTheRecorder() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("RecCycle");
        executor.addListener(recorder);

        final RuntimeException a = new RuntimeException("cycle-a");
        final RuntimeException b = new RuntimeException("cycle-b", a);
        a.initCause(b);   // a -> b -> a

        executor.submit((Runnable) () -> {
            throw a;
        });

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(() -> {
            final ExecutorNodeSnapshot node = node(recorder, executor);
            assertThat(node.records()).hasSize(1);
            assertThat(node.records().get(0).failure()).contains("cycle-a");
        });
        swallowAggregate(executor);
    }

    static void swallowAggregate(ThrottledExecutor executor) {
        try {
            executor.waitForCompletion();
        } catch (RuntimeException expected) {
            // the aggregate for the deliberate failure
        }
    }

    static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
