package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.net.SocketTimeoutException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the two-tier retention model: the ring keeps recent detail, the aggregates
 * keep everything forever at O(1) — counts per status, duration min/max/mean, a decade
 * log-histogram, and failure counts keyed by root-cause class (capped, with an
 * {@code "(other)"} overflow so a pathological class churn cannot grow the map).
 */
class AggregateTests {

    private static ExecutorNodeSnapshot soleNode(ExecutionRecorder recorder) {
        final List<ExecutorNodeSnapshot> roots = recorder.snapshot().roots();
        assertThat(roots).hasSize(1);
        return roots.get(0);
    }

    @Test
    @DisplayName("the ring keeps only the last N records — the highest sequences survive")
    void ringKeepsOnlyTheLastNRecords() {
        final ExecutionRecorder recorder = new ExecutionRecorder(new RecorderConfig(8, 64, 64));
        final ThrottledExecutor executor = new ThrottledExecutor("AggRing");
        executor.addListener(recorder);

        for (int i = 0; i < 30; i++) {
            executor.submit((Runnable) () -> { }).join();
        }
        executor.waitForCompletion();

        final ExecutorNodeSnapshot node = soleNode(recorder);
        assertThat(node.records()).hasSize(8);

        final List<Long> sequences = new ArrayList<>();
        for (TaskRecord record : node.records()) {
            sequences.add(record.sequence());
        }
        // Sequences are per-executor and monotonic; the ring must hold the 8 newest, in order.
        assertThat(sequences).isSorted();
        assertThat(sequences.get(0)).as("22 older records scrolled off").isGreaterThan(22 - 1);
    }

    @Test
    @DisplayName("aggregates survive record scroll-off")
    void aggregatesSurviveRecordScrollOff() {
        final ExecutionRecorder recorder = new ExecutionRecorder(new RecorderConfig(8, 64, 64));
        final ThrottledExecutor executor = new ThrottledExecutor("AggScroll");
        executor.addListener(recorder);

        for (int i = 0; i < 30; i++) {
            executor.submit((Runnable) () -> { }).join();
        }
        executor.waitForCompletion();

        final ExecutorNodeSnapshot node = soleNode(recorder);
        assertThat(node.records()).hasSize(8);
        assertThat(node.submitted()).isEqualTo(30);
        assertThat(node.statusCounts().get(TaskStatus.COMPLETED)).isEqualTo(30L);
        assertThat(node.durationCount()).isEqualTo(30);
        assertThat(node.durationMinNanos()).isGreaterThanOrEqualTo(0);
        assertThat(node.durationMaxNanos()).isGreaterThanOrEqualTo(node.durationMinNanos());
        assertThat(node.meanDurationNanos()).isBetween((double) node.durationMinNanos(), (double) node.durationMaxNanos());

        long histogramTotal = 0;
        for (long bucket : node.histogram()) {
            histogramTotal += bucket;
        }
        assertThat(histogramTotal).isEqualTo(30);
    }

    @Test
    @DisplayName("histogram bucket edges are upper-exclusive decades")
    void histogramBucketEdgesAreUpperExclusive() {
        // Buckets: 0 <1µs · 1 <10µs · 2 <100µs · 3 <1ms · 4 <10ms · 5 <100ms · 6 <1s · 7 <10s · 8 ≥10s
        assertThat(DurationHistogram.bucketFor(0L)).isEqualTo(0);
        assertThat(DurationHistogram.bucketFor(999L)).isEqualTo(0);                  // just under 1 µs
        assertThat(DurationHistogram.bucketFor(1_000L)).isEqualTo(1);               // exactly 1 µs — upper-exclusive
        assertThat(DurationHistogram.bucketFor(9_999_999L)).isEqualTo(4);           // ~9.99 ms -> <10ms
        assertThat(DurationHistogram.bucketFor(10_000_000L)).isEqualTo(5);          // exactly 10 ms -> <100ms
        assertThat(DurationHistogram.bucketFor(9_999_999_999L)).isEqualTo(7);       // ~9.99 s -> <10s
        assertThat(DurationHistogram.bucketFor(10_000_000_000L)).isEqualTo(8);      // exactly 10 s -> >=10s
        assertThat(DurationHistogram.bucketFor(Long.MAX_VALUE)).isEqualTo(8);
        assertThat(DurationHistogram.LABELS).hasSize(DurationHistogram.BUCKETS);
    }

    @Test
    @DisplayName("failure counts are keyed by root-cause class and survive scroll-off")
    void failureCountsAreKeyedByRootCauseClassAndSurviveScrollOff() {
        final ExecutionRecorder recorder = new ExecutionRecorder(new RecorderConfig(8, 64, 64));
        final ThrottledExecutor executor = new ThrottledExecutor("AggFailures");
        executor.addListener(recorder);

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (int i = 0; i < 47; i++) {
            futures.add(executor.submit((Runnable) () -> {
                throw new IllegalStateException("wrap", new SocketTimeoutException("t"));
            }));
        }
        for (CompletableFuture<Void> future : futures) {
            assertThat(future).failsWithin(Duration.ofSeconds(10));
        }
        ExecutionRecorderTests.swallowAggregate(executor);

        final ExecutorNodeSnapshot node = soleNode(recorder);
        assertThat(node.records()).hasSize(8);
        assertThat(node.failureCounts().get(SocketTimeoutException.class.getName()))
                .as("\"47 x SocketTimeoutException\" survives after the records scrolled off")
                .isEqualTo(47L);
    }

    @Test
    @DisplayName("distinct failure-class keys are capped, with an (other) overflow bucket")
    void failureClassKeysAreCappedWithAnOtherBucket() {
        final int cap = 4;
        final ExecutionRecorder recorder = new ExecutionRecorder(new RecorderConfig(64, 64, cap));
        final ThrottledExecutor executor = new ThrottledExecutor("AggCap");
        executor.addListener(recorder);

        // 8 distinct root-cause classes, one failure each.
        final List<RuntimeException> exceptions = List.of(
                new IllegalStateException("1"), new IllegalArgumentException("2"),
                new UnsupportedOperationException("3"), new java.util.NoSuchElementException("4"),
                new java.util.ConcurrentModificationException("5"), new ArithmeticException("6"),
                new IndexOutOfBoundsException("7"), new NullPointerException("8"));
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        for (RuntimeException exception : exceptions) {
            futures.add(executor.submit((Runnable) () -> {
                throw exception;
            }));
        }
        for (CompletableFuture<Void> future : futures) {
            assertThat(future).failsWithin(Duration.ofSeconds(10));
        }
        ExecutionRecorderTests.swallowAggregate(executor);

        final ExecutorNodeSnapshot node = soleNode(recorder);
        assertThat(node.failureCounts()).hasSize(cap + 1);
        assertThat(node.failureCounts().get("(other)"))
                .as("overflow beyond the cap lands in (other) — the map is genuinely O(1)")
                .isEqualTo(8L - cap);

        long total = 0;
        for (long count : node.failureCounts().values()) {
            total += count;
        }
        assertThat(total).as("no failure is lost, only its key precision").isEqualTo(8);
    }

    @Test
    @DisplayName("fifty thousand tasks through one node stay bounded")
    void fiftyThousandTasksThroughOneNodeStayBounded() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("AggBounded");
        executor.addListener(recorder);

        for (int wave = 0; wave < 10; wave++) {
            for (int i = 0; i < 5_000; i++) {
                executor.submit((Runnable) () -> { });
            }
            executor.waitForCompletion();
        }

        final ExecutorNodeSnapshot node = soleNode(recorder);
        assertThat(node.submitted()).isEqualTo(50_000);
        assertThat(node.statusCounts().get(TaskStatus.COMPLETED)).isEqualTo(50_000L);
        assertThat(node.records()).as("the ring, not the history, bounds the detail").hasSize(256);
        assertThat(recorder.snapshot().roots()).hasSize(1);
    }
}
