package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

/**
 * Tests for how the executor collects and reports task failures.
 *
 * <p>Two distinct defects are pinned here:</p>
 *
 * <ul>
 *   <li><b>Per-thread accumulation.</b> Failures were stashed in a
 *       {@code ThreadLocal<List<Throwable>>} whose backing list was registered into a
 *       queue on first touch and never removed. On the fixed-size pool that leaked one
 *       list per worker; under a thread-per-task backend it would leak one list per
 *       <i>task</i>, without bound, for the executor's lifetime. The accumulator must
 *       therefore hold no per-thread state at all.</li>
 *   <li><b>Aggregation completeness.</b> Every failure in a batch must reach the
 *       aggregate exception, not just whichever one happened to land first.</li>
 * </ul>
 */
class ThrowableCollectionTests {

    /**
     * Structural pin for the virtual-thread prerequisite: no <em>per-instance</em>
     * per-thread state may survive in the executor.
     *
     * <p>An instance {@code ThreadLocal} is the dangerous shape, because it multiplies out
     * to (executors × threads) and, under a thread-per-task backend, threads means tasks.
     * The accumulator that prompted this made that concrete by registering each thread's
     * list into a queue it never drained.</p>
     *
     * <p>The class does still hold one <em>static</em> {@code ThreadLocal}, for ambient
     * parent discovery. That one is deliberately allowed: it is a single slot per thread
     * for the whole JVM rather than one per executor, it holds a single reference rather
     * than a growing collection, and the submission decorator restores it in a
     * {@code finally} — using {@code remove()} when there was no previous binding, so a
     * pooled worker is never left pinning a dead executor.</p>
     */
    @Test
    @DisplayName("the executor holds no per-instance ThreadLocal state")
    void executorShouldNotHoldPerThreadThrowableState() {

        final List<String> instanceThreadLocalFields = new ArrayList<>();

        for (Field field : ThrottledExecutor.class.getDeclaredFields()) {

            final boolean isThreadLocal = ThreadLocal.class.isAssignableFrom(field.getType());
            final boolean isStatic = java.lang.reflect.Modifier.isStatic(field.getModifiers());

            if (isThreadLocal && !isStatic) {
                instanceThreadLocalFields.add(field.getName());
            }
        }

        assertThat(instanceThreadLocalFields)
                .as("per-instance ThreadLocal fields multiply out to executors × threads, and "
                        + "under a thread-per-task backend that is one retained object per task")
                .isEmpty();
    }

    /**
     * The throwable collection must be drained when it is reported. Otherwise a second
     * batch on the same executor re-reports the first batch's failures alongside its own.
     */
    @Test
    @DisplayName("a second failing batch reports only its own failures")
    void secondBatchShouldNotRethrowThePreviousBatchesThrowables() {

        final ThrottledExecutor executor = new ThrottledExecutor("StaleThrowables");

        executor.submit((Runnable) () -> {
            throw new IllegalStateException("batch one failure");
        });

        final ThreadContextAwareCompletionException firstBatch =
                catchAggregate(executor);

        assertThat(firstBatch.aggregationOfThrowables)
                .as("first batch reports exactly its own single failure")
                .hasSize(1);

        // Drop the completed futures so allOf covers only the second batch.
        executor.clearFuturesListFromCompletedTasks();

        executor.submit((Runnable) () -> {
            throw new IllegalStateException("batch two failure");
        });

        final ThreadContextAwareCompletionException secondBatch =
                catchAggregate(executor);

        assertThat(secondBatch.aggregationOfThrowables)
                .as("second batch must report ONLY its own failure — the first batch's "
                        + "throwable must have been drained when it was reported")
                .hasSize(1);
    }

    /**
     * Every task failure in a batch must reach the aggregate. Repeated because the
     * underlying defect is a race: the exception handler ran on a derived stage that
     * nothing waited on, so the aggregate could be read before the handlers had recorded.
     */
    @RepeatedTest(20)
    @DisplayName("every failure in a batch is aggregated, not just the first")
    void allFailuresShouldBeAggregatedNotJustTheFirst() {

        final int taskCount = 32;
        final ThrottledExecutor executor = new ThrottledExecutor("Aggregate");

        for (int i = 0; i < taskCount; i++) {
            final int index = i;
            executor.submit((Runnable) () -> {
                throw new IllegalStateException("failure-" + index);
            });
        }

        final ThreadContextAwareCompletionException aggregate = catchAggregate(executor);

        assertThat(aggregate.aggregationOfThrowables)
                .as("all %d task failures must be bundled into the aggregate exception", taskCount)
                .hasSize(taskCount);
    }

    /** Waits for the batch, asserting it failed, and returns the aggregate exception. */
    private static ThreadContextAwareCompletionException catchAggregate(ThrottledExecutor executor) {

        final Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(executor::waitForCompletion);

        assertThat(thrown)
                .as("a batch containing failing tasks must throw")
                .isInstanceOf(ThreadContextAwareCompletionException.class);

        return (ThreadContextAwareCompletionException) thrown;
    }

    /** Guard: a batch with no failures must not throw and must report completion. */
    @Test
    @DisplayName("a clean batch still completes normally")
    void successfulBatchShouldNotThrow() {

        final ThrottledExecutor executor = new ThrottledExecutor("CleanBatch");
        final CompletableFuture<Void> future = executor.submit((Runnable) () -> { });

        assertThatThrownBy(() -> {
            executor.waitForCompletion();
            throw new IllegalStateException("sentinel — waitForCompletion returned normally");
        }).hasMessage("sentinel — waitForCompletion returned normally");

        assertThat(future.isCompletedExceptionally()).isFalse();
    }
}
