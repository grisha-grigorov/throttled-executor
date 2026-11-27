package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for blocking from inside a pool task — the nested-executor pattern where a task
 * running on the pool creates its own executor, submits work to it, and waits.
 *
 * <p>This only works if the pool can compensate for the blocked worker. Without
 * compensation, a batch of parent tasks large enough to saturate the pool leaves no
 * worker free to run any child task, and every parent waits out its full timeout.</p>
 *
 * <p>{@code waitForCompletion()} wraps its wait in {@link java.util.concurrent.ForkJoinPool#managedBlock}
 * explicitly. {@code awaitTermination(...)} does not, so these tests establish whether it
 * needs to.</p>
 */
class NestedBlockingTests {

    /** Mirrors the executor's own pool sizing so the pool can be reliably saturated. */
    private static int poolSize() {
        final int available = Runtime.getRuntime().availableProcessors();
        final int reduced = available - 2;
        return reduced <= 2 ? available : reduced;
    }

    /**
     * Saturates the pool with parent tasks that each block in
     * {@code child.awaitTermination(...)}, and requires every one of them to see its
     * child actually finish.
     *
     * <p>If the pool cannot compensate, all workers sit blocked, no child task ever gets
     * scheduled, and every {@code awaitTermination} returns {@code false} at timeout.</p>
     */
    @Test
    @DisplayName("awaitTermination from inside pool tasks does not starve their children")
    void awaitTerminationFromInsidePoolTasksShouldNotDeadlock() {

        // One more than the pool holds, to guarantee saturation.
        final int parentCount = poolSize() + 1;
        final ThrottledExecutor parent = new ThrottledExecutor("NestedParent");
        final List<CompletableFuture<Boolean>> results = new ArrayList<>();

        for (int i = 0; i < parentCount; i++) {
            final int index = i;
            results.add(parent.submit((Callable<Boolean>) () -> {

                final ThrottledExecutor child = new ThrottledExecutor("NestedChild-" + index);
                child.submit((Runnable) () -> sleep(300));
                child.shutdown();

                return child.awaitTermination(5, TimeUnit.SECONDS);
            }));
        }

        parent.waitForCompletion();

        for (int i = 0; i < results.size(); i++) {
            assertThat(results.get(i).join())
                    .as("parent task %d blocked in awaitTermination — its child must still "
                            + "get a worker and finish, which requires pool compensation", i)
                    .isTrue();
        }
    }

    /** A timeout must be reported as a timeout, and must not cancel the running task. */
    @Test
    @DisplayName("awaitTermination returns false on timeout without cancelling the task")
    void awaitTerminationShouldReturnFalseOnTimeout() throws InterruptedException {

        final ThrottledExecutor executor = new ThrottledExecutor("TimeoutCase");
        final CompletableFuture<Void> slow = executor.submit((Runnable) () -> sleep(3_000));
        executor.shutdown();

        assertThat(executor.awaitTermination(200, TimeUnit.MILLISECONDS))
                .as("the task needs 3s; a 200ms wait must report false")
                .isFalse();

        assertThat(slow.isCancelled())
                .as("awaitTermination timing out must not cancel the in-flight task")
                .isFalse();
    }

    /** Tasks that failed are still terminated — an exceptional batch is a finished batch. */
    @Test
    @DisplayName("awaitTermination returns true when tasks failed exceptionally")
    void awaitTerminationShouldReturnTrueWhenTasksFailedExceptionally() throws InterruptedException {

        final ThrottledExecutor executor = new ThrottledExecutor("FailedBatch");
        executor.submit((Runnable) () -> {
            throw new IllegalStateException("deliberate failure");
        });
        executor.shutdown();

        assertThat(executor.awaitTermination(10, TimeUnit.SECONDS))
                .as("a failed task is a completed task")
                .isTrue();
    }

    /** Nothing submitted means nothing to wait for. */
    @Test
    @DisplayName("awaitTermination on an empty executor returns immediately")
    void awaitTerminationOnAnEmptyExecutorShouldReturnImmediately() throws InterruptedException {

        final ThrottledExecutor executor = new ThrottledExecutor("EmptyCase");
        executor.shutdown();

        final long startNanos = System.nanoTime();
        final boolean terminated = executor.awaitTermination(10, TimeUnit.SECONDS);
        final long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos);

        assertThat(terminated).isTrue();
        assertThat(elapsedMillis)
                .as("an empty executor must not wait")
                .isLessThan(1_000L);
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
