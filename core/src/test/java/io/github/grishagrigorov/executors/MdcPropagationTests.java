package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Tests for SLF4J {@link MDC} propagation across the submission boundary.
 *
 * <p>The executor copies the submitter's MDC and applies it inside the task so log lines
 * from a worker carry the originating request's context. Two defects in that decoration
 * are pinned here:</p>
 *
 * <ul>
 *   <li><b>No restore.</b> The context was applied and never undone, so a pooled worker
 *       kept the last task's MDC and bled it into whatever unrelated task ran next.</li>
 *   <li><b>Null map.</b> {@code MDC.getCopyOfContextMap()} returns {@code null} when the
 *       submitter's MDC is empty, and {@code MDC.setContextMap(null)} is not safe across
 *       bindings — so submitting with an empty MDC could fail the task outright.</li>
 * </ul>
 */
class MdcPropagationTests {

    private static final String KEY = "job";

    @BeforeEach
    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    /** Mirrors the executor's own pool sizing, so we can be sure every worker is exercised. */
    private static int poolSize() {
        final int available = Runtime.getRuntime().availableProcessors();
        final int reduced = available - 2;
        return reduced <= 2 ? available : reduced;
    }

    /**
     * The common case: an empty MDC on the submitting thread. The captured map is
     * {@code null}, and the decoration must cope rather than blowing up the task.
     */
    @Test
    @DisplayName("submitting with an empty MDC does not fail the task")
    void submittingWithAnEmptyMdcShouldNotFailTheTask() {

        MDC.clear();

        final ThrottledExecutor executor = new ThrottledExecutor("EmptyMdc");
        // Cast required: submit() is overloaded on both Callable and Supplier.
        final CompletableFuture<String> future = executor.submit((Supplier<String>) () -> "completed");

        assertThat(future.join())
                .as("a task submitted with an empty MDC must run normally")
                .isEqualTo("completed");
    }

    /** The submitter's context must be visible inside the task. */
    @Test
    @DisplayName("a task sees the submitter's MDC context")
    void taskShouldSeeTheSubmittersMdcContext() {

        MDC.put(KEY, "voyage-42");

        final ThrottledExecutor executor = new ThrottledExecutor("MdcVisible");
        final CompletableFuture<String> future = executor.submit((Supplier<String>) () -> MDC.get(KEY));

        assertThat(future.join()).isEqualTo("voyage-42");
    }

    /**
     * The leak. One task establishes a context on some worker; later tasks submitted with
     * an empty MDC must not inherit it.
     *
     * <p>Enough tasks are submitted to cover every worker in the pool, since which worker
     * carries the stale context is not deterministic.</p>
     */
    @Test
    @DisplayName("a task with no MDC does not inherit a previous task's context")
    void taskWithNoMdcShouldNotSeeAPreviousTasksContext() {

        final ThrottledExecutor polluter = new ThrottledExecutor("MdcPolluter");

        MDC.put(KEY, "stale-context");
        // One task per worker, so the context is planted as widely as possible.
        for (int i = 0; i < poolSize() * 2; i++) {
            polluter.submit((Runnable) () -> sleepBriefly());
        }
        polluter.waitForCompletion();

        MDC.clear();

        final ThrottledExecutor probe = new ThrottledExecutor("MdcProbe");
        final ConcurrentLinkedQueue<Map<String, String>> observed = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < poolSize() * 4; i++) {
            probe.submit((Runnable) () -> {
                final Map<String, String> context = MDC.getCopyOfContextMap();
                observed.add(context == null ? Map.of() : context);
            });
        }
        probe.waitForCompletion();

        assertThat(observed)
                .as("no task submitted with an empty MDC may observe a previous task's context")
                .allSatisfy(context -> assertThat(context).doesNotContainKey(KEY));
    }

    /** Submitting must not disturb the submitting thread's own context. */
    @Test
    @DisplayName("the submitter's own MDC is unchanged by submitting")
    void mdcOfTheSubmittingThreadShouldBeUnchangedAfterSubmit() {

        MDC.put(KEY, "caller-context");

        final ThrottledExecutor executor = new ThrottledExecutor("MdcCaller");
        executor.submit((Runnable) () -> MDC.put(KEY, "worker-context"));
        executor.waitForCompletion();

        assertThat(MDC.get(KEY))
                .as("the caller's MDC must survive submission untouched")
                .isEqualTo("caller-context");
    }

    private static void sleepBriefly() {
        try {
            Thread.sleep(20);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
