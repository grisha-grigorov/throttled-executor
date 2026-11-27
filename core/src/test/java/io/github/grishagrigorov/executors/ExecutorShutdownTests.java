package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.concurrent.CompletableFuture;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

/**
 * Tests for {@link ThrottledExecutor#shutdownThreadExecutors()}.
 *
 * <p>Tagged {@code shutdown} and run by the separate {@code shutdownTest} Gradle task:
 * these tear down process-wide pools that every other test in the module depends on, so
 * they cannot share a JVM with the rest of the suite.</p>
 *
 * <p>The gap being closed is that shutdown used to reach only the ForkJoinPool. The
 * single-threaded executor behind {@code cancelRemainingRunningTasks()} was never shut
 * down and used a non-daemon thread, so once anything had cancelled tasks that thread kept
 * the JVM alive after {@code main} returned.</p>
 */
/*
 * Explicitly ordered. These tests share JVM-global pool state, and one of them asserts
 * behaviour that only holds BEFORE any pool has been created — which is not something an
 * arbitrary execution order can provide.
 */
@Tag("shutdown")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class ExecutorShutdownTests {

    private static boolean runtimeHasVirtualThreads() {
        return Runtime.version().feature() >= 21;
    }

    @Test
    @Order(2)
    @DisplayName("shutdown reaches every pool the executor owns, including the cancellation thread")
    void shutdownThreadExecutorsShouldShutDownEveryPool() {

        // Touch each pool so it actually exists before shutdown.
        final ThrottledExecutor forkJoin =
                ThrottledExecutor.withBackend("ShutdownFJ", ExecutorBackend.FORK_JOIN);
        forkJoin.submit((Supplier<String>) () -> "ran").join();

        if (runtimeHasVirtualThreads()) {
            final ThrottledExecutor virtual =
                    ThrottledExecutor.withBackend("ShutdownVT", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
            virtual.submit((Supplier<String>) () -> "ran").join();
        }

        // Force the cancellation executor into existence.
        final ThrottledExecutor cancelling = new ThrottledExecutor("ShutdownCancel");
        cancelling.submit((Runnable) () -> { });
        cancelling.cancelRemainingRunningTasks();

        ThrottledExecutor.shutdownThreadExecutors();

        assertThat(ThrottledExecutor.allPoolsShutDown())
                .as("every pool this class created must be shut down, not just the ForkJoinPool — "
                        + "a live non-daemon cancellation thread pins the JVM after main returns")
                .isTrue();
    }

    @Test
    @Order(1)
    @DisplayName("shutdown does not force the virtual backend into existence")
    void shutdownShouldNotCreateTheVirtualBackendJustToCloseIt() {

        assumeTrue(runtimeHasVirtualThreads(), "requires Java 21+");

        // Ordered first precisely so nothing has asked for the virtual backend yet.
        assumeTrue(!ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isCreated(),
                "virtual backend already created by another test in this JVM");

        assertThatCode(ThrottledExecutor::shutdownThreadExecutors)
                .doesNotThrowAnyException();

        assertThat(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isCreated())
                .as("shutting down must not allocate a pool that was never used")
                .isFalse();
    }

    @Test
    @Order(3)
    @DisplayName("shutdown is safe to call twice")
    void shutdownThreadExecutorsShouldBeIdempotent() {

        ThrottledExecutor.shutdownThreadExecutors();

        assertThatCode(ThrottledExecutor::shutdownThreadExecutors)
                .as("shutdown runs on application exit paths; a second call must not blow up")
                .doesNotThrowAnyException();
    }

    /**
     * A submission the shared pool refuses must be reported through the returned future,
     * never thrown out of {@code submit()} itself.
     *
     * <p>{@code submit()} already reports a closed executor that way. Dispatch to a
     * shut-down pool used to behave differently — {@code supplyAsync} let
     * {@link java.util.concurrent.RejectedExecutionException} escape synchronously, so the
     * caller got an unchecked throw at the call site instead of a future to consume. Two
     * refusal paths, two different shapes, one of them unannounced.</p>
     */
    @Test
    @Order(4)
    @DisplayName("a submission refused by a shut-down pool is reported through the future")
    void submitAfterShutdownShouldFailTheFutureRatherThanThrow() {

        ThrottledExecutor.shutdownThreadExecutors();

        final ThrottledExecutor executor = new ThrottledExecutor("AfterShutdown");

        // Reaching the next line at all is half the assertion: submit() must not throw.
        final CompletableFuture<String> future = executor.submit((Supplier<String>) () -> "ran");

        assertThat(future)
                .as("refusal must arrive via the future, and promptly")
                .failsWithin(java.time.Duration.ofSeconds(10));
    }
}
