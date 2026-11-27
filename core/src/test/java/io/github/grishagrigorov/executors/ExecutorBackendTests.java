package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the pluggable backing executor.
 *
 * <p>The pool a task runs on is selected per executor instance. The ForkJoinPool remains
 * the default, so existing behaviour is unchanged; a virtual-thread-per-task backend is
 * reached only when explicitly asked for, and only on a runtime that has virtual threads.
 * Because this module compiles against the Java 17 API, the virtual executor is obtained
 * through a reflective probe — there is deliberately no compile-time reference to it.</p>
 */
class ExecutorBackendTests {

    /** Virtual threads arrived in Java 21. */
    private static boolean runtimeHasVirtualThreads() {
        return Runtime.version().feature() >= 21;
    }

    @Test
    @DisplayName("the default backend is the ForkJoinPool")
    void defaultBackendShouldBeForkJoin() {

        assumeTrue(System.getProperty(ExecutorBackend.BACKEND_PROPERTY) == null,
                "default deliberately overridden for this run");

        assertThat(ExecutorBackend.defaultBackend())
                .as("unchanged behaviour out of the box — callers opt in to virtual threads")
                .isEqualTo(ExecutorBackend.FORK_JOIN);
    }

    @Test
    @DisplayName("the system property selects the default backend")
    void backendPropertyShouldSelectTheDefault() {

        final String original = System.getProperty(ExecutorBackend.BACKEND_PROPERTY);

        try {
            System.setProperty(ExecutorBackend.BACKEND_PROPERTY, "virtual");
            assertThat(ExecutorBackend.defaultBackend()).isEqualTo(ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

            System.setProperty(ExecutorBackend.BACKEND_PROPERTY, "forkJoin");
            assertThat(ExecutorBackend.defaultBackend()).isEqualTo(ExecutorBackend.FORK_JOIN);

            // An unreadable value must not fail the application; it falls back to the default.
            System.setProperty(ExecutorBackend.BACKEND_PROPERTY, "nonsense");
            assertThat(ExecutorBackend.defaultBackend())
                    .as("an unrecognised value warns and falls back rather than throwing")
                    .isEqualTo(ExecutorBackend.FORK_JOIN);

        } finally {
            if (original == null) {
                System.clearProperty(ExecutorBackend.BACKEND_PROPERTY);
            } else {
                System.setProperty(ExecutorBackend.BACKEND_PROPERTY, original);
            }
        }
    }

    @Test
    @DisplayName("ForkJoin backend tasks run on the pool's named workers")
    void forkJoinBackendTasksShouldRunOnNamedPoolWorkers() {

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("ForkJoinBackend", ExecutorBackend.FORK_JOIN);

        final CompletableFuture<String> threadName =
                executor.submit((Supplier<String>) () -> Thread.currentThread().getName());

        assertThat(threadName.join()).startsWith("executors-worker-thread");
    }

    @Test
    @DisplayName("the virtual backend reports availability matching the runtime")
    void virtualBackendAvailabilityShouldMatchTheRuntime() {

        assertThat(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable())
                .as("virtual threads exist from Java 21; this runtime is %s",
                        Runtime.version().feature())
                .isEqualTo(runtimeHasVirtualThreads());
    }

    @Test
    @DisplayName("\"the thread dies with the task\" tracks which pool actually ran it, not the enum's name")
    void threadOwnershipShouldTrackTheRealPool() {

        /*
         * The invariant that makes cancellation safe to act on. Interrupting a running body is
         * only legitimate when the thread belongs to the task; a pooled worker outlives it and
         * the flag leaks into whatever it picks up next.
         *
         * pool() falls back to the shared ForkJoinPool when virtual threads are missing, while
         * the enum constant still reads VIRTUAL_THREAD_PER_TASK — so answering the ownership
         * question from the constant was wrong on exactly the runtime the library sets as its
         * floor. This pins the two together: whatever pool() decides, the predicate must agree,
         * on any JVM. Java17JarSmokeTests proves the consequence on a real 17 runtime; this is
         * the guard that fails the moment the fallback rule changes without the predicate.
         */
        final boolean virtualPoolIsItsOwn =
                ExecutorBackend.VIRTUAL_THREAD_PER_TASK.pool() != ExecutorBackend.FORK_JOIN.pool();

        assertThat(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.threadDiesWithTheTask())
                .as("the virtual backend owns its threads exactly when it got its own pool")
                .isEqualTo(virtualPoolIsItsOwn);

        assertThat(ExecutorBackend.FORK_JOIN.threadDiesWithTheTask())
                .as("a ForkJoin worker is shared and outlives every task it runs")
                .isFalse();

        assertThat(ExecutorBackend.CALLER_RUNS.threadDiesWithTheTask())
                .as("the caller's thread is emphatically not the task's to interrupt")
                .isFalse();
    }

    @Test
    @DisplayName("virtual backend tasks actually run on virtual threads")
    void virtualBackendTasksShouldRunOnVirtualThreads() {

        assumeTrue(runtimeHasVirtualThreads(), "requires Java 21+");

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("VirtualBackend", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

        // Compiles because test sources build at the toolchain level, not the 17 floor.
        final CompletableFuture<Boolean> isVirtual =
                executor.submit((Supplier<Boolean>) () -> Thread.currentThread().isVirtual());

        assertThat(isVirtual.join())
                .as("a task on the virtual backend must run on a virtual thread")
                .isTrue();
    }

    @Test
    @DisplayName("an unavailable virtual backend falls back to the ForkJoinPool")
    void virtualBackendShouldFallBackToForkJoinWhenUnavailable() {

        // On a runtime without virtual threads the probe returns nothing and work must
        // still run, on the ForkJoinPool, rather than failing.
        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("VirtualFallback", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

        final CompletableFuture<String> threadName =
                executor.submit((Supplier<String>) () -> Thread.currentThread().getName());

        if (runtimeHasVirtualThreads()) {
            assertThat(threadName.join()).doesNotStartWith("executors-worker-thread");
        } else {
            assertThat(threadName.join()).startsWith("executors-worker-thread");
        }
    }

    /**
     * Under the ForkJoinPool the pool's own size is the real concurrency bound and
     * {@link MaxConcurrencyPolicy} is largely decorative. Under a thread-per-task backend
     * nothing else bounds concurrency, so the policy becomes the only thing standing
     * between a batch and unbounded parallelism. This test is vacuous on ForkJoin and
     * meaningful only here.
     */
    @Test
    @DisplayName("MaxConcurrencyPolicy is the only concurrency bound under the virtual backend")
    void maxConcurrencyShouldBeLoadBearingUnderTheVirtualBackend() {

        assumeTrue(runtimeHasVirtualThreads(), "requires Java 21+");

        final int cap = 3;
        final int taskCount = 50;

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("VirtualCap", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger peakInFlight = new AtomicInteger();

        final List<ThrottledTask<Runnable>> jobs = new ArrayList<>();
        for (int i = 0; i < taskCount; i++) {
            jobs.add(new ThrottledTask<>(() -> {
                final int current = inFlight.incrementAndGet();
                peakInFlight.accumulateAndGet(current, Math::max);
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    inFlight.decrementAndGet();
                }
            }, 1_000L, "capped-" + i));
        }

        executor.submitThrottled(jobs, new MaxConcurrencyPolicy(cap));
        executor.waitForCompletion();

        assertThat(peakInFlight.get())
                .as("with %d virtual-thread tasks and a cap of %d, the policy must hold the line",
                        taskCount, cap)
                .isLessThanOrEqualTo(cap);

        assertThat(peakInFlight.get())
                .as("sanity: the batch really did run concurrently, so the cap was exercised")
                .isGreaterThan(1);
    }

    @Test
    @DisplayName("default concurrency is sized differently per backend")
    void defaultMaxConcurrencyShouldDifferByBackend() {

        assertThat(ExecutorBackend.FORK_JOIN.defaultMaxConcurrency())
                .as("bounded by cores — the pool is the real limit anyway")
                .isEqualTo(Runtime.getRuntime().availableProcessors());

        assertThat(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.defaultMaxConcurrency())
                .as("bounded by downstream resources (sockets, handles), not by CPU count")
                .isGreaterThan(Runtime.getRuntime().availableProcessors());
    }

    /**
     * The backing pool is an implementation detail. It used to be a
     * {@code public static final ForkJoinPool} field, which let callers submit straight to
     * it, bypassing every accounting and throttling guarantee this class makes.
     */
    @Test
    @DisplayName("the backing pool is not a public static field")
    void backingPoolShouldNotBeExposedAsAPublicStaticField() {

        final List<String> exposed = new ArrayList<>();

        for (Field field : ThrottledExecutor.class.getDeclaredFields()) {
            final boolean publicStatic =
                    Modifier.isPublic(field.getModifiers()) && Modifier.isStatic(field.getModifiers());

            if (publicStatic && ExecutorService.class.isAssignableFrom(field.getType())) {
                exposed.add(field.getName());
            }
        }

        assertThat(exposed)
                .as("a publicly reachable pool lets callers bypass the executor's own accounting")
                .isEmpty();
    }
}
