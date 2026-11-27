package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Smoke tests that run the CORE JAR on a real Java 17 JVM — the floor the library
 * advertises. Everything else in the family's test estate executes on the 25 toolchain,
 * so until this suite existed the base variant's pre-21 branches (the virtual-thread
 * probe failing soft, the ForkJoinPool fallback) had never once run on the JVM they were
 * written for.
 *
 * <p>This source set compiles at {@code --release 17} (the main test suite compiles at
 * toolchain 25 and cannot load here) and its classpath contains the built jar, not the
 * classes directory — so multi-release selection applies and picks the BASE variant.</p>
 */
class Java17JarSmokeTests {

    @Test
    @DisplayName("this really is a Java 17 JVM")
    void thisReallyIsAJava17Jvm() {
        assertThat(Runtime.version().feature())
                .as("the toolchain launcher wiring, pinned — everything else here depends on it")
                .isEqualTo(17);
    }

    @Test
    @DisplayName("core loads from the jar, at the base floor")
    void coreLoadsFromTheJarAtTheBaseFloor() throws IOException {
        assertThat(ThrottledExecutor.class.getProtectionDomain().getCodeSource().getLocation().getPath())
                .as("the classpath must carry the jar, or none of this exercises the artifact")
                .endsWith(".jar");

        // Selection-aware read: on 17 the versions/21 entry must NOT be selected.
        try (InputStream in = ThrottledExecutor.class.getClassLoader()
                .getResourceAsStream("io/github/grishagrigorov/executors/VirtualThreadSupport.class")) {
            assertThat(in).isNotNull();
            final DataInputStream data = new DataInputStream(in);
            assertThat(data.readInt()).isEqualTo(0xCAFEBABE);
            data.readUnsignedShort();
            assertThat(data.readUnsignedShort())
                    .as("a 17 JVM selects the base (major 61) variant")
                    .isEqualTo(61);
        }
    }

    @Test
    @DisplayName("plain submit and waitForCompletion work")
    void plainSubmitAndWaitWork() {
        final ThrottledExecutor executor = new ThrottledExecutor("Smoke17");

        final CompletableFuture<String> future = executor.submit((Supplier<String>) () -> "ran on 17");
        executor.waitForCompletion();

        assertThat(future.join()).isEqualTo("ran on 17");
    }

    /**
     * The branch this suite exists for: on a pre-21 JVM the probe fails soft and the
     * virtual backend degrades to the shared ForkJoinPool — executed here on a real 17
     * JVM for the first time.
     */
    @Test
    @DisplayName("the virtual backend falls back to the ForkJoinPool")
    void theVirtualBackendFallsBackToTheForkJoinPool() {
        assertThat(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable())
                .as("no virtual threads on 17")
                .isFalse();

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Smoke17Virtual", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
        final AtomicReference<String> thread = new AtomicReference<>();

        executor.submit((Runnable) () -> thread.set(Thread.currentThread().getName())).join();
        executor.waitForCompletion();

        assertThat(thread.get())
                .as("work must still run — on the ForkJoinPool's named workers")
                .startsWith("executors-worker-thread");
    }

    /**
     * The other half of that fallback, and a nastier one: cancellation must not interrupt a
     * body running on a pooled worker.
     *
     * <p>Interrupting is safe only when the thread belongs to the task and dies with it, which
     * is true of a virtual thread per task and of nothing else. The guard used to read the
     * executor's <em>declared</em> backend, which still says {@code VIRTUAL_THREAD_PER_TASK}
     * here even though {@code pool()} handed the work to the shared ForkJoinPool — so on this
     * JVM, the floor the library advertises, cancelling a running task set the interrupt flag
     * on a worker that outlives it, and the flag surfaced inside somebody else's task later.
     * That is the exact leak the guard exists to prevent.</p>
     */
    @Test
    @DisplayName("cancelling a running task does NOT interrupt the pooled worker it fell back to")
    void cancellingDoesNotInterruptTheFallbackWorker() throws Exception {

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Smoke17Cancel", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

        final CountDownLatch running = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final AtomicBoolean flagLeftSet = new AtomicBoolean();
        final CountDownLatch done = new CountDownLatch(1);

        final CompletableFuture<?> future = executor.submit((Runnable) () -> {
            running.countDown();
            try {
                // Long enough that the cancel below lands while this is parked.
                if (!release.await(10, TimeUnit.SECONDS)) {
                    return;
                }
            } catch (InterruptedException e) {
                interrupted.set(true);
                Thread.currentThread().interrupt();
                done.countDown();
                return;
            }
            flagLeftSet.set(Thread.currentThread().isInterrupted());
            done.countDown();
        });

        assertThat(running.await(10, TimeUnit.SECONDS)).isTrue();
        future.cancel(true);
        release.countDown();

        try {
            // A cancelled batch is not a clean batch: the cancellation is recorded like any
            // other failure and surfaces here. Expected, and not what this test is about —
            // swallowing it is what lets the assertions below be the ones that decide.
            executor.waitForCompletion();
        } catch (RuntimeException expected) {
            assertThat(expected).isInstanceOf(ThreadContextAwareCompletionException.class);
        }

        // The body has to have finished before its flags can be read.
        assertThat(done.await(10, TimeUnit.SECONDS)).as("the body ran to its end").isTrue();

        assertThat(interrupted.get())
                .as("a ForkJoin worker outlives the task, so the flag would leak into the next one")
                .isFalse();
        assertThat(flagLeftSet.get())
                .as("and it must not be left set on the way out either")
                .isFalse();
    }

    @Test
    @DisplayName("throttled submission works")
    void throttledSubmissionWorks() {
        final ThrottledExecutor executor = new ThrottledExecutor("Smoke17Throttled");

        executor.submitThrottled(
                List.of(new ThrottledTask<>((Runnable) () -> { }, 1_000L, "smoke"),
                        new ThrottledTask<>((Runnable) () -> { }, 1_000L, "smoke2")),
                new MaxConcurrencyPolicy(2));
        executor.waitForCompletion();

        assertThat(executor.getFutures().allMatch(pair -> pair.getCompletableFuture().isDone())).isTrue();
    }
}
