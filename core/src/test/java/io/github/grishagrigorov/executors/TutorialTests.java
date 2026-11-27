package io.github.grishagrigorov.executors;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import io.github.grishagrigorov.executors.exceptions.RootCauseExceptionUtil;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The migration tutorial, as tests. Every snippet under "Migrating existing code" in
 * {@code utils/executors/README.md} is lifted from a test in this file — same identifiers, same
 * calls — so the guide cannot drift from what the library actually does. Each nested class
 * opens with the legacy shape it replaces; the tests show the replacement.
 */
@DisplayName("Tutorial — migrating existing code")
class TutorialTests {

    @Nested
    @DisplayName("A. new Thread(...).start()")
    class FireAndForget {

        /*
         * Before:
         *
         *     new Thread(() -> auditLog.flush()).start();
         *
         * Nobody holds the thread, nobody learns whether it finished, and an exception inside
         * it goes to the default uncaught-exception handler — usually stderr, usually unread.
         */

        @Test
        @DisplayName("a fire-and-forget thread becomes submit(runnable), and waitForCompletion() proves it ran")
        void fireAndForgetBecomesSubmit() {

            final ThrottledExecutor executor = new ThrottledExecutor("Housekeeping");
            final AtomicBoolean flushed = new AtomicBoolean();

            executor.submit(() -> flushed.set(true));   // the thread you used to start

            executor.waitForCompletion();               // the join you never had
            assertThat(flushed).isTrue();
            executor.close();
        }

        @Test
        @DisplayName("the exception the thread used to swallow now surfaces from waitForCompletion()")
        void theSwallowedExceptionSurfaces() {

            final ThrottledExecutor executor = new ThrottledExecutor("Housekeeping");

            // A block body that only throws matches Callable and Supplier too, so say Runnable.
            executor.submit((Runnable) () -> {
                throw new IllegalStateException("audit log is read-only");
            });

            assertThatThrownBy(executor::waitForCompletion)
                    .isInstanceOf(ThreadContextAwareCompletionException.class)
                    .satisfies(aggregate -> assertThat(RootCauseExceptionUtil.getRootCause(aggregate))
                            .isInstanceOf(IllegalStateException.class)
                            .hasMessage("audit log is read-only"));
            executor.close();
        }
    }

    @Nested
    @DisplayName("B. ExecutorService + Runnable")
    class RunnableOnAPool {

        /*
         * Before:
         *
         *     ExecutorService pool = Executors.newFixedThreadPool(4);
         *     pool.submit(() -> reindex());
         *     pool.shutdown();
         *     pool.awaitTermination(1, TimeUnit.MINUTES);
         *
         * A pool per call site, hand-rolled shutdown bookkeeping, and a Future<?> whose
         * failure you only see if you remember to get() it.
         */

        @Test
        @DisplayName("a Runnable handed to a pool is handed to the executor unchanged; close() replaces the shutdown dance")
        void runnableOnAPool() {

            final ThrottledExecutor executor = new ThrottledExecutor("Reindex");
            final AtomicInteger reindexed = new AtomicInteger();

            final CompletableFuture<Void> done = executor.submit((Runnable) reindexed::incrementAndGet);

            executor.waitForCompletion();   // replaces shutdown() + awaitTermination(...)
            executor.close();               // no pool of your own to tear down
            assertThat(done).isCompleted();
            assertThat(reindexed).hasValue(1);
        }

        @Test
        @DisplayName("it IS an ExecutorService — code that takes one accepts it as-is")
        void itIsAnExecutorService() {

            final ThrottledExecutor executor = new ThrottledExecutor("Legacy");
            final AtomicBoolean ran = new AtomicBoolean();

            legacyMethodThatTakesAPool(executor, () -> ran.set(true));

            executor.waitForCompletion();
            assertThat(ran).isTrue();
            executor.close();
        }

        /** A method you do not want to touch yet: it only knows {@link ExecutorService}. */
        private void legacyMethodThatTakesAPool(ExecutorService pool, Runnable work) {
            pool.execute(work);
        }
    }

    @Nested
    @DisplayName("C. Callable + Future")
    class CallableWithAFuture {

        /*
         * Before:
         *
         *     Future<Integer> f = pool.submit(() -> countRows());
         *     Integer rows = f.get();     // blocks; wraps everything in ExecutionException
         *
         * A java.util.concurrent.Future can only be polled or blocked on — there is no
         * "and then".
         */

        @Test
        @DisplayName("a Callable's Future becomes a CompletableFuture you can compose")
        void callableBecomesAComposableFuture() {

            final ThrottledExecutor executor = new ThrottledExecutor("RowCount");

            // A value-returning lambda matches both submit(Callable) and submit(Supplier),
            // so a cast picks one; they behave identically for a pure lambda.
            final CompletableFuture<Integer> rows = executor.submit((Callable<Integer>) () -> 21);
            final CompletableFuture<Integer> doubled = rows.thenApply(n -> n * 2);

            assertThat(doubled.join()).isEqualTo(42);   // join(): no checked ExecutionException
            executor.close();
        }

        @Test
        @DisplayName("a checked exception from the Callable lands in the future as-is, and in the batch report")
        void checkedExceptionsLandInTheFuture() {

            final ThrottledExecutor executor = new ThrottledExecutor("RowCount");

            final CompletableFuture<Integer> rows = executor.submit((Callable<Integer>) () -> {
                throw new IOException("connection reset");
            });

            assertThat(rows).failsWithin(5, TimeUnit.SECONDS)
                    .withThrowableThat()
                    .withCauseInstanceOf(IOException.class);      // not wrapped in a RuntimeException
            assertThatThrownBy(executor::waitForCompletion)
                    .isInstanceOf(ThreadContextAwareCompletionException.class);
            executor.close();
        }
    }

    @Nested
    @DisplayName("D. Fan-out: invokeAll / a loop of submits")
    class FanOut {

        /*
         * Before:
         *
         *     List<Future<Report>> futures = pool.invokeAll(callables);
         *     for (Future<Report> f : futures) { reports.add(f.get()); }
         *
         * or the hand-rolled version: a loop of submit(), then a loop of get(). Either way
         * every task starts as soon as a worker is free, whatever it is about to allocate.
         */

        @Test
        @DisplayName("invokeAllThrottled returns the values in submission order")
        void invokeAllInSubmissionOrder() {

            final ThrottledExecutor executor = new ThrottledExecutor("Reports");
            final List<ThrottledTask<Callable<String>>> reports = List.of(
                    ThrottledTask.of(() -> "voyage", 50_000_000L, "VoyageReport"),
                    ThrottledTask.of(() -> "leg", 5_000_000L, "LegReport"),
                    ThrottledTask.of(() -> "noon", 500_000L, "NoonReport"));

            final List<String> values = executor.invokeAllThrottled(reports);

            assertThat(values).containsExactly("voyage", "leg", "noon");
            executor.close();
        }

        @Test
        @DisplayName("a policy bounds how many run at once — the thing the loop of submits could never do")
        void concurrencyIsBounded() {

            final ThrottledExecutor executor = new ThrottledExecutor("Reports");
            final AtomicInteger running = new AtomicInteger();
            final AtomicInteger highWater = new AtomicInteger();

            final List<ThrottledTask<Callable<Integer>>> reports = new ArrayList<>();
            for (int i = 0; i < 12; i++) {
                final int id = i;
                reports.add(ThrottledTask.of(() -> {
                    highWater.accumulateAndGet(running.incrementAndGet(), Math::max);
                    Thread.sleep(20);       // long enough for the batch to overlap if it could
                    running.decrementAndGet();
                    return id;
                }, 1_000L, "Report-" + i));
            }

            final List<Integer> ids = executor.invokeAllThrottled(reports, new MaxConcurrencyPolicy(2));

            assertThat(ids).containsExactlyElementsOf(IntStream.range(0, 12).boxed().toList());
            assertThat(highWater).hasValueLessThanOrEqualTo(2);
            executor.close();
        }
    }

    @Nested
    @DisplayName("E. A thread per element of a stream")
    class Streaming {

        /*
         * Before:
         *
         *     while (cursor.hasNext()) {
         *         Row row = cursor.next();
         *         new Thread(() -> process(row)).start();
         *     }
         *
         * As many threads as there are rows, all at once, and nothing to wait on afterwards.
         */

        @Test
        @DisplayName("a stream of work is pulled through a bounded window instead of a thread per element")
        void streamThroughAWindow() {

            final ThrottledExecutor executor = new ThrottledExecutor("Rows");
            final AtomicInteger processed = new AtomicInteger();

            final Iterator<ThrottledTask<Runnable>> rows = IntStream.range(0, 1_000)
                    .mapToObj(row -> ThrottledTask.of((Runnable) processed::incrementAndGet, 1_000L, "Row-" + row))
                    .iterator();

            executor.submitThrottled(rows, new MaxConcurrencyPolicy(4), 16);   // at most 16 pulled ahead

            executor.waitForCompletion();
            assertThat(processed).hasValue(1_000);
            executor.close();
        }
    }

    @Nested
    @DisplayName("F. A timed wait")
    class TimedWait {

        /*
         * Before:
         *
         *     Result r = task.get(55, TimeUnit.SECONDS);    // TimeoutException, and now what?
         *     pool.awaitTermination(55, TimeUnit.SECONDS);  // true/false, and which one hung?
         */

        @Test
        @DisplayName("waitForCompletion(Duration) reports what is still outstanding instead of throwing")
        void timedWaitReportsTheStragglers() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Upload");
            final CountDownLatch networkIsBack = new CountDownLatch(1);

            executor.submit((Callable<String>) () -> "quick");
            executor.submitThrottled(List.of(ThrottledTask.of((Runnable) () -> {
                try {
                    networkIsBack.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 1_000L, "SlowUpload")));

            final WaitResult sofar = executor.waitForCompletion(Duration.ofMillis(200));

            assertThat(sofar.complete()).isFalse();
            assertThat(sofar.outstanding()).extracting(TaskDescriptor::name).containsExactly("SlowUpload");

            networkIsBack.countDown();
            assertThat(executor.waitForCompletion(Duration.ofSeconds(10)).complete()).isTrue();
            executor.close();
        }
    }

    @Nested
    @DisplayName("G. Cancellation")
    class Cancellation {

        /*
         * Before:
         *
         *     thread.interrupt();                 // and hope the body checks the flag
         *     future.cancel(true);
         *     for (Runnable r : pool.shutdownNow()) { ((Future<?>) r).cancel(true); }
         */

        @Test
        @DisplayName("future.cancel(true) on a running task settles the future now and reports CANCELLED_AFTER_START when the body ends")
        void cancelARunningTask() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Sync");
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch ended = new CountDownLatch(1);
            final AtomicReference<TaskStatus> status = new AtomicReference<>();

            executor.addListener(new TaskExecutionListener<Void>() {
                @Override
                public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {
                    status.set(outcome.status());
                    ended.countDown();
                }
            });

            final CompletableFuture<Void> sync = executor.submit(() -> {
                started.countDown();
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();     // the virtual backend interrupts; pooled ones do not
                }
            });

            assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(sync.cancel(true)).isTrue();
            assertThat(sync).isCancelled();                  // immediately, whatever the body does next

            release.countDown();                             // let a pooled body finish on its own
            assertThat(ended.await(10, TimeUnit.SECONDS)).isTrue();
            assertThat(status).hasValue(TaskStatus.CANCELLED_AFTER_START);
            executor.close();
        }

        @Test
        @DisplayName("cancelSubtree() replaces shutdownNow() plus the loop over what it returned")
        void cancelSubtreeReplacesShutdownNow() {

            final ThrottledExecutor root = new ThrottledExecutor("Sync");
            final ThrottledExecutor child = root.newChild("Pages");
            final CountDownLatch release = new CountDownLatch(1);

            final CompletableFuture<Void> page = child.submit(() -> {
                try {
                    release.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            });

            root.cancelSubtree();                            // closes and cancels root, child, everything queued

            assertThat(page).isCancelled();
            assertThat(root.isClosed()).isTrue();
            assertThat(child.isClosed()).isTrue();
            assertThat(root.submit(() -> { }))               // a closed executor rejects through the future
                    .failsWithin(5, TimeUnit.SECONDS);
            release.countDown();
        }
    }

    @Nested
    @DisplayName("H. A Runnable that is also a Future (FutureTask, javafx.concurrent.Task)")
    class RunnableFutures {

        /*
         * Before:
         *
         *     var task = new ReportTask_GetCargoAndDistanceArray(ms);   // extends javafx.concurrent.Task
         *     ThreadPool.USER_THREAD_POOL.submit(task);
         *     List<Cargo_DistancePair> pairs = task.get();
         *
         * The task object is the Runnable AND the Future. That idiom survives unchanged.
         */

        @Test
        @DisplayName("a FutureTask (or a JavaFX Task) is submitted as the Runnable it is, and read through the Future it is")
        void runnableFutureSurvivesUnchanged() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("UserPool");
            final FutureTask<String> task = new FutureTask<>(() -> "cargo pairs");

            executor.submit(task);                           // submit(Runnable)

            assertThat(task.get(10, TimeUnit.SECONDS)).isEqualTo("cargo pairs");
            executor.waitForCompletion();
            executor.close();
        }
    }
}
