package io.github.grishagrigorov.executors;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code submitThrottled} hands back the futures it creates, in the order the caller listed the
 * tasks — not the order the policy chose to admit them.
 */
@DisplayName("submitThrottled — the futures")
class SubmitThrottledFuturesTests {

    @Nested
    @DisplayName("ordering")
    class Ordering {

        @Test
        @DisplayName("one future per task, in SUBMISSION order, even when the policy admits in the opposite order")
        void submissionOrderNotAdmissionOrder() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("SubmissionOrder");
            final List<ThrottledTask<Callable<String>>> batch = List.of(
                    ThrottledTask.of(() -> "first", 10L, "A"),
                    ThrottledTask.of(() -> "second", 20L, "B"),
                    ThrottledTask.of(() -> "third", 30L, "C"));

            // Smallest first: the exact reverse of the batch order, so an implementation that
            // appended futures as tasks were admitted would hand them back as C, B, A.
            final List<CompletableFuture<?>> futures = executor.submitThrottled(batch, smallestFirst());
            executor.waitForCompletion();

            assertThat(futures).hasSize(3);
            assertThat(futures.get(0).get()).isEqualTo("first");
            assertThat(futures.get(1).get()).isEqualTo("second");
            assertThat(futures.get(2).get()).isEqualTo("third");
            executor.close();
        }

        @Test
        @DisplayName("the same task instance submitted twice gets two distinct futures, one per position")
        void duplicateInstanceGetsOneFuturePerPosition() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("DuplicateInstance");
            final ThrottledTask<Callable<String>> once = ThrottledTask.of(() -> "x", 10L, "A");

            final List<CompletableFuture<?>> futures = executor.submitThrottled(List.of(once, once));
            executor.waitForCompletion();

            assertThat(futures).hasSize(2);
            assertThat(futures.get(0)).isNotSameAs(futures.get(1));
            assertThat(futures.get(0).get()).isEqualTo("x");
            assertThat(futures.get(1).get()).isEqualTo("x");
            executor.close();
        }

        @Test
        @DisplayName("an empty batch returns an empty list, not null")
        void emptyBatch() {

            final ThrottledExecutor executor = new ThrottledExecutor("EmptyBatch");

            assertThat(executor.submitThrottled(List.<ThrottledTask<Runnable>>of())).isEmpty();
            executor.close();
        }
    }

    @Nested
    @DisplayName("results and failures")
    class Results {

        @Test
        @DisplayName("a Runnable task's future completes with null; a failing task's future completes exceptionally")
        void runnableAndFailure() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Results");
            final List<ThrottledTask<Runnable>> batch = List.of(
                    ThrottledTask.of(() -> { }, 10L, "ok"),
                    ThrottledTask.of(() -> {
                        throw new IllegalStateException("boom");
                    }, 10L, "bad"));

            final List<CompletableFuture<?>> futures = executor.submitThrottled(batch);

            assertThat(futures.get(0).get(5, TimeUnit.SECONDS)).isNull();
            assertThat(futures.get(1)).failsWithin(5, TimeUnit.SECONDS)
                    .withThrowableThat().withMessageContaining("boom");
            executor.close();
        }
    }

    @Nested
    @DisplayName("invokeAllThrottled")
    class InvokeAll {

        @Test
        @DisplayName("waits and returns the values in submission order")
        void returnsValuesInSubmissionOrder() {

            final ThrottledExecutor executor = new ThrottledExecutor("InvokeAll");
            final List<ThrottledTask<Callable<Integer>>> batch = List.of(
                    ThrottledTask.of(() -> 1, 10L, "A"),
                    ThrottledTask.of(() -> 2, 20L, "B"),
                    ThrottledTask.of(() -> 3, 30L, "C"));

            assertThat(executor.invokeAllThrottled(batch, smallestFirst())).containsExactly(1, 2, 3);
            executor.close();
        }

        @Test
        @DisplayName("called from inside a task of the same executor, it returns instead of deadlocking")
        void insideItsOwnTaskItStillReturns() throws Exception {

            /*
             * Nested submission is first-class here, and this method's javadoc invites exactly
             * this use — yet its hidden waitForCompletion() joined every future the executor
             * tracks, the calling task's own included. That future settles only when the caller
             * returns, which it cannot do until the join returns: a deadlock on every backend,
             * and a hang of the submitting thread on CALLER_RUNS. From inside its own subtree it
             * must wait for the batch it submitted, not for itself.
             */
            final ThrottledExecutor executor = new ThrottledExecutor("InvokeAllNested");

            final CompletableFuture<List<Integer>> outer = executor.submit((Supplier<List<Integer>>) () ->
                    executor.invokeAllThrottled(List.of(
                            ThrottledTask.<Callable<Integer>>of(() -> 1, 10L, "one"),
                            ThrottledTask.<Callable<Integer>>of(() -> 2, 20L, "two")), new MaxConcurrencyPolicy(2)));

            assertThat(outer.get(10, TimeUnit.SECONDS)).containsExactly(1, 2);

            executor.waitForCompletion();
            executor.close();
        }

        @Test
        @DisplayName("and from inside, a failing task still surfaces as the aggregate — once")
        void insideItsOwnTaskFailuresStillAggregate() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("InvokeAllNestedFail");
            final AtomicReference<Throwable> seenInside = new AtomicReference<>();

            executor.submit((Runnable) () -> {
                try {
                    executor.invokeAllThrottled(List.of(
                            ThrottledTask.<Callable<Integer>>of(() -> { throw new IllegalStateException("boom"); }, 10L, "bad")),
                            new MaxConcurrencyPolicy(2));
                } catch (Throwable t) {
                    seenInside.set(t);
                }
            }).get(10, TimeUnit.SECONDS);

            assertThat(seenInside.get())
                    .as("the caller inside the task gets the documented aggregate")
                    .isInstanceOf(ThreadContextAwareCompletionException.class)
                    .hasRootCauseMessage("boom");

            assertThat(executor.waitForCompletion())
                    .as("and the outer wait does not report the same failure a second time")
                    .isTrue();
            executor.close();
        }

        @Test
        @DisplayName("an interrupted submission gives up promptly instead of waiting out what it already started")
        void interruptedSubmissionDoesNotWaitForTheRest() throws Exception {

            /*
             * The javadoc promises that an interrupt restores the flag and throws
             * CancellationException. It did — eventually. waitForCompletion() ran first and
             * blocks on CompletableFuture.join(), which is uninterruptible, so an already-set
             * interrupt flag did nothing to shorten it: a shutdown interrupting a submitter
             * mid-batch abandoned the un-submitted tail and then sat waiting for every task it
             * had already started.
             *
             * Here the running task holds for ten seconds. The interrupted caller must be back
             * long before that, so the assertion is on the clock, not on the exception alone.
             */
            final ThrottledExecutor executor = new ThrottledExecutor("InvokeAllInterrupted");

            final CountDownLatch running = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            final List<ThrottledTask<Callable<Integer>>> batch = List.of(
                    // Sized so the DEFAULT order (largest first) admits the holding task first:
                    // the other one then cannot be admitted, which is what leaves the submitter
                    // parked waiting for a slot when the interrupt arrives.
                    ThrottledTask.of(() -> {
                        running.countDown();
                        // Long enough that waiting this out would be unmistakable.
                        release.await(10, TimeUnit.SECONDS);
                        return 1;
                    }, 20L, "Holding"),
                    ThrottledTask.of(() -> 2, 10L, "Blocked"));

            final AtomicReference<Throwable> thrown = new AtomicReference<>();
            final Thread submitter = new Thread(() -> {
                try {
                    executor.invokeAllThrottled(batch, new MaxConcurrencyPolicy(1));
                } catch (Throwable t) {
                    thrown.set(t);
                }
            }, "interrupted-submitter");
            submitter.setDaemon(true);
            submitter.start();

            assertThat(running.await(5, TimeUnit.SECONDS)).as("the first task is in flight").isTrue();
            Thread.sleep(200);                 // the second is now genuinely waiting for admission
            submitter.interrupt();

            submitter.join(4_000);

            assertThat(submitter.isAlive())
                    .as("it must not still be parked waiting for the ten-second task")
                    .isFalse();
            assertThat(thrown.get())
                    .as("and it reports the interruption for what it was")
                    .isInstanceOf(java.util.concurrent.CancellationException.class);

            release.countDown();
            executor.close();
        }
    }

    @Nested
    @DisplayName("interruption")
    class Interruption {

        @Test
        @DisplayName("the abandoned tasks come back as cancelled futures, so the list still matches the batch")
        void abandonedTasksComeBackCancelled() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Interrupted");
            final List<ThrottledTask<Runnable>> batch = List.of(
                    ThrottledTask.of(() -> { }, 10L, "A"),
                    ThrottledTask.of(() -> { }, 10L, "B"));

            final AtomicReference<List<CompletableFuture<?>>> handed = new AtomicReference<>();
            final CountDownLatch inside = new CountDownLatch(1);

            // A policy that refuses everything AND claims one task in flight, so the harness
            // liveness floor never fires and the coordinator is guaranteed to block.
            final Thread coordinator = new Thread(() -> {
                inside.countDown();
                handed.set(executor.submitThrottled(batch, refuseEverything()));
            }, "coordinator");
            coordinator.start();

            assertThat(inside.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(200);            // let it reach the wait
            coordinator.interrupt();
            coordinator.join(5_000);

            assertThat(handed.get()).as("a future per task even though none ran").hasSize(2);
            assertThat(handed.get()).allSatisfy(f -> assertThat(f).isCancelled());
            executor.close();
        }
    }

    /** Smallest size first — the reverse of the default LARGEST_FIRST. */
    private static AdmissionPolicy smallestFirst() {
        final AdmissionPolicy delegate = new MaxConcurrencyPolicy(1);
        return new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return delegate.admit(candidate, snapshot);
            }

            @Override
            public Comparator<ThrottledTask<?>> selectionOrder() {
                return Comparator.comparingLong(task -> task.size);
            }
        };
    }

    /** Refuses every candidate and reports one task in flight, which disables the liveness floor. */
    private static AdmissionPolicy refuseEverything() {
        return new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return false;
            }

            @Override
            public int inFlightTaskCount() {
                return 1;
            }
        };
    }
}
