package io.github.grishagrigorov.executors;

import java.time.Duration;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code waitForCompletion(Duration)} — the bounded wait. The untimed one blocks forever on a
 * task that never returns, which for a batch talking to a socket is a hang, not a wait.
 */
@DisplayName("waitForCompletion(Duration)")
class TimedWaitTests {

    @Nested
    @DisplayName("when the work finishes in time")
    class Completes {

        @Test
        @DisplayName("returns complete, with nothing outstanding")
        void completesWithinTheBudget() {

            final ThrottledExecutor executor = new ThrottledExecutor("TimedOk");
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "A"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "B")));

            final WaitResult result = executor.waitForCompletion(Duration.ofSeconds(10));

            assertThat(result.complete()).isTrue();
            assertThat(result.outstanding()).isEmpty();
            executor.close();
        }

        @Test
        @DisplayName("still throws the aggregate when a task failed")
        void failuresStillThrow() {

            final ThrottledExecutor executor = new ThrottledExecutor("TimedFail");
            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                throw new IllegalStateException("boom");
            }, 10L, "bad")));

            assertThatThrownBy(() -> executor.waitForCompletion(Duration.ofSeconds(10)))
                    .isInstanceOf(ThreadContextAwareCompletionException.class)
                    .hasMessageContaining("boom");
            executor.close();
        }
    }

    @Nested
    @DisplayName("when the clock runs out")
    class TimesOut {

        @Test
        @DisplayName("returns not-complete and names the tasks still running, without throwing")
        void reportsOutstanding() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("TimedOut");
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch started = new CountDownLatch(1);

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                started.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 10L, "Hanging")));

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            final long before = System.nanoTime();
            final WaitResult result = executor.waitForCompletion(Duration.ofMillis(300));
            final long elapsedMillis = (System.nanoTime() - before) / 1_000_000;

            assertThat(result.complete()).isFalse();
            assertThat(result.outstanding()).extracting(TaskDescriptor::name).containsExactly("Hanging");
            assertThat(elapsedMillis).as("it waited about the budget, not forever").isLessThan(10_000);

            release.countDown();
            executor.waitForCompletion();
            executor.close();
        }

        @Test
        @DisplayName("a child's failure is still raised when the budget expires, not left to leak into the next batch")
        void childFailuresAreDrainedOnTimeout() throws Exception {

            final ThrottledExecutor root = new ThrottledExecutor("TimedChildFail");
            final ThrottledExecutor child = root.newChild("Child");
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch failed = new CountDownLatch(1);

            // The child fails; the parent then hangs, so the wait expires with the child's
            // failure already recorded but not yet reported.
            child.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                failed.countDown();
                throw new IllegalStateException("child boom");
            }, 10L, "bad")));
            assertThat(failed.await(5, TimeUnit.SECONDS)).isTrue();

            root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 10L, "Hanging")));

            assertThatThrownBy(() -> root.waitForCompletion(Duration.ofMillis(300)))
                    .as("the failure must not be swallowed by the timeout")
                    .isInstanceOf(ThreadContextAwareCompletionException.class)
                    .hasMessageContaining("child boom");

            release.countDown();
        }

        @Test
        @DisplayName("an absurdly large budget means 'wait indefinitely', not an ArithmeticException")
        void hugeBudgetsSaturate() {

            final ThrottledExecutor executor = new ThrottledExecutor("TimedHuge");
            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "A")));

            // Duration.toNanos() overflows past ~292 years; both of these exceed it.
            assertThat(executor.waitForCompletion(java.time.temporal.ChronoUnit.FOREVER.getDuration()).complete())
                    .isTrue();
            assertThat(executor.waitForCompletion(Duration.ofDays(400_000)).complete()).isTrue();
        }

        @Test
        @DisplayName("a zero or negative budget is one non-blocking check, not an immediate lie")
        void zeroBudgetChecksOnce() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("TimedZero");
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch started = new CountDownLatch(1);

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                started.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 10L, "Hanging")));

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(executor.waitForCompletion(Duration.ZERO).complete()).isFalse();

            release.countDown();
            executor.waitForCompletion();
            executor.close();
        }

        @Test
        @DisplayName("an absurdly NEGATIVE budget is also one non-blocking check, not a 292-year wait")
        void hugeNegativeBudgetsClampToZero() throws Exception {

            /*
             * The mirror of hugeBudgetsSaturate, and the case its fix missed. toNanos() overflows
             * on large negative durations too, and the ArithmeticException was read as "too large
             * to count, wait indefinitely" — so Duration.ofDays(-400_000) skipped the max(0, ...)
             * clamp entirely and became a deadline ~292 years out. A caller passing a negative
             * budget means "do not wait", which is exactly what the contract above promises.
             *
             * Run on its own thread: before the fix this does not fail, it hangs.
             */
            final ThrottledExecutor executor = new ThrottledExecutor("TimedNegative");
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch started = new CountDownLatch(1);

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                started.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 10L, "Hanging")));

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            final java.util.concurrent.atomic.AtomicReference<Boolean> complete =
                    new java.util.concurrent.atomic.AtomicReference<>();
            final Thread waiter = new Thread(
                    () -> complete.set(executor.waitForCompletion(Duration.ofDays(-400_000)).complete()),
                    "negative-budget");
            waiter.setDaemon(true);
            waiter.start();
            waiter.join(5_000);

            assertThat(waiter.isAlive())
                    .as("a negative budget must not park the caller")
                    .isFalse();
            assertThat(complete.get()).as("the work really is unfinished").isFalse();

            release.countDown();
            executor.waitForCompletion();
            executor.close();
        }

        @Test
        @DisplayName("the saturated deadline survives a negative nanoTime()")
        void saturatedDeadlineSurvivesANegativeClock() {

            /*
             * System.nanoTime() is explicitly allowed to be negative, and is on some platforms.
             * deadlineFor() uses Math.addExact precisely because of that — and then the remaining
             * budget was computed with a raw `deadline - now`, which wraps for a saturated
             * deadline and a negative now, reporting "already expired" and turning "wait
             * indefinitely" into a single non-blocking check: the exact failure the saturation
             * exists to prevent, one line further down.
             *
             * Driven directly because a negative clock cannot be arranged from a test.
             */
            assertThat(ThrottledExecutor.remainingNanos(Long.MAX_VALUE, -1_000L))
                    .as("saturated deadline, negative clock — still an indefinite wait")
                    .isEqualTo(Long.MAX_VALUE);

            assertThat(ThrottledExecutor.remainingNanos(Long.MAX_VALUE, 1_000L))
                    .as("a positive clock needs no saturation — the exact difference is still ~292 years")
                    .isEqualTo(Long.MAX_VALUE - 1_000L);

            assertThat(ThrottledExecutor.remainingNanos(500L, 200L))
                    .as("the ordinary case is untouched")
                    .isEqualTo(300L);

            assertThat(ThrottledExecutor.remainingNanos(200L, 500L))
                    .as("a spent budget is still reported as spent")
                    .isNegative();

            assertThat(ThrottledExecutor.remainingNanos(Long.MIN_VALUE + 1, Long.MAX_VALUE))
                    .as("and the other end cannot wrap into a positive wait")
                    .isNegative();
        }
    }
}
