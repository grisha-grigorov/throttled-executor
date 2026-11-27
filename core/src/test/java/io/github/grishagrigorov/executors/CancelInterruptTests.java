package io.github.grishagrigorov.executors;

import java.util.concurrent.CompletableFuture;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;

import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cancelling a running task interrupts it — on the virtual backend only, where the thread
 * running the body belongs to that task and to nothing else.
 */
@DisplayName("cancel — interruption")
class CancelInterruptTests {

    @Nested
    @DisplayName("on the virtual backend")
    class Virtual {

        @Test
        @DisplayName("a running task is interrupted, so a blocking body actually stops")
        void runningTaskIsInterrupted() throws Exception {

            Assumptions.assumeTrue(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                    "virtual threads unavailable on this runtime");

            final ThrottledExecutor executor = ThrottledExecutor.withBackend(
                    "InterruptVirtual", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicBoolean sawInterrupt = new AtomicBoolean();

            final CompletableFuture<Void> future = executor.submit(() -> {
                started.countDown();
                try {
                    // Would block for half a minute if nothing interrupted it.
                    Thread.sleep(30_000);
                } catch (InterruptedException expected) {
                    sawInterrupt.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            future.cancel(true);

            assertThat(finished.await(5, TimeUnit.SECONDS))
                    .as("the body stopped instead of sleeping out its 30s")
                    .isTrue();
            assertThat(sawInterrupt).isTrue();
            executor.close();
        }

        @Test
        @DisplayName("cancel(false) does NOT interrupt — the caller asked to let it finish")
        void cancelWithoutInterruptLetsTheBodyFinish() throws Exception {

            /*
             * Future.cancel's contract, which this had stopped honouring: "if false, in-progress
             * tasks are allowed to complete". The flag was accepted and dropped, and once
             * cancellation learned to interrupt, cancel(false) started interrupting too.
             *
             * It is not a theoretical break. Cancelling with false is what a caller does to a
             * task that is mid-write or mid-statement precisely so it can close cleanly;
             * interrupting it instead hands them a ClosedByInterruptException or an aborted
             * transaction. cancelSubtree() passes true, so honouring the flag costs nothing.
             */
            Assumptions.assumeTrue(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                    "virtual threads unavailable on this runtime");

            final ThrottledExecutor executor = ThrottledExecutor.withBackend(
                    "NoInterruptVirtual", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicBoolean sawInterrupt = new AtomicBoolean();

            final CompletableFuture<Void> future = executor.submit(() -> {
                started.countDown();
                try {
                    // Released by the test, not by an interrupt. If the flag is ignored this
                    // throws instead of waiting.
                    if (!release.await(10, TimeUnit.SECONDS)) {
                        throw new IllegalStateException("never released");
                    }
                } catch (InterruptedException unexpected) {
                    sawInterrupt.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();

            assertThat(future.cancel(false))
                    .as("the future is still cancelled — only the interrupt is withheld")
                    .isTrue();
            assertThat(future.isCancelled()).isTrue();

            // Give an interrupt that should not exist time to land before releasing.
            assertThat(finished.await(300, TimeUnit.MILLISECONDS))
                    .as("nothing should have knocked the body out of its wait")
                    .isFalse();

            release.countDown();

            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(sawInterrupt)
                    .as("cancel(false) promised the body it could run to its own end")
                    .isFalse();
            executor.close();
        }
    }

    @Nested
    @DisplayName("racing the listener fan-out")
    class DuringOnStart {

        @Test
        @DisplayName("a cancellation arriving while onStart listeners run still interrupts the body")
        void cancelDuringOnStartStillInterrupts() throws Exception {

            org.junit.jupiter.api.Assumptions.assumeTrue(
                    ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                    "virtual threads unavailable on this runtime");

            final ThrottledExecutor executor = ThrottledExecutor.withBackend(
                    "InterruptDuringOnStart", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

            final CountDownLatch insideOnStart = new CountDownLatch(1);
            final AtomicBoolean cancelDelivered = new AtomicBoolean();
            final AtomicBoolean sawInterrupt = new AtomicBoolean();
            final CountDownLatch finished = new CountDownLatch(1);

            executor.addListener(new io.github.grishagrigorov.executors.spi.TaskExecutionListener<Void>() {
                @Override
                public void onStart(io.github.grishagrigorov.executors.spi.TaskDescriptor task, Void state) {
                    insideOnStart.countDown();
                    // Spin rather than await: a latch would throw InterruptedException, which the
                    // listener guard swallows AND which clears the very flag under test.
                    while (!cancelDelivered.get()) {
                        Thread.onSpinWait();
                    }
                }
            });

            final CompletableFuture<Void> future = executor.submit(() -> {
                try {
                    Thread.sleep(30_000);
                } catch (InterruptedException expected) {
                    sawInterrupt.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(insideOnStart.await(5, TimeUnit.SECONDS)).isTrue();
            future.cancel(true);
            cancelDelivered.set(true);

            assertThat(finished.await(5, TimeUnit.SECONDS))
                    .as("the body slept out its 30s: the cancellation found no thread to interrupt")
                    .isTrue();
            assertThat(sawInterrupt).isTrue();
            executor.close();
        }
    }

    @Nested
    @DisplayName("racing the listener fan-out with cancel(false)")
    class FanOutWithoutInterrupt {

        @Test
        @DisplayName("a cancel(false) arriving while onStart listeners run does NOT interrupt the body")
        void cancelFalseDuringOnStartDoesNotInterrupt() throws Exception {

            /*
             * The post-fan-out re-check exists for a cancel(true) that read `runner` before it
             * was published: it interrupts when the state is CANCEL_REQUESTED. But the
             * STARTED -> CANCEL_REQUESTED transition is made for cancel(false) too, and the
             * flag was recorded nowhere — so a cancel(false) landing in this window was
             * interrupting after all, on the body's very first blocking call, contradicting
             * what cancel(false) promises and what cancelWithoutInterruptLetsTheBodyFinish
             * asserts for the plain case.
             */
            org.junit.jupiter.api.Assumptions.assumeTrue(
                    ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                    "virtual threads unavailable on this runtime");

            final ThrottledExecutor executor = ThrottledExecutor.withBackend(
                    "NoInterruptDuringOnStart", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);

            final CountDownLatch insideOnStart = new CountDownLatch(1);
            final AtomicBoolean cancelDelivered = new AtomicBoolean();
            final CountDownLatch release = new CountDownLatch(1);
            final AtomicBoolean sawInterrupt = new AtomicBoolean();
            final CountDownLatch finished = new CountDownLatch(1);

            executor.addListener(new io.github.grishagrigorov.executors.spi.TaskExecutionListener<Void>() {
                @Override
                public void onStart(io.github.grishagrigorov.executors.spi.TaskDescriptor task, Void state) {
                    insideOnStart.countDown();
                    while (!cancelDelivered.get()) {
                        Thread.onSpinWait();
                    }
                }
            });

            final CompletableFuture<Void> future = executor.submit(() -> {
                try {
                    // The first blocking call after the fan-out: a wrongly delivered interrupt
                    // throws here at once, rather than waiting on the latch.
                    release.await(10, TimeUnit.SECONDS);
                } catch (InterruptedException unexpected) {
                    sawInterrupt.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(insideOnStart.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(future.cancel(false)).isTrue();
            cancelDelivered.set(true);

            assertThat(finished.await(300, TimeUnit.MILLISECONDS))
                    .as("nothing should have knocked the body out of its wait")
                    .isFalse();
            release.countDown();

            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(sawInterrupt)
                    .as("cancel(false) promised the body it could run to its own end — in this window too")
                    .isFalse();
            executor.close();
        }
    }

    @Nested
    @DisplayName("on the fork-join backend")
    class ForkJoin {

        @Test
        @DisplayName("a running task is NOT interrupted — the flag would leak into a pooled worker")
        void runningTaskIsNotInterrupted() throws Exception {

            // Explicitly FORK_JOIN, not the default: the suite also runs under
            // -Dio.github.grishagrigorov.executors.backend=virtual, where the default is the one
            // backend that DOES interrupt, and this test would then contradict itself.
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("InterruptForkJoin", ExecutorBackend.FORK_JOIN);
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch finished = new CountDownLatch(1);
            final AtomicBoolean sawInterrupt = new AtomicBoolean();

            final CompletableFuture<Void> future = executor.submit(() -> {
                started.countDown();
                try {
                    // Long enough that an interrupt, if one came, would be observed here.
                    sawInterrupt.set(!release.await(2, TimeUnit.SECONDS));
                } catch (InterruptedException interrupted) {
                    sawInterrupt.set(true);
                    Thread.currentThread().interrupt();
                } finally {
                    finished.countDown();
                }
            });

            assertThat(started.await(5, TimeUnit.SECONDS)).isTrue();
            future.cancel(true);
            Thread.sleep(300);
            release.countDown();

            assertThat(finished.await(5, TimeUnit.SECONDS)).isTrue();
            assertThat(sawInterrupt)
                    .as("the pooled worker outlives the task, so its interrupt flag is not ours to set")
                    .isFalse();
            executor.close();
        }
    }

    @Nested
    @DisplayName("the harness claim")
    class TheClaim {

        @Test
        @DisplayName("is held until the body actually ends, not until the future settles")
        void claimOutlivesAnEarlySettlement() throws Exception {

            /*
             * cancel() settles the future at once and the harness release was wired to the
             * future — so on a pooled backend, where the interrupt is a no-op and the body runs
             * to its natural end, the claim (and with it a memory budget's bytes, a permit, an
             * exclusive holder's lock) was handed back while the body was still running. The
             * harness then believed the pool empty and admitted new work beside a body that had
             * not finished. The release belongs to the END of the body, whichever way it ends.
             */
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("ClaimHeld", ExecutorBackend.FORK_JOIN);
            final CountDownLatch aStarted = new CountDownLatch(1);
            final CountDownLatch releaseA = new CountDownLatch(1);
            final CountDownLatch bStarted = new CountDownLatch(1);
            final AdmissionPolicy oneAtATime = new MaxConcurrencyPolicy(1);

            final java.util.List<CompletableFuture<?>> a = executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> {
                        aStarted.countDown();
                        try {
                            releaseA.await(10, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, 10L, "A")), oneAtATime);
            assertThat(aStarted.await(5, TimeUnit.SECONDS)).isTrue();

            // Settles the future now; the body keeps running (no interrupt on a pooled worker).
            assertThat(a.get(0).cancel(true)).isTrue();

            final Thread coordinator = new Thread(() -> executor.submitThrottled(
                    List.of(ThrottledTask.<Runnable>of(bStarted::countDown, 10L, "B")), oneAtATime), "b");
            coordinator.setDaemon(true);
            coordinator.start();

            assertThat(bStarted.await(400, TimeUnit.MILLISECONDS))
                    .as("A's body is still running, so its claim is still held and B must wait")
                    .isFalse();

            releaseA.countDown();

            assertThat(bStarted.await(5, TimeUnit.SECONDS)).as("and B goes once A has ended").isTrue();
            coordinator.join(5_000);
            executor.close();
        }
    }

    @Nested
    @DisplayName("what does not change")
    class Unchanged {

        @Test
        @DisplayName("a task cancelled before it starts is still never run, and never interrupts anything")
        void cancelledBeforeStart() throws Exception {

            Assumptions.assumeTrue(ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                    "virtual threads unavailable on this runtime");

            final ThrottledExecutor executor = ThrottledExecutor.withBackend(
                    "InterruptQueued", ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
            final AtomicBoolean ran = new AtomicBoolean();

            final TaskRunHandle handle = TaskRunHandle.prepare(executor, ran);
            assertThat(handle.cancel()).isTrue();
            handle.runAnyway();

            assertThat(ran).as("the body never ran").isFalse();
            executor.close();
        }
    }

    /** Drives the completion state machine directly, without a pool deciding when things happen. */
    private record TaskRunHandle(ThrottledExecutor.TaskRun<Void> run) {

        static TaskRunHandle prepare(ThrottledExecutor executor, AtomicBoolean ran) {
            return new TaskRunHandle(executor.newRun(() -> {
                ran.set(true);
                return null;
            }, "job", "Queued", 10L));
        }

        boolean cancel() {
            return run.future().cancel(true);
        }

        void runAnyway() {
            run.execute();
        }
    }
}
