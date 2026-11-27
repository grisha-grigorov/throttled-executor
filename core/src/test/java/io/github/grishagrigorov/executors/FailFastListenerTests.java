package io.github.grishagrigorov.executors;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code FailFastListener} — stop the batch on the first failure. There is no reason to build
 * nine hundred more reports after the database went away.
 */
@DisplayName("FailFastListener")
class FailFastListenerTests {

    @Nested
    @DisplayName("cancelling")
    class Cancelling {

        @Test
        @DisplayName("the first failure cancels the rest of the batch")
        void firstFailureStopsTheBatch() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("FailFast", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new FailFastListener());

            final AtomicInteger ran = new AtomicInteger();

            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> {
                        throw new IllegalStateException("database gone");
                    }, 900L, "first"),
                    ThrottledTask.<Runnable>of(ran::incrementAndGet, 100L, "second"),
                    ThrottledTask.<Runnable>of(ran::incrementAndGet, 50L, "third")));

            assertThatThrownBy(executor::waitForCompletion)
                    .isInstanceOf(ThreadContextAwareCompletionException.class);

            assertThat(ran.get()).as("nothing after the failure ran").isZero();
            assertThat(executor.isClosed()).isTrue();
        }

        @Test
        @DisplayName("a clean batch is untouched")
        void cleanBatchIsUntouched() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("FailFastClean", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new FailFastListener());

            final AtomicInteger ran = new AtomicInteger();
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "a"),
                    ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "b")));
            executor.waitForCompletion();

            assertThat(ran.get()).isEqualTo(2);
            assertThat(executor.isClosed()).isFalse();
            executor.close();
        }
    }

    @Nested
    @DisplayName("re-entrancy")
    class Reentrancy {

        @Test
        @DisplayName("it cancels once — the cancellations it causes must not make it cancel again")
        void cancelsOnlyOnce() {

            // cancelSubtree() runs every listener's onComplete synchronously on this thread, so
            // a naive implementation re-enters itself once per cancelled task.
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("FailFastOnce", ExecutorBackend.CALLER_RUNS);
            final FailFastListener failFast = new FailFastListener();
            executor.addListener(failFast);

            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> {
                        throw new IllegalStateException("boom");
                    }, 900L, "first"),
                    ThrottledTask.<Runnable>of(() -> { }, 100L, "second")));

            assertThatThrownBy(executor::waitForCompletion)
                    .isInstanceOf(ThreadContextAwareCompletionException.class);

            assertThat(failFast.triggered()).isTrue();
            assertThat(failFast.cancellations())
                    .as("one cancellation, however many tasks it took down")
                    .isEqualTo(1);
        }
    }
}
