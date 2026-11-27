package io.github.grishagrigorov.executors;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Streaming submission: the batch arrives through an {@code Iterator} and only a bounded window
 * of it is ever materialised, so a producer that discovers work incrementally does not have to
 * build the whole list first.
 */
@DisplayName("submitThrottled(Iterator, ...)")
class StreamingSubmissionTests {

    @Nested
    @DisplayName("the window")
    class Window {

        @Test
        @DisplayName("only the window is pulled ahead — the rest of the stream stays untouched while work runs")
        void pullsLazily() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Lazy");
            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch firstStarted = new CountDownLatch(1);
            final AtomicInteger ran = new AtomicInteger();

            final List<ThrottledTask<Runnable>> batch = new ArrayList<>();
            batch.add(ThrottledTask.of(() -> {
                firstStarted.countDown();
                await(release);
                ran.incrementAndGet();
            }, 10L, "blocker"));
            for (int i = 1; i < 10; i++) {
                batch.add(ThrottledTask.of(ran::incrementAndGet, 10L, "t" + i));
            }

            final CountingIterator<Runnable> stream = new CountingIterator<>(batch.iterator());
            final Thread coordinator = new Thread(
                    () -> executor.submitThrottled(stream, new MaxConcurrencyPolicy(1), 3), "streaming");
            coordinator.start();

            assertThat(firstStarted.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(200);                       // let the refill settle

            assertThat(stream.pulled.get())
                    .as("one window's worth, not the whole stream")
                    .isLessThanOrEqualTo(4)
                    .isLessThan(10);

            release.countDown();

            // Not join-and-hope: a coordinator still parked here means the streaming refill
            // wedged, and the assertions below would otherwise race it rather than fail.
            coordinator.join(10_000);
            assertThat(coordinator.isAlive()).as("the coordinator drained the stream").isFalse();

            executor.waitForCompletion();

            assertThat(ran.get()).as("everything still ran").isEqualTo(10);
            assertThat(stream.pulled.get()).isEqualTo(10);
            executor.close();
        }

        @Test
        @DisplayName("a window as large as the stream orders exactly like the List overload")
        void fullWindowMatchesTheListOverload() throws Exception {

            final List<Long> streamed = completionOrder(3);
            final List<Long> listed = completionOrderFromList();

            assertThat(streamed).isEqualTo(listed).containsExactly(300L, 200L, 100L);
        }

        @Test
        @DisplayName("a window below one is rejected — there would be nothing to choose between")
        void windowMustBePositive() {

            final ThrottledExecutor executor = new ThrottledExecutor("BadWindow");

            assertThatThrownBy(() -> executor.submitThrottled(
                    List.<ThrottledTask<Runnable>>of().iterator(), AdmissionPolicy.defaultPolicy(), 0))
                    .isInstanceOf(IllegalArgumentException.class);
            executor.close();
        }

        @Test
        @DisplayName("an empty stream is a no-op")
        void emptyStream() {

            final ThrottledExecutor executor = new ThrottledExecutor("EmptyStream");

            executor.submitThrottled(List.<ThrottledTask<Runnable>>of().iterator());
            executor.waitForCompletion();
            executor.close();
        }
    }

    @Nested
    @DisplayName("validation")
    class Validation {

        @Test
        @DisplayName("an unsupported job is caught when it is pulled — which is after earlier tasks have already run")
        void validationIsPerCandidate() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("BadJob");
            final AtomicInteger ran = new AtomicInteger();

            final List<ThrottledTask<Object>> batch = List.of(
                    ThrottledTask.<Object>of((Runnable) ran::incrementAndGet, 10L, "good-1"),
                    ThrottledTask.<Object>of((Runnable) ran::incrementAndGet, 10L, "good-2"),
                    ThrottledTask.<Object>of("not a job", 10L, "bad"));

            assertThatThrownBy(() -> executor.submitThrottled(batch.iterator(), new MaxConcurrencyPolicy(1), 1))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("Unsupported job type");

            executor.waitForCompletion();
            assertThat(ran.get())
                    .as("the fail-fast-with-nothing-claimed property of the List overload cannot hold for a stream")
                    .isPositive();
            executor.close();
        }
    }

    /** Runs a size-ordered batch through the streaming overload and reports completion order. */
    private static List<Long> completionOrder(int window) throws Exception {

        final ThrottledExecutor executor = new ThrottledExecutor("StreamOrder");
        final List<Long> order = new CopyOnWriteArrayList<>();

        executor.submitThrottled(sizedBatch(order).iterator(), new MaxConcurrencyPolicy(1), window);
        executor.waitForCompletion();
        executor.close();

        return order;
    }

    /** The same batch through the List overload. */
    private static List<Long> completionOrderFromList() throws Exception {

        final ThrottledExecutor executor = new ThrottledExecutor("ListOrder");
        final List<Long> order = new CopyOnWriteArrayList<>();

        executor.submitThrottled(sizedBatch(order), new MaxConcurrencyPolicy(1));
        executor.waitForCompletion();
        executor.close();

        return order;
    }

    /** Three tasks listed smallest-first; the default LARGEST_FIRST order must reverse them. */
    private static List<ThrottledTask<Runnable>> sizedBatch(List<Long> order) {
        return List.of(
                ThrottledTask.of(() -> order.add(100L), 100L, "small"),
                ThrottledTask.of(() -> order.add(200L), 200L, "medium"),
                ThrottledTask.of(() -> order.add(300L), 300L, "large"));
    }

    private static void await(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** An iterator that records how much of the stream has actually been consumed. */
    private static final class CountingIterator<T> implements Iterator<ThrottledTask<T>> {

        final AtomicInteger pulled = new AtomicInteger();
        private final Iterator<ThrottledTask<T>> delegate;

        CountingIterator(Iterator<ThrottledTask<T>> delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public ThrottledTask<T> next() {
            pulled.incrementAndGet();
            return delegate.next();
        }
    }
}
