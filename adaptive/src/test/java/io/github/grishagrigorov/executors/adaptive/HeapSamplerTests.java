package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The sampler's session contract, driven directly. {@code begin} hands back a session id and
 * {@code end} only answers for the session it is asked about: a never-begun, already-ended,
 * stale or post-close {@code end} is "not measured" ({@code -1}), never a zero that the
 * listener could mistake for a real reading — a solo heap peak of {@code 0} is a genuine
 * measurement and, on the virtual backend, the only one a task gets.
 */
class HeapSamplerTests {

    /**
     * The fake heap the sampler thread reads. Each test parks it at the session's baseline
     * before {@code begin}: the thread starts on the first {@code begin} and ticks once right
     * away, and a reading below the baseline would lower the running low (a reading above it
     * would raise the peak), so the assertions would depend on whether that tick landed
     * before or after {@code end}.
     */
    private final AtomicLong heap = new AtomicLong();

    /** A sampler whose thread, if ever started, reads this test's fake heap. */
    private HeapSampler sampler() {
        return new HeapSampler(heap::get, Duration.ofSeconds(1));
    }

    @Test
    @DisplayName("end() without a session is not measured, not zero")
    void endWithoutSessionIsNotMeasured() {
        final HeapSampler sampler = sampler();

        assertThat(sampler.end(0L, 100)).isEqualTo(-1L);
        assertThat(sampler.end(42L, 100)).isEqualTo(-1L);
    }

    @Test
    @DisplayName("a session answers once: the second end() for it is not measured")
    void sessionAnswersOnce() {
        final HeapSampler sampler = sampler();
        heap.set(100);

        final long session = sampler.begin(100);
        assertThat(sampler.end(session, 164)).isEqualTo(64L);
        assertThat(sampler.end(session, 200)).isEqualTo(-1L);
        sampler.close();
    }

    @Test
    @DisplayName("after close() begin() starts nothing and end() is not measured")
    void closedSamplerMeasuresNothing() {
        final HeapSampler sampler = sampler();
        sampler.close();

        final long session = sampler.begin(100);
        assertThat(sampler.end(session, 164)).isEqualTo(-1L);
    }

    @Test
    @DisplayName("a stale end() neither consumes nor answers for a newer session")
    void staleEndLeavesTheNewerSessionAlone() {
        final HeapSampler sampler = sampler();
        heap.set(200);                                  // a tick neither lowers the second session's low nor raises its peak

        final long first = sampler.begin(100);
        final long second = sampler.begin(200);

        assertThat(sampler.end(first, 150)).as("the first session was superseded").isEqualTo(-1L);
        assertThat(sampler.end(second, 260)).as("the second session is still open").isEqualTo(60L);
        sampler.close();
    }
}
