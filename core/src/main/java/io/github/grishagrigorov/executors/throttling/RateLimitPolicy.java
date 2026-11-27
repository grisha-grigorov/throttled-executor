package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayDeque;

/**
 * Admits at most {@code maxStarts} task <i>starts</i> per sliding time
 * {@code window}. Size estimates are ignored; only start frequency matters.
 *
 * <p>Start timestamps are recorded by {@link #onTaskStart} and evicted lazily
 * once they fall out of the window. State is synchronized — an instance may be
 * shared across executors to enforce a global rate.</p>
 *
 * <p>Time-based denial cannot be lifted by a task completing, so the framework's
 * timed {@code awaitStateChange} doubles as the polling mechanism: a blocked
 * coordinator re-checks at least once per poll interval.</p>
 *
 * <pre>{@code
 * // At most 10 task starts per second, wall clock:
 * new RateLimitPolicy(10, Duration.ofSeconds(1), InstantSource.system());
 * }</pre>
 */
public final class RateLimitPolicy implements AdmissionPolicy {

    private final int maxStarts;
    private final Duration window;
    private final InstantSource clock;

    /** Start timestamps (epoch millis), oldest first; guarded by its own monitor. */
    private final ArrayDeque<Long> startTimestamps = new ArrayDeque<>();

    /**
     * @param maxStarts maximum task starts within any single window
     * @param window    sliding window length
     * @param clock     time source; inject a fake for deterministic tests
     */
    public RateLimitPolicy(int maxStarts, Duration window, InstantSource clock) {
        this.maxStarts = maxStarts;
        this.window = window;
        this.clock = clock;
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        final long cutoffMillis = clock.millis() - window.toMillis();
        synchronized (startTimestamps) {
            // Lazily evict starts that have slid out of the window.
            while (!startTimestamps.isEmpty() && startTimestamps.peekFirst() <= cutoffMillis) {
                startTimestamps.pollFirst();
            }
            return startTimestamps.size() < maxStarts;
        }
    }

    @Override
    public void onTaskStart(ThrottledTask<?> task) {
        synchronized (startTimestamps) {
            startTimestamps.addLast(clock.millis());
        }
    }
}
