package io.github.grishagrigorov.executors.observability;

/**
 * The fixed decade histogram every node's durations are bucketed into. Nine buckets,
 * upper-exclusive, from {@code <1µs} to {@code >=10s} — enough to see "most tasks take
 * 10 ms, a few take 4 s" at O(1) memory forever.
 */
public final class DurationHistogram {

    /** Number of buckets. */
    public static final int BUCKETS = 9;

    /** Upper-exclusive thresholds in nanoseconds; anything at or past the last is bucket 8. */
    private static final long[] THRESHOLDS = {
            1_000L,             // < 1 µs
            10_000L,            // < 10 µs
            100_000L,           // < 100 µs
            1_000_000L,         // < 1 ms
            10_000_000L,        // < 10 ms
            100_000_000L,       // < 100 ms
            1_000_000_000L,     // < 1 s
            10_000_000_000L,    // < 10 s
    };

    /** Human labels, index-aligned with the buckets. */
    public static final String[] LABELS = {
            "<1µs", "<10µs", "<100µs", "<1ms", "<10ms", "<100ms", "<1s", "<10s", ">=10s"
    };

    private DurationHistogram() {
    }

    /**
     * @param durationNanos a non-negative body duration
     * @return the bucket index, 0..8
     */
    public static int bucketFor(long durationNanos) {
        for (int i = 0; i < THRESHOLDS.length; i++) {
            if (durationNanos < THRESHOLDS[i]) {
                return i;
            }
        }
        return BUCKETS - 1;
    }
}
