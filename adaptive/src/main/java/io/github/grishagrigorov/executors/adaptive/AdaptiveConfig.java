package io.github.grishagrigorov.executors.adaptive;

import java.time.Duration;
import java.util.Objects;

/**
 * Tuning knobs for {@link AdaptiveSizing}.
 *
 * <p>The last two decide how long a measurement stays trusted. A learned size describes the
 * code that was measured; after a release, or simply after a long time, it may describe
 * nothing that still runs. Samples outside {@code version} / {@code maxSampleAge} are
 * ignored by the sizing, and a task whose samples have all gone stale is treated as never
 * seen — quarantined and measured again on its next run.</p>
 *
 * <pre>{@code
 * AdaptiveConfig.defaults()
 *         .withVersion(Main.class.getPackage().getImplementationVersion())   // or a build id, a git sha…
 *         .withMaxSampleAge(Duration.ofDays(90));
 * }</pre>
 *
 * @param ringSize            samples kept per task name ({@code >= 1}); the estimate is the
 *                            maximum over them
 * @param headroom            multiplier applied to every learned estimate ({@code >= 1});
 *                            never applied to a declared size nothing was learned about
 * @param estimator           which measurement estimates are read through
 * @param selectionKey        what queued candidates are ordered by — {@link SelectionKey#SIZE}
 *                            to pack the memory budget, {@link SelectionKey#DURATION} to
 *                            shorten the makespan
 * @param unknownReserveBytes what the delegate policy's ledger sees while an unprofiled,
 *                            unsized task runs ({@code >= 0})
 * @param heapSamplePeriod    how often the heap sampler reads heap-in-use while a solo task
 *                            runs; {@link Duration#ZERO} disables the sampler thread (only the
 *                            readings at start and end remain)
 * @param version             the application version new samples are stamped with, and the
 *                            only version whose samples the sizing trusts; {@code null} =
 *                            unversioned: nothing is stamped and every sample counts. Must not
 *                            be empty
 * @param maxSampleAge        how old a sample may be and still count, measured from the
 *                            sizing's clock at resolution time; {@code null} = no limit.
 *                            Must be positive
 */
public record AdaptiveConfig(
        int ringSize,
        double headroom,
        Estimator estimator,
        long unknownReserveBytes,
        Duration heapSamplePeriod,
        String version,
        Duration maxSampleAge,
        SelectionKey selectionKey) {

    /** Validates every component; the message names the offending one. */
    public AdaptiveConfig {
        if (ringSize < 1) {
            throw new IllegalArgumentException("ringSize must be >= 1, got " + ringSize);
        }
        if (Double.isNaN(headroom) || headroom < 1.0) {
            throw new IllegalArgumentException("headroom must be >= 1.0, got " + headroom);
        }
        Objects.requireNonNull(estimator, "estimator");
        if (unknownReserveBytes < 0) {
            throw new IllegalArgumentException("unknownReserveBytes must be >= 0, got " + unknownReserveBytes);
        }
        Objects.requireNonNull(heapSamplePeriod, "heapSamplePeriod");
        if (heapSamplePeriod.isNegative()) {
            throw new IllegalArgumentException("heapSamplePeriod must not be negative, got " + heapSamplePeriod);
        }
        if (version != null && version.isEmpty()) {
            throw new IllegalArgumentException("version must be null (unversioned) or non-empty");
        }
        if (maxSampleAge != null && (maxSampleAge.isNegative() || maxSampleAge.isZero())) {
            throw new IllegalArgumentException("maxSampleAge must be null (no limit) or positive, got " + maxSampleAge);
        }
        Objects.requireNonNull(selectionKey, "selectionKey");
    }

    /**
     * Unversioned, with samples that never expire.
     *
     * @param ringSize            samples kept per task name ({@code >= 1})
     * @param headroom            multiplier on learned estimates ({@code >= 1})
     * @param estimator           which measurement estimates are read through
     * @param unknownReserveBytes the ledger reserve for an unknown task ({@code >= 0})
     * @param heapSamplePeriod    the heap sampler period; {@link Duration#ZERO} disables it
     */
    public AdaptiveConfig(int ringSize, double headroom, Estimator estimator, long unknownReserveBytes, Duration heapSamplePeriod) {
        this(ringSize, headroom, estimator, unknownReserveBytes, heapSamplePeriod, null, null, SelectionKey.SIZE);
    }

    /**
     * Ordering by size, the behaviour before durations were learned.
     *
     * @param ringSize            samples kept per task name ({@code >= 1})
     * @param headroom            multiplier on learned estimates ({@code >= 1})
     * @param estimator           which measurement estimates are read through
     * @param unknownReserveBytes the ledger reserve for an unknown task ({@code >= 0})
     * @param heapSamplePeriod    the heap sampler period; {@link Duration#ZERO} disables it
     * @param version             the application version, or {@code null} for unversioned
     * @param maxSampleAge        how old a sample may be and still count, or {@code null}
     */
    public AdaptiveConfig(int ringSize, double headroom, Estimator estimator, long unknownReserveBytes,
            Duration heapSamplePeriod, String version, Duration maxSampleAge) {
        this(ringSize, headroom, estimator, unknownReserveBytes, heapSamplePeriod, version, maxSampleAge, SelectionKey.SIZE);
    }

    /**
     * The defaults: 16 samples, 1.25 headroom, {@link Estimator#ALLOCATION}, the whole
     * maximum heap reserved for an unknown task, a 10 ms sampler, unversioned, no age limit.
     *
     * @return the default configuration
     */
    public static AdaptiveConfig defaults() {
        return new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, Runtime.getRuntime().maxMemory(), Duration.ofMillis(10));
    }

    /**
     * @param version the application version, or {@code null} for unversioned
     * @return a copy with only {@code version} changed
     */
    public AdaptiveConfig withVersion(String version) {
        return new AdaptiveConfig(ringSize, headroom, estimator, unknownReserveBytes, heapSamplePeriod, version, maxSampleAge, selectionKey);
    }

    /**
     * @param maxSampleAge how old a sample may be and still count, or {@code null} for no limit
     * @return a copy with only {@code maxSampleAge} changed
     */
    public AdaptiveConfig withMaxSampleAge(Duration maxSampleAge) {
        return new AdaptiveConfig(ringSize, headroom, estimator, unknownReserveBytes, heapSamplePeriod, version, maxSampleAge, selectionKey);
    }

    /**
     * @param selectionKey what queued candidates are ordered by
     * @return a copy with only {@code selectionKey} changed
     */
    public AdaptiveConfig withSelectionKey(SelectionKey selectionKey) {
        return new AdaptiveConfig(ringSize, headroom, estimator, unknownReserveBytes, heapSamplePeriod, version, maxSampleAge,
                Objects.requireNonNull(selectionKey, "selectionKey"));
    }
}
