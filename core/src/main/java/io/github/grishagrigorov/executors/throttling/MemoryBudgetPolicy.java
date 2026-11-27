package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * Admits a task while the combined in-flight memory estimate stays under a
 * fixed ceiling: {@code (trackedSizeBytes + candidate.size) * multiplier < ceilingBytes}.
 *
 * <p>The framework tracks the raw size sum in exact longs; the multiplier is
 * applied once per check, so no truncation drift can accumulate across
 * start/end cycles (the historical {@code MbLoadThrottle} bug class).</p>
 *
 * <pre>{@code
 * // 85% of max heap, estimates taken at face value:
 * new MemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);
 *
 * // Conservative — treat every estimate as 1.5x its declared size:
 * new MemoryBudgetPolicy(2_000_000_000L, 1.5);
 * }</pre>
 */
public final class MemoryBudgetPolicy implements AdmissionPolicy {

    private final long ceilingBytes;
    private final double multiplier;

    /**
     * @param ceilingBytes total in-flight estimate ceiling, in bytes
     * @param multiplier   scale factor applied to estimates ({@code > 1} = conservative)
     */
    public MemoryBudgetPolicy(long ceilingBytes, double multiplier) {
        this.ceilingBytes = ceilingBytes;
        this.multiplier = multiplier;
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        /**
         * The multiplier is applied ONCE to the raw total — no per-task truncation, so no
         * drift can accumulate across start/end cycles. Double arithmetic keeps extreme
         * estimates (e.g. Long.MAX_VALUE) from overflowing into a wrongly-admitted negative.
         */
        return ((double) snapshot.trackedSizeBytes() + (double) candidate.size) * multiplier < (double) ceilingBytes;
    }
}
