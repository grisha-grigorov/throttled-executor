package io.github.grishagrigorov.executors.adaptive;

/**
 * Which measurement a {@link TaskProfile} is read through when a size is estimated. Each
 * estimator has a primary measurement and falls back to the other when the primary was
 * never recorded for the task (virtual-thread workers, for instance, yield no allocation
 * figures, so such tasks learn through heap peaks alone).
 */
public enum Estimator {

    /**
     * Per-thread allocated bytes — exact and attributable, but counts everything the body
     * churned, not only what it retained. Falls back to {@link #SOLO_HEAP_PEAK}.
     */
    ALLOCATION,

    /**
     * Growth of heap-in-use while the body ran <em>alone</em> — closer to "peak retained",
     * but only attributable when nothing else ran. Falls back to {@link #ALLOCATION}.
     */
    SOLO_HEAP_PEAK
}
