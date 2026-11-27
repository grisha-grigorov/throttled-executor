package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * Admits a task while fewer than {@code maxConcurrentTasks} throttled tasks
 * are in flight — a semaphore-style cap, independent of task sizes.
 *
 * <pre>{@code
 * new MaxConcurrencyPolicy(4);   // at most 4 throttled tasks at once
 * }</pre>
 */
public final class MaxConcurrencyPolicy implements AdmissionPolicy {

    private final int maxConcurrentTasks;

    /**
     * @param maxConcurrentTasks maximum number of in-flight throttled tasks
     */
    public MaxConcurrencyPolicy(int maxConcurrentTasks) {
        this.maxConcurrentTasks = maxConcurrentTasks;
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        return snapshot.runningTasks() < maxConcurrentTasks;
    }
}
