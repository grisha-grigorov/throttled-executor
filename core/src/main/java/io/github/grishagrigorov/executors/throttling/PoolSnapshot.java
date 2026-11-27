package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

/**
 * Read-only, consistent view of the throttled pool state, handed to an
 * {@link AdmissionPolicy} at decision time.
 *
 * <p>Policies must base their decision solely on the candidate task and
 * this snapshot — never on mutable state captured from earlier calls.
 * The snapshot is immutable; two reads of the same instance always agree.</p>
 *
 * @see AdmissionPolicy#admit
 * @see io.github.grishagrigorov.executors.internal.ThrottleHarness#snapshot()
 */
public interface PoolSnapshot {

    /**
     * @return the number of throttled tasks currently in flight
     */
    int runningTasks();

    /**
     * @return the exact sum of the raw {@code ThrottledTask.size} estimates
     *         (in bytes) of all in-flight tasks — no multiplier applied
     */
    long trackedSizeBytes();
}
