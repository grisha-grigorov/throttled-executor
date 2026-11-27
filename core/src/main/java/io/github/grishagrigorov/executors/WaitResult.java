package io.github.grishagrigorov.executors;

import java.util.List;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;

/**
 * What a bounded wait found: whether the subtree finished, and if not, which tasks were still
 * unsettled when the clock ran out.
 *
 * <p>Returned by {@link ThrottledExecutor#waitForCompletion(java.time.Duration)}. A timeout
 * is not an error — nothing is cancelled and the tasks keep running — so it is reported rather
 * than thrown. Task <em>failures</em> are still thrown, exactly as the untimed wait throws them.</p>
 *
 * <pre>{@code
 * WaitResult result = executor.waitForCompletion(Duration.ofMinutes(2));
 *
 * if (!result.complete()) {
 *     LOGGER.warn("still running after 2 minutes: {}", result.outstanding());
 *     executor.cancelSubtree();
 * }
 * }</pre>
 *
 * @param complete    {@code true} if the whole subtree settled within the budget
 * @param outstanding the tasks still unsettled when the budget expired, empty when
 *                    {@code complete}. Descriptors, not futures — a snapshot for logging, not a
 *                    handle to act on.
 */
public record WaitResult(boolean complete, List<TaskDescriptor> outstanding) {

    /**
     * Canonicalises the outstanding list so it is immutable and never {@code null}.
     *
     * @param complete    whether the subtree settled
     * @param outstanding the unsettled tasks, or {@code null} for none
     */
    public WaitResult {
        outstanding = outstanding == null ? List.of() : List.copyOf(outstanding);
    }
}
