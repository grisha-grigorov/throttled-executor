package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.util.List;
import java.util.Map;

/**
 * Immutable snapshot of one executor's history node. Per-node consistent (taken under the
 * node's lock); the tree as a whole is assembled node by node, so cross-node totals are
 * approximate while work is in flight.
 *
 * @param executorId       the executor's unique id — the node's identity
 * @param path             root-first tree path, display only
 * @param liveness         {@code LIVE}, {@code CLOSED} or {@code COLLECTED}, probed at snapshot time
 * @param submitted        tasks submitted (including in-flight ones; {@code submitted - totalRecorded()} is the live view)
 * @param statusCounts     completions per {@link TaskStatus}, all time
 * @param durationCount    bodies that ran, all time
 * @param durationMinNanos shortest body, 0 when none ran
 * @param durationMaxNanos longest body
 * @param durationSumNanos total body time (overflow horizon ~292 years)
 * @param histogram        decade buckets, index-aligned with {@link DurationHistogram#LABELS}
 * @param failureCounts    per root-cause class, capped with {@code "(other)"} overflow
 * @param evictedChildren  descendant nodes folded away beneath this node
 * @param records          the ring — most recent tasks, oldest first
 * @param children         child nodes, insertion order
 */
public record ExecutorNodeSnapshot(
        String executorId,
        String path,
        String liveness,
        long submitted,
        Map<TaskStatus, Long> statusCounts,
        long durationCount,
        long durationMinNanos,
        long durationMaxNanos,
        long durationSumNanos,
        long[] histogram,
        Map<String, Long> failureCounts,
        long evictedChildren,
        List<TaskRecord> records,
        List<ExecutorNodeSnapshot> children) {

    public ExecutorNodeSnapshot {
        histogram = histogram.clone();
        statusCounts = Map.copyOf(statusCounts);
        failureCounts = Map.copyOf(failureCounts);
        records = List.copyOf(records);
        children = List.copyOf(children);
    }

    /** @return completions of any status, all time */
    public long totalRecorded() {
        long total = 0;
        for (long count : statusCounts.values()) {
            total += count;
        }
        return total;
    }

    /** @return mean body duration in nanoseconds, 0 when no body ever ran */
    public double meanDurationNanos() {
        return durationCount == 0 ? 0.0 : (double) durationSumNanos / durationCount;
    }
}
