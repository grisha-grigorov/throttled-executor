package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.lang.ref.WeakReference;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * The mutable history node for one executor. Identified by the executor's id string and
 * designed to <b>outlive</b> the executor: the only reference back is a
 * {@link WeakReference}, used for liveness display and fold eligibility — a node never
 * pins the executor it describes.
 *
 * <p>All state is guarded by the node's own monitor (coarse on purpose — a synchronized
 * ledger measured at the throttled path's throughput ceiling). The one rule that keeps
 * the locking safe: <b>never hold two node monitors at once</b>. Folding reads the child
 * under the child's lock, releases, then merges under the parent's lock; the
 * {@link #folded} flag closes the window in between (a racing recorder that acquires a
 * folded node retries through the index instead of writing into a detached node).</p>
 */
final class ExecutorNode {

    /** Key of the failure-count overflow bucket once {@code failureClassLimit} distinct keys exist. */
    static final String OTHER_FAILURES = "(other)";

    final String executorId;
    final String path;
    final WeakReference<ThrottledExecutor> executorRef;
    final ExecutorNode parent;

    // ── guarded by synchronized(this) ────────────────────────────────────────────
    private final LinkedHashMap<String, ExecutorNode> children = new LinkedHashMap<>();
    private final ArrayDeque<TaskRecord> ring = new ArrayDeque<>();
    private final long[] statusCounts = new long[TaskStatus.values().length];
    private final long[] histogram = new long[DurationHistogram.BUCKETS];
    private final LinkedHashMap<String, Long> failureCounts = new LinkedHashMap<>();
    private long submitted;
    private long durationCount;
    private long durationMinNanos;
    private long durationMaxNanos;
    /** Overflow horizon: ~292 years of cumulative body time — not worth Math.addExact. */
    private long durationSumNanos;
    private long evictedChildren;
    private boolean folded;

    ExecutorNode(String executorId, String path, ThrottledExecutor executor, ExecutorNode parent) {
        this.executorId = executorId;
        this.path = path;
        this.executorRef = new WeakReference<>(executor);
        this.parent = parent;
    }

    /** @return false if this node has been folded away — the caller must retry via the index */
    synchronized boolean recordSubmit() {
        if (folded) {
            return false;
        }
        submitted++;
        return true;
    }

    /** @return false if this node has been folded away — the caller must retry via the index */
    synchronized boolean record(TaskRecord record, RecorderConfig config, String rootCauseClass) {
        if (folded) {
            return false;
        }

        ring.addLast(record);
        while (ring.size() > config.ringSize()) {
            ring.removeFirst();
        }

        statusCounts[record.status().ordinal()]++;

        if (record.durationNanos() >= 0) {
            if (durationCount == 0 || record.durationNanos() < durationMinNanos) {
                durationMinNanos = record.durationNanos();
            }
            if (record.durationNanos() > durationMaxNanos) {
                durationMaxNanos = record.durationNanos();
            }
            durationCount++;
            durationSumNanos += record.durationNanos();
            histogram[DurationHistogram.bucketFor(record.durationNanos())]++;
        }

        if (rootCauseClass != null) {
            countFailure(rootCauseClass, 1, config.failureClassLimit());
        }

        return true;
    }

    /** Caller must hold this node's monitor (private helper for record + merge). */
    private void countFailure(String key, long delta, int cap) {
        if (failureCounts.containsKey(key) || failureCounts.size() < cap) {
            failureCounts.merge(key, delta, Long::sum);
        } else {
            failureCounts.merge(OTHER_FAILURES, delta, Long::sum);
        }
    }

    /** Registers a child node. Parent monitor only — never nested with the child's. */
    synchronized void attachChild(ExecutorNode child) {
        children.put(child.executorId, child);
    }

    /** @return this node's children, copied in insertion order */
    synchronized List<ExecutorNode> childrenSnapshot() {
        return new ArrayList<>(children.values());
    }

    /**
     * The frozen state of a fold candidate, taken under the child's lock so the fold can
     * merge it into the parent WITHOUT holding both monitors.
     */
    record FoldedState(long submitted, long[] statusCounts, long durationCount, long durationMinNanos,
            long durationMaxNanos, long durationSumNanos, long[] histogram,
            Map<String, Long> failureCounts, long evictedDescendants) {
    }

    /**
     * Marks this node folded and hands back its aggregate state. After this returns, any
     * concurrent {@link #record} on this node fails and retries through the index.
     *
     * @return the state to merge into the parent, or {@code null} if already folded
     */
    synchronized FoldedState foldAway() {
        if (folded) {
            return null;
        }
        folded = true;
        return new FoldedState(submitted, statusCounts.clone(), durationCount, durationMinNanos,
                durationMaxNanos, durationSumNanos, histogram.clone(),
                new LinkedHashMap<>(failureCounts), evictedChildren);
    }

    /** Merges a folded child's aggregates into this node. Records are dropped — compressed, not lost. */
    synchronized void mergeFoldedChild(String childId, FoldedState child, RecorderConfig config) {
        children.remove(childId);
        submitted += child.submitted();
        for (int i = 0; i < statusCounts.length; i++) {
            statusCounts[i] += child.statusCounts()[i];
        }
        if (child.durationCount() > 0) {
            if (durationCount == 0 || child.durationMinNanos() < durationMinNanos) {
                durationMinNanos = child.durationMinNanos();
            }
            if (child.durationMaxNanos() > durationMaxNanos) {
                durationMaxNanos = child.durationMaxNanos();
            }
            durationCount += child.durationCount();
            durationSumNanos += child.durationSumNanos();
        }
        for (int i = 0; i < histogram.length; i++) {
            histogram[i] += child.histogram()[i];
        }
        for (Map.Entry<String, Long> failure : child.failureCounts().entrySet()) {
            countFailure(failure.getKey(), failure.getValue(), config.failureClassLimit());
        }
        // The folded child itself plus every descendant already folded beneath it.
        evictedChildren += 1 + child.evictedDescendants();
    }

    synchronized boolean isFolded() {
        return folded;
    }

    /** LIVE / CLOSED / COLLECTED, probed lazily — never cached. */
    String liveness() {
        final ThrottledExecutor executor = executorRef.get();
        if (executor == null) {
            return "COLLECTED";
        }
        return executor.isClosed() ? "CLOSED" : "LIVE";
    }

    /** @return whether this node's executor has been garbage collected — the ONLY fold trigger */
    boolean isCollected() {
        return executorRef.get() == null;
    }

    /** Immutable copy of this node's own state (children filled in by the recorder's walk). */
    synchronized ExecutorNodeSnapshot snapshotOwn(List<ExecutorNodeSnapshot> childSnapshots) {
        final Map<TaskStatus, Long> counts = new EnumMap<>(TaskStatus.class);
        for (TaskStatus status : TaskStatus.values()) {
            if (statusCounts[status.ordinal()] > 0) {
                counts.put(status, statusCounts[status.ordinal()]);
            }
        }
        return new ExecutorNodeSnapshot(executorId, path, liveness(), submitted, counts,
                durationCount, durationMinNanos, durationMaxNanos, durationSumNanos,
                histogram.clone(), new LinkedHashMap<>(failureCounts), evictedChildren,
                List.copyOf(ring), childSnapshots);
    }
}
