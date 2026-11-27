package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * A memory budget shared across <em>independently-rooted</em> executors.
 *
 * <p>Two policies exist for one job because the accounting lives in different places:</p>
 *
 * <ul>
 *   <li>{@link MemoryBudgetPolicy} reads the {@link PoolSnapshot} it is handed — the
 *       counters of ONE executor tree. Within a tree that is exactly right, and the
 *       root-delegated harness makes it subtree-global for free. Across trees it is
 *       useless, <b>even when the instance is shared</b>: each tree hands it a snapshot
 *       that knows nothing about the others, so N trees each grant themselves the full
 *       ceiling.</li>
 *   <li>This policy owns its own ledger, driven by the {@link #onTaskStart}/
 *       {@link #onTaskEnd} hooks — the {@link RateLimitPolicy} template — and
 *       deliberately <b>ignores the snapshot</b>. Share one instance across every
 *       executor that should compete for the same budget, and the ledger spans them
 *       all.</li>
 * </ul>
 *
 * <pre>{@code
 * // One budget for two unrelated subsystems:
 * GlobalMemoryBudgetPolicy heapBudget =
 *         new GlobalMemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);
 *
 * reportExecutor.submitThrottled(reportTasks, heapBudget);   // root 1
 * importExecutor.submitThrottled(importTasks, heapBudget);   // root 2, same ledger
 * }</pre>
 *
 * <p><strong>Liveness floor.</strong> {@link #inFlightTaskCount()} reports this ledger's
 * count, and the framework keys its liveness floor on the larger of that and the per-tree
 * harness count. Without the ledger's half, an <em>idle</em> root would see its own zero
 * running tasks and admit unconditionally — straight past a budget another root has fully
 * occupied.</p>
 *
 * <p><strong>Precision.</strong> Roots do not share a harness lock, so admission across
 * roots is not one atomic step: at a moment of global idleness, simultaneously-scanning
 * roots can each floor-admit one task before the other's claim becomes visible. The
 * transient overshoot is bounded by the number of such roots and only at global zero —
 * this is a fail-safe budget, not an exact reservation system. Note also that a task
 * completing in one root wakes only that root's waiters; other roots re-check on the
 * framework's poll interval, so cross-root admission latency is up to that interval.</p>
 *
 * @see AdmissionPolicy#defaultPolicy(io.github.grishagrigorov.executors.ExecutorBackend)
 */
public final class GlobalMemoryBudgetPolicy implements AdmissionPolicy {

    private final long ceilingBytes;
    private final double multiplier;

    /** Guards the ledger; admission decisions and hook updates must observe one state. */
    private final Object ledgerLock = new Object();

    private long inFlightBytes;
    private int inFlightTasks;

    /**
     * The single process-wide heap budget used by
     * {@link AdmissionPolicy#defaultPolicy(io.github.grishagrigorov.executors.ExecutorBackend)}:
     * 85% of max heap, estimates at face value.
     *
     * <p>A singleton on purpose — the heap it guards is process-wide, so a fail-safe
     * default that handed each caller its own ledger would defend nothing. Lazily held
     * so merely loading this class allocates nothing.</p>
     */
    public static GlobalMemoryBudgetPolicy processWideHeapBudget() {
        return ProcessWideHolder.HEAP;
    }

    private static final class ProcessWideHolder {
        static final GlobalMemoryBudgetPolicy HEAP =
                new GlobalMemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);
    }

    /**
     * @param ceilingBytes total in-flight estimate ceiling, in bytes
     * @param multiplier   scale factor applied to estimates ({@code > 1} = conservative)
     */
    public GlobalMemoryBudgetPolicy(long ceilingBytes, double multiplier) {
        this.ceilingBytes = ceilingBytes;
        this.multiplier = multiplier;
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot ignoredSnapshot) {
        synchronized (ledgerLock) {
            /*
             * Same arithmetic as MemoryBudgetPolicy: the multiplier applied once to the raw
             * total (no per-task truncation drift), in double so an absurd estimate like
             * Long.MAX_VALUE refuses cleanly instead of overflowing into admission.
             */
            return ((double) inFlightBytes + (double) candidate.size) * multiplier < (double) ceilingBytes;
        }
    }

    @Override
    public void onTaskStart(ThrottledTask<?> task) {
        synchronized (ledgerLock) {
            inFlightTasks++;
            inFlightBytes += task.size;
        }
    }

    @Override
    public void onTaskEnd(ThrottledTask<?> task) {
        synchronized (ledgerLock) {
            inFlightTasks--;
            inFlightBytes -= task.size;
        }
    }

    /** This ledger's own count — what redirects the liveness floor to global state. */
    @Override
    public int inFlightTaskCount() {
        synchronized (ledgerLock) {
            return inFlightTasks;
        }
    }

    /** Explicitly: this ledger's whole point is to span executor trees. */
    @Override
    public boolean ledgerSpansExecutors() {
        return true;
    }

    /** @return the summed in-flight size estimates, for monitoring and tests */
    public long inFlightBytes() {
        synchronized (ledgerLock) {
            return inFlightBytes;
        }
    }
}
