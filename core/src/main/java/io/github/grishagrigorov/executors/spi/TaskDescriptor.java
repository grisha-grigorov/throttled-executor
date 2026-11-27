package io.github.grishagrigorov.executors.spi;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledExecutor;

/**
 * Identifies one submitted task to {@link TaskExecutionListener}s. Immutable; created once
 * per submission and handed to all three hooks.
 *
 * <p>Holds the <em>executor</em>, never the <em>job</em>: a listener that stores a
 * descriptor's fields (or the descriptor itself) keeps the executor reachable but never
 * pins the task body or anything its lambda captured — which is what lets a flight
 * recorder retain history while the tasks themselves are collected.</p>
 *
 * @param executor              the executor the task was submitted to
 * @param sequence              per-executor submission sequence number, strictly increasing
 * @param name                  the {@code ThrottledTask} name for throttled submissions;
 *                              {@code null} for plain {@code submit(...)} calls
 * @param sizeEstimate          the {@code ThrottledTask} size estimate in bytes; {@code -1} if unknown
 * @param submittedAtNanos      {@link System#nanoTime()} at submission, for durations
 * @param submittedAtEpochMillis {@link System#currentTimeMillis()} at submission, for wall-clock display
 */
public record TaskDescriptor(
        ThrottledExecutor executor,
        long sequence,
        String name,
        long sizeEstimate,
        long submittedAtNanos,
        long submittedAtEpochMillis) {

    /** Sentinel for {@link #sizeEstimate()} when the submission carried no estimate. */
    public static final long UNKNOWN_SIZE = -1L;

    /** @return the owning executor's id — delegates, so nothing is computed per submission */
    public String executorId() {
        return executor.executorId;
    }

    /** @return the owning executor's root-first tree path, e.g. {@code Voyage → Leg-3 → FuelCalc} */
    public String executorPath() {
        return executor.executorPath();
    }

    /** @return the pool the task runs on */
    public ExecutorBackend backend() {
        return executor.backend();
    }

    /** @return a short human label: the name if present, else {@code task#<sequence>} */
    public String displayName() {
        return name != null ? name : "task#" + sequence;
    }

    @Override
    public String toString() {
        return "TaskDescriptor[" + displayName() + " on " + executorId()
                + (sizeEstimate >= 0 ? ", ~" + sizeEstimate + " bytes" : "") + "]";
    }
}
