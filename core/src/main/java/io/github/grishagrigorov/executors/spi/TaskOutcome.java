package io.github.grishagrigorov.executors.spi;

/**
 * The result of one task, as reported to {@link TaskExecutionListener#onComplete}.
 *
 * @param status           how the task ended
 * @param failure          for {@link TaskStatus#FAILED} the raw throwable the body threw (not
 *                         wrapped in a {@code CompletionException}); for the two cancelled
 *                         statuses a {@code CancellationException}; for
 *                         {@link TaskStatus#REJECTED} the rejection cause; {@code null} for
 *                         {@link TaskStatus#COMPLETED}
 * @param startedAtNanos   {@link System#nanoTime()} when the body started, or {@code -1} if it
 *                         never did — the test a listener uses to decide whether it has thread
 *                         state to restore
 * @param completedAtNanos {@link System#nanoTime()} when the outcome was determined
 */
public record TaskOutcome(TaskStatus status, Throwable failure, long startedAtNanos, long completedAtNanos) {

    /** Sentinel for {@link #startedAtNanos()} when the body never ran. */
    public static final long NEVER_STARTED = -1L;

    /** @return whether the body ran, i.e. whether {@code onStart} was called for this task */
    public boolean bodyRan() {
        return startedAtNanos >= 0;
    }

    /**
     * @return wall time the body spent running, in nanoseconds, or {@code -1} if it never ran
     */
    public long durationNanos() {
        return bodyRan() ? completedAtNanos - startedAtNanos : -1L;
    }
}
