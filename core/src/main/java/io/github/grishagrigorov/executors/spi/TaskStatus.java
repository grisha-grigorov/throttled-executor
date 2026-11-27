package io.github.grishagrigorov.executors.spi;

/**
 * How a submitted task ended. Exactly one of these is reported to
 * {@link TaskExecutionListener#onComplete} for every submission.
 */
public enum TaskStatus {

    /** The body ran and returned normally. */
    COMPLETED,

    /** The body ran and threw; {@link TaskOutcome#failure()} is the raw throwable. */
    FAILED,

    /**
     * The future was cancelled while the task was still queued. The body never ran and
     * {@link TaskExecutionListener#onStart} was never called.
     */
    CANCELLED_BEFORE_START,

    /**
     * The future was cancelled while the body was running. The outcome is reported when the body
     * eventually returns — later than the future is observed cancelled. {@code onStart}
     * <em>was</em> called.
     *
     * <p>On the virtual backend the running thread is interrupted, so a body blocked on IO or a
     * latch returns promptly. On the pooled backends it is not — the worker outlives the task,
     * and an interrupt set there would surface inside somebody else's work — so the body runs to
     * its natural end.</p>
     */
    CANCELLED_AFTER_START,

    /**
     * The submission was refused — the executor is closed or the shared pool has been shut
     * down. The body never ran; reported on the submitting thread.
     */
    REJECTED;

    /** @return whether the task body actually ran (and so {@code onStart} fired) */
    public boolean bodyRan() {
        return this == COMPLETED || this == FAILED || this == CANCELLED_AFTER_START;
    }
}
