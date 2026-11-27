package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import java.util.concurrent.CompletableFuture;

/**
 * Associates a task's {@link CompletableFuture} with the original job (Runnable, Callable,
 * or Supplier) that produced it and, when known, its {@link TaskDescriptor}. Stored in the
 * {@link ThrottledExecutor} futures queue for tracking, cancellation, and completion
 * monitoring, and handed out read-only through {@link ThrottledExecutor#getFutures()}.
 *
 * <p>A value the executor produces, never one it accepts: only the executor constructs a
 * pair, so the class is final and immutable. The future is both the cancellation target and
 * the wait target — the completion path records a failure (and runs every listener's
 * {@code onComplete}) <em>before</em> the future settles, so there is no separate "settled"
 * stage any more.</p>
 *
 * @see ThrottledExecutor
 */
public final class FuturePair {

    private final CompletableFuture<?> completableFuture;
    private final Object job;
    private final TaskDescriptor descriptor;

    FuturePair(CompletableFuture<?> completableFuture, Object job, TaskDescriptor descriptor) {
        this.completableFuture = completableFuture;
        this.job = job;
        this.descriptor = descriptor;
    }

    /** @return the task's future: cancel it to cancel the task, wait on it to wait for the task */
    public CompletableFuture<?> getCompletableFuture() {
        return completableFuture;
    }

    /** @return the job as submitted — a {@code Runnable}, {@code Callable} or {@code Supplier} */
    public Object getJob() {
        return job;
    }

    /** @return the task's descriptor, or {@code null} for pairs created without one */
    public TaskDescriptor getDescriptor() {
        return descriptor;
    }
}
