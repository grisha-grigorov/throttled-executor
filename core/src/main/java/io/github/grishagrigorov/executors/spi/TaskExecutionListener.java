package io.github.grishagrigorov.executors.spi;

/**
 * Observes the lifecycle of every task submitted to a {@code ThrottledExecutor}
 * (and, through inheritance, to its whole subtree).
 *
 * <p>This is the library's single extension seam for cross-cutting concerns: context
 * propagation (MDC, tracing, security principals), execution metadata and flight
 * recording, metrics, JFR events. The executor's own MDC propagation and ambient-parent
 * binding are implemented as listeners of exactly this shape.</p>
 *
 * <h2>Contract</h2>
 * <ul>
 *   <li><b>Observe, never alter.</b> Listeners cannot change what a task does, wrap it, retry
 *       it or swallow its failure. This is deliberate: a wrapping decorator that swallowed an
 *       exception would make every recorder after it see a success.</li>
 *   <li><b>Exactly once.</b> Every {@link #onSubmit} is followed by exactly one
 *       {@link #onComplete}, whatever happens in between — including rejection and
 *       cancellation before the body ever ran.</li>
 *   <li><b>{@link #onStart} fires only for bodies that actually ran</b>, i.e. when the final
 *       status is {@link TaskStatus#COMPLETED}, {@link TaskStatus#FAILED} or
 *       {@link TaskStatus#CANCELLED_AFTER_START}.</li>
 *   <li><b>Ordering.</b> {@code onSubmit} and {@code onStart} run root-first through the
 *       executor tree (inherited listeners before the executor's own); {@code onComplete}
 *       runs in reverse, so a listener that installs thread state first is the one that
 *       restores it last — ordinary scope semantics.</li>
 *   <li><b>Threads.</b> {@code onSubmit} runs on the submitting thread. {@code onStart} and,
 *       for tasks that ran, {@code onComplete} run on the worker that ran the body. For
 *       {@link TaskStatus#REJECTED} and {@link TaskStatus#CANCELLED_BEFORE_START},
 *       {@code onComplete} runs on the thread that rejected or cancelled. A listener that
 *       installs thread-local state in {@code onStart} must therefore restore it in
 *       {@code onComplete} <em>only if</em> {@code outcome.startedAtNanos() >= 0}.</li>
 *   <li><b>A listener can never fail the task.</b> Every invocation is individually guarded:
 *       an exception is logged and the task proceeds, and one misbehaving listener cannot
 *       suppress the others. ({@link VirtualMachineError}s are not swallowed.)</li>
 *   <li><b>Snapshotted at submission.</b> A task sees the listeners present when it was
 *       submitted; listeners added later never see it.</li>
 *   <li><b>Fast and non-blocking.</b> Hooks run inline on the submitting and worker threads.
 *       In {@code onComplete} the task's future is <em>not yet complete</em> — use the
 *       {@link TaskOutcome}; never {@code join()} the task's own future from a hook.</li>
 *   <li><b>Thread-safe.</b> One listener instance serves every task on every thread.
 *       Per-task state goes in {@code C}, never in listener fields.</li>
 * </ul>
 *
 * <pre>{@code
 * // Propagate a request id from the submitter to the worker, Micrometer-style:
 * final class RequestIdListener implements TaskExecutionListener<String> {
 *     @Override public String onSubmit(TaskDescriptor task) { return RequestId.current(); }
 *     @Override public void onStart(TaskDescriptor task, String id) { RequestId.set(id); }
 *     @Override public void onComplete(TaskDescriptor task, String id, TaskOutcome o) {
 *         if (o.startedAtNanos() >= 0) RequestId.clear();
 *     }
 * }
 * root.addListener(new RequestIdListener());   // inherited by the whole subtree
 * }</pre>
 *
 * @param <C> this listener's per-task state: returned by {@link #onSubmit}, handed back to
 *            the other two hooks. Use {@link Void} and return {@code null} if none is needed.
 */
public interface TaskExecutionListener<C> {

    /**
     * Called on the submitting thread, before the task is handed to the pool.
     *
     * @return per-task state to carry into {@link #onStart} and {@link #onComplete}; may be null
     */
    default C onSubmit(TaskDescriptor task) {
        return null;
    }

    /**
     * Called for a <b>throttled</b> task once it has been admitted, before {@link #onStart}, with
     * how long it waited and whether the throttle actually permitted it.
     *
     * <p>Queue latency was always derivable from the descriptor's timestamps. What was not
     * observable is <em>why</em>: whether a task waited because the policy refused it, and
     * whether it eventually started because the policy relented or because the liveness floor
     * overrode it. The second is the difference between a throttle working and a throttle giving
     * up, and it is exactly what you want to know when a batch is slow.</p>
     *
     * <p>Only throttled submissions fire this — a plain {@code submit()} passes through no
     * policy, so there is nothing to report. Runs on the same thread as {@link #onStart}, in the
     * same order as the other forward hooks.</p>
     *
     * <p><b>Only for bodies that actually run</b>, exactly like {@link #onStart}. A task the
     * harness admitted and charged for, which is then cancelled while queued or rejected because
     * the executor closed, never fires this — so its wait and its {@code floorAdmitted} flag are
     * not reported. Totals built from this hook are therefore totals over <em>started</em> tasks;
     * a batch cut short by {@code cancelSubtree()} will show fewer throttle events than it had
     * admissions.</p>
     *
     * <pre>{@code
     * @Override
     * public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {
     *     if (floorAdmitted) {
     *         LOGGER.warn("{} started only because nothing else was running", task.displayName());
     *     }
     * }
     * }</pre>
     *
     * @param task          the admitted task
     * @param waitedNanos   nanoseconds this task spent queued before it was admitted, measured
     *                      from when its coordinator first offered it. Small but non-zero for a
     *                      task admitted on the first scan
     * @param floorAdmitted {@code true} if the policy refused it and the liveness floor admitted
     *                      it anyway (nothing was in flight), {@code false} if the policy admitted
     *                      it on its merits
     */
    default void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {
    }

    /**
     * Called on the worker thread immediately before the task body runs. Not called if the
     * body never runs (rejected, or cancelled while queued).
     */
    default void onStart(TaskDescriptor task, C state) {
    }

    /**
     * Called exactly once per submission, after the body returned or threw, or when the task
     * was rejected or cancelled before it could start. The task's future is not yet complete
     * when this runs.
     */
    default void onComplete(TaskDescriptor task, C state, TaskOutcome outcome) {
    }
}
