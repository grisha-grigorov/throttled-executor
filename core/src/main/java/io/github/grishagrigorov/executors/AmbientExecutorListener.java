package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;

/**
 * Built-in listener: publishes the executor whose task is running on the current thread,
 * for the duration of that task, so {@link ThrottledExecutor#withAmbientDiscovery}
 * can find its parent.
 *
 * <p>EVERY executor binds this (it is a built-in, ahead of user listeners and not removable);
 * only an explicitly opted-in construction reads it. Binding and reading are separate
 * concerns: any executor can serve as an ambient parent, but nothing is adopted by accident.</p>
 *
 * <p>A {@link ThreadLocal} rather than a {@code ScopedValue}, which is Java 25 and
 * unavailable at this module's Java 17 floor. Save/restore across the task gives the same
 * dynamic-extent semantics; the restore uses {@code remove()} when there was no previous
 * binding, so a pooled worker is never left holding a reference to a dead executor. (Note a
 * {@code ScopedValue} version could <em>not</em> be expressed in this start/complete shape —
 * it binds by wrapping — so that is not a drop-in swap.)</p>
 */
final class AmbientExecutorListener implements TaskExecutionListener<AmbientExecutorListener.State> {

    static final AmbientExecutorListener INSTANCE = new AmbientExecutorListener();

    private static final ThreadLocal<ThrottledExecutor> AMBIENT = new ThreadLocal<>();

    /** Per-task: the binding the worker had before this task, restored afterwards. */
    static final class State {
        ThrottledExecutor previous;
    }

    private AmbientExecutorListener() {
    }

    /** @return the executor whose task is running on this thread, or {@code null} */
    static ThrottledExecutor current() {
        return AMBIENT.get();
    }

    @Override
    public State onSubmit(TaskDescriptor task) {
        return new State();
    }

    @Override
    public void onStart(TaskDescriptor task, State state) {
        state.previous = AMBIENT.get();
        AMBIENT.set(task.executor());
    }

    @Override
    public void onComplete(TaskDescriptor task, State state, TaskOutcome outcome) {
        if (!outcome.bodyRan()) {
            return;   // nothing was installed on this thread
        }
        /*
         * Restore, do not merely clear. Tasks nest — an inner executor's task can run inside
         * an outer one's — so clearing would lose the enclosing binding. remove() when there
         * was none, never set(null): a lingering ThreadLocal entry on a pooled worker holds
         * a strong reference to the executor and, through it, everything it owns.
         */
        if (state.previous == null) {
            AMBIENT.remove();
        } else {
            AMBIENT.set(state.previous);
        }
    }
}
