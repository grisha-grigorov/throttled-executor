package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import java.util.Map;
import org.slf4j.MDC;

/**
 * Built-in listener: carries the submitter's SLF4J {@link MDC} context into the task and
 * restores the worker's previous context afterwards.
 *
 * <p>Restoring matters because workers are pooled: without it a worker keeps the last task's
 * context and bleeds it into unrelated work that happens to run there next.</p>
 *
 * <p>Installed unconditionally ahead of user listeners (see
 * {@code ThrottledExecutor#BUILT_IN_LISTENERS}); not removable. Stateless — the per-task
 * maps live in the {@link State} returned from {@link #onSubmit}.</p>
 */
final class MdcPropagationListener implements TaskExecutionListener<MdcPropagationListener.State> {

    static final MdcPropagationListener INSTANCE = new MdcPropagationListener();

    /** Per-task: what the submitter had, and what the worker had before we overwrote it. */
    static final class State {
        final Map<String, String> captured;
        Map<String, String> previous;

        State(Map<String, String> captured) {
            this.captured = captured;
        }
    }

    private MdcPropagationListener() {
    }

    @Override
    public State onSubmit(TaskDescriptor task) {
        return new State(MDC.getCopyOfContextMap());
    }

    @Override
    public void onStart(TaskDescriptor task, State state) {
        state.previous = MDC.getCopyOfContextMap();
        apply(state.captured);
    }

    @Override
    public void onComplete(TaskDescriptor task, State state, TaskOutcome outcome) {
        // Only a body that ran installed anything on this thread. For REJECTED and
        // CANCELLED_BEFORE_START this runs on the submitter/canceller, whose MDC is not ours.
        if (outcome.bodyRan()) {
            apply(state.previous);
        }
    }

    /**
     * Applies a captured context map, treating {@code null} (what
     * {@link MDC#getCopyOfContextMap()} returns for an empty context) as "clear".
     *
     * <p>{@code MDC.setContextMap(null)} is not defined consistently across SLF4J bindings,
     * and this library declares only the SLF4J API — the consumer chooses the binding — so
     * the null case is handled here rather than left to chance.</p>
     */
    static void apply(Map<String, String> contextMap) {
        if (contextMap == null) {
            MDC.clear();
        } else {
            MDC.setContextMap(contextMap);
        }
    }
}
