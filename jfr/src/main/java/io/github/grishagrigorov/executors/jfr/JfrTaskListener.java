package io.github.grishagrigorov.executors.jfr;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;

/**
 * Emits one {@link TaskEvent} per task to Java Flight Recorder.
 *
 * <p>Attach explicitly — {@code root.addListener(JfrTaskListener.INSTANCE)} — or let an
 * opted-in root discover it: this jar registers the listener under
 * {@code META-INF/services}, so {@code root.addDiscoveredListeners()} picks it up. Either
 * way the listener is inherited by the root's whole subtree.</p>
 *
 * <p>Cost when no recording is active: one allocation per task and an {@code isEnabled()}
 * check, nothing else — {@link #onSubmit} hands back no state, so the other hooks are
 * no-ops. Stateless; the per-task state is the event itself.</p>
 *
 * <pre>{@code
 * var root = new ThrottledExecutor("App");
 * root.addListener(JfrTaskListener.INSTANCE);
 * // run with:  java -XX:StartFlightRecording:filename=app.jfr ...
 * // open app.jfr in JDK Mission Control -> Event Browser -> Executors
 * }</pre>
 */
public final class JfrTaskListener implements TaskExecutionListener<TaskEvent> {

    /** Shared instance; the listener holds no state of its own. */
    public static final JfrTaskListener INSTANCE = new JfrTaskListener();

    /** Public, no-arg: required by {@link java.util.ServiceLoader}. Prefer {@link #INSTANCE}. */
    public JfrTaskListener() {
    }

    @Override
    public TaskEvent onSubmit(TaskDescriptor task) {
        final TaskEvent event = new TaskEvent();
        // No recording has this event enabled -> no state, and every later hook is a no-op.
        return event.isEnabled() ? event : null;
    }

    /**
     * Commits a {@link ThrottleEvent} for a task that went through a policy.
     *
     * <p>Independent of the {@link TaskEvent} state: a recording may enable either event without
     * the other, and {@code isEnabled()} is checked separately so an unwanted event costs one
     * allocation and nothing else.</p>
     */
    @Override
    public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {

        final ThrottleEvent event = new ThrottleEvent();

        if (!event.isEnabled()) {
            return;
        }

        event.executorId = task.executorId();
        event.executorPath = task.executorPath();
        event.taskName = task.displayName();
        event.sequence = task.sequence();
        event.waitedNanos = waitedNanos;
        event.floorAdmitted = floorAdmitted;
        event.commit();
    }

    @Override
    public void onStart(TaskDescriptor task, TaskEvent event) {
        if (event != null) {
            event.begin();
        }
    }

    @Override
    public void onComplete(TaskDescriptor task, TaskEvent event, TaskOutcome outcome) {
        if (event == null) {
            return;
        }

        event.executorId = task.executorId();
        event.executorPath = task.executorPath();
        event.taskName = task.displayName();
        event.sequence = task.sequence();
        event.sizeEstimate = task.sizeEstimate();
        event.backend = task.backend().name();
        event.status = outcome.status().name();
        event.bodyRan = outcome.bodyRan();
        event.queuedNanos = outcome.bodyRan() ? outcome.startedAtNanos() - task.submittedAtNanos() : 0L;
        event.failure = describe(outcome.failure());

        if (outcome.bodyRan()) {
            event.end();       // duration = body wall time
        }
        event.commit();        // a never-started task commits with no duration
    }

    /** Class and message only — the Throwable itself must not be retained by a recording. */
    private static String describe(Throwable failure) {
        if (failure == null) {
            return null;
        }
        final String message = failure.getMessage();
        return message == null ? failure.getClass().getName() : failure.getClass().getName() + ": " + message;
    }
}
