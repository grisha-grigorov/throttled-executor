package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;

/**
 * The {@code ServiceLoader} entry point for this module: a stateless facade whose hooks
 * delegate to {@link ExecutionRecorder#shared()}.
 *
 * <p>Why a facade: {@code ServiceLoader} instantiates a fresh object per load, and a fresh
 * recorder nobody holds a reference to could never be queried. Delegating to the shared
 * instance means {@code root.addDiscoveredListeners()} feeds a recorder that is reachable
 * afterwards — via {@code ExecutionRecorder.shared()} in code, or over JMX.</p>
 *
 * <p>Caveats: calling {@code addDiscoveredListeners()} twice on one root double-feeds
 * (family-wide property of discovery); and discovery resolves through core's classloader,
 * so in layered-classloader containers where this jar is invisible to core's loader, use
 * the explicit {@code root.addListener(ExecutionRecorder.shared())} instead.</p>
 */
public final class DiscoveredExecutionRecorder implements TaskExecutionListener<Void> {

    /** Public no-arg constructor, as {@link java.util.ServiceLoader} requires. */
    public DiscoveredExecutionRecorder() {
    }

    @Override
    public Void onSubmit(TaskDescriptor task) {
        return ExecutionRecorder.shared().onSubmit(task);
    }

    @Override
    public void onStart(TaskDescriptor task, Void state) {
        ExecutionRecorder.shared().onStart(task, state);
    }

    @Override
    public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {
        // Every hook, not most of them: this one was missing, and the admission counters
        // stayed at zero for anyone attaching the recorder through discovery.
        ExecutionRecorder.shared().onThrottled(task, waitedNanos, floorAdmitted);
    }

    @Override
    public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {
        ExecutionRecorder.shared().onComplete(task, state, outcome);
    }
}
