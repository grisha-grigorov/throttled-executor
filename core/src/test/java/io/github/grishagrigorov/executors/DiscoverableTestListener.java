package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Test fixture registered under {@code META-INF/services} in the test resources, so
 * {@code ServiceLoader} can find it. Public with a public no-arg constructor, as the
 * ServiceLoader contract requires. Counts completions across every instance the loader
 * creates, since a fresh loader may instantiate a fresh instance.
 */
public final class DiscoverableTestListener implements TaskExecutionListener<Void> {

    /** Completions seen by any discovered instance, JVM-wide. Reset by tests that read it. */
    public static final AtomicInteger COMPLETIONS = new AtomicInteger();

    public DiscoverableTestListener() {
    }

    @Override
    public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {
        COMPLETIONS.incrementAndGet();
    }
}
