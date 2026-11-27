package io.github.grishagrigorov.executors.micrometer;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

/**
 * Publishes executor activity to a Micrometer {@link MeterRegistry}.
 *
 * <p>{@code executors-observability} answers "what happened in this JVM, right now", through its
 * own MBean and renderings. This answers "what is happening over time", through whatever the
 * application already runs — Prometheus, a Spring Boot actuator endpoint, JMX. Neither replaces
 * the other, and they can be attached together.</p>
 *
 * <pre>{@code
 * var root = new ThrottledExecutor("NightlyReports");
 * root.addListener(new MicrometerTaskListener(meterRegistry));
 * }</pre>
 *
 * <h2>The meters</h2>
 *
 * <table border="1">
 *   <caption>Meters published, all prefixed {@code executors.} by default</caption>
 *   <tr><th>Meter</th><th>Type</th><th>Meaning</th></tr>
 *   <tr><td>{@code executors.tasks}</td><td>counter</td>
 *       <td>completed tasks, tagged {@code status} and {@code executor}</td></tr>
 *   <tr><td>{@code executors.task.duration}</td><td>timer</td>
 *       <td>body wall time, for tasks that ran</td></tr>
 *   <tr><td>{@code executors.task.queued}</td><td>timer</td>
 *       <td>submission to body start</td></tr>
 *   <tr><td>{@code executors.throttle.wait}</td><td>timer</td>
 *       <td>how long a task was queued before admission, from when its coordinator first
 *           offered it — not how long the policy refused it</td></tr>
 *   <tr><td>{@code executors.throttle.floorAdmitted}</td><td>counter</td>
 *       <td>tasks the policy refused and the liveness floor started anyway</td></tr>
 * </table>
 *
 * <p>Tasks are tagged by <b>executor id</b>, not by task name. Task names are chosen by callers
 * and can be unbounded — one per voyage, one per report — and an unbounded tag value is the
 * classic way to melt a metrics backend. Executor ids come from a small fixed set the
 * application controls. If you want per-task-name metrics, that is a decision to make with a
 * known-bounded name set, and a good reason to write your own listener.</p>
 *
 * <p>Meters are resolved through the registry on every call rather than cached in fields.
 * Micrometer's registry already does that lookup cheaply and it keeps this listener correct when
 * a registry is reconfigured underneath it.</p>
 *
 * <p>No {@code META-INF/services} entry: this listener needs a registry, and a
 * {@link java.util.ServiceLoader} cannot supply one. Attach it explicitly.</p>
 */
public final class MicrometerTaskListener implements TaskExecutionListener<Void> {

    /** Meter name prefix used when none is given. */
    public static final String DEFAULT_PREFIX = "executors";

    private final MeterRegistry registry;
    private final String prefix;

    /**
     * @param registry where the meters are published
     */
    public MicrometerTaskListener(MeterRegistry registry) {
        this(registry, DEFAULT_PREFIX);
    }

    /**
     * @param registry where the meters are published
     * @param prefix   meter name prefix, without a trailing dot
     */
    public MicrometerTaskListener(MeterRegistry registry, String prefix) {
        this.registry = Objects.requireNonNull(registry, "registry");
        this.prefix = Objects.requireNonNull(prefix, "prefix");
    }

    @Override
    public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {

        Timer.builder(prefix + ".throttle.wait")
                .tag("executor", task.executorId())
                .register(registry)
                .record(waitedNanos, TimeUnit.NANOSECONDS);

        if (floorAdmitted) {
            // The one worth alarming on: the policy refused this task and it ran regardless,
            // which means the throttle is no longer the thing deciding concurrency.
            Counter.builder(prefix + ".throttle.floorAdmitted")
                    .tag("executor", task.executorId())
                    .register(registry)
                    .increment();
        }
    }

    @Override
    public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {

        Counter.builder(prefix + ".tasks")
                .tag("executor", task.executorId())
                .tag("status", outcome.status().name())
                .register(registry)
                .increment();

        if (!outcome.bodyRan()) {
            // Nothing to time: a rejected or queue-cancelled task has no body duration, and no
            // queue time either — recording zero would drag both averages towards a lie.
            return;
        }

        Timer.builder(prefix + ".task.duration")
                .tag("executor", task.executorId())
                .register(registry)
                .record(outcome.durationNanos(), TimeUnit.NANOSECONDS);

        Timer.builder(prefix + ".task.queued")
                .tag("executor", task.executorId())
                .register(registry)
                .record(outcome.startedAtNanos() - task.submittedAtNanos(), TimeUnit.NANOSECONDS);
    }
}
