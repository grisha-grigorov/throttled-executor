package io.github.grishagrigorov.executors.jfr;

import jdk.jfr.Category;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/**
 * One event per throttled task, recording what admission cost it.
 *
 * <p>Separate from {@link TaskEvent} rather than a field on it, because the two answer different
 * questions and are wanted at different times. {@code TaskEvent} covers every submission and is
 * about the task; this one exists only for throttled tasks and is about the <em>throttle</em>. A
 * recording that wants to know why a batch was slow can enable this alone, and pay nothing for
 * the rest.</p>
 *
 * <p>The field worth filtering on is {@code floorAdmitted}: it means the policy refused the task
 * and the liveness floor started it anyway. A handful of those is the floor doing its job; a
 * batch full of them means the throttle is refusing everything and the floor is the only thing
 * making progress — which is a serial batch wearing a parallel batch's clothes.</p>
 *
 * <p>Instantaneous, not timed: the wait is carried as an explicit {@code @Timespan} field, since
 * the event is committed after the wait has already happened.</p>
 */
@Name(ThrottleEvent.NAME)
@Label("Executor throttle")
@Category({"Executors"})
@Description("A throttled task's admission: how long it waited to be admitted, and whether the policy permitted it or the liveness floor overrode it.")
@StackTrace(false)
public final class ThrottleEvent extends Event {

    /** The event type name, as used in {@code Recording.enable(...)} and .jfc files. */
    public static final String NAME = "io.github.grishagrigorov.executors.Throttle";

    @Label("Executor id")
    public String executorId;

    @Label("Executor path")
    @Description("Root-first path through the executor tree, e.g. Voyage -> Leg-3 -> FuelCalc")
    public String executorPath;

    @Label("Task")
    public String taskName;

    @Label("Sequence")
    @Description("Per-executor submission sequence number")
    public long sequence;

    @Label("Waited")
    @Description("How long this task was queued before it was admitted, measured from when its coordinator first offered it. Not 'how long the policy refused it' — a task admitted on the first scan still reports a small non-zero time")
    @Timespan(Timespan.NANOSECONDS)
    public long waitedNanos;

    @Label("Floor admitted")
    @Description("True if the policy refused this task and the liveness floor started it anyway")
    public boolean floorAdmitted;
}
