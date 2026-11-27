package io.github.grishagrigorov.executors.jfr;

import jdk.jfr.Category;
import jdk.jfr.DataAmount;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;
import jdk.jfr.Timespan;

/**
 * One Flight Recorder event per task submitted to a {@code ThrottledExecutor}.
 *
 * <p>{@code begin()} is called when the body starts and {@code end()} when it finishes, so
 * the event's duration is the body's wall time and JDK Mission Control draws it as a bar on
 * the worker thread's timeline — next to the GC pauses, lock waits and socket reads that
 * happened during it. Tasks that never ran (rejected, cancelled while queued) are emitted
 * too, with no duration: in a production recording those are often the interesting ones.</p>
 *
 * <p>Holds strings and numbers only — never the job, never the {@code Throwable} — so a
 * long recording pins nothing.</p>
 *
 * <p>Enable with any recording, e.g. {@code -XX:StartFlightRecording:filename=app.jfr}; the
 * event is in the "Executors" category. Stack traces are off by default (the submitting
 * stack is rarely interesting and costs per task); turn them on in a custom .jfc if needed.</p>
 */
@Name(TaskEvent.NAME)
@Label("Executor task")
@Category({"Executors"})
@Description("A task submitted to a ThrottledExecutor: its identity, where in the executor tree it ran, how it ended, and how long its body took.")
@StackTrace(false)
public final class TaskEvent extends Event {

    /** The event type name, as used in {@code Recording.enable(...)} and .jfc files. */
    public static final String NAME = "io.github.grishagrigorov.executors.Task";

    @Label("Executor id")
    public String executorId;

    @Label("Executor path")
    @Description("Root-first path through the executor tree, e.g. Voyage -> Leg-3 -> FuelCalc")
    public String executorPath;

    @Label("Task")
    @Description("The ThrottledTask name for throttled submissions, else task#<sequence>")
    public String taskName;

    @Label("Sequence")
    @Description("Per-executor submission sequence number")
    public long sequence;

    @Label("Size estimate")
    @DataAmount
    @Description("The ThrottledTask size estimate in bytes; -1 when the submission carried none")
    public long sizeEstimate;

    @Label("Backend")
    public String backend;

    @Label("Status")
    @Description("COMPLETED, FAILED, CANCELLED_BEFORE_START, CANCELLED_AFTER_START or REJECTED")
    public String status;

    @Label("Body ran")
    public boolean bodyRan;

    @Label("Queued")
    @Timespan(Timespan.NANOSECONDS)
    @Description("Time between submission and the body starting; 0 if it never started")
    public long queuedNanos;

    @Label("Failure")
    @Description("Failure class and message for FAILED / cancelled / rejected outcomes; never the Throwable itself")
    public String failure;
}
