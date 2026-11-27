package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;
import javax.management.JMException;
import javax.management.MBeanServer;
import javax.management.ObjectName;

/**
 * A {@link TaskExecutionListener} that keeps a bounded execution history — the "flight
 * recorder" of the executors family. One {@link ExecutorNode} per executor, mirroring the
 * executor tree; each node keeps a ring of recent {@link TaskRecord}s plus permanent
 * aggregates, and nodes of collected executors are eventually folded into their parents so
 * history is compressed rather than lost. Query with {@link #snapshot()}, render with
 * {@link RecorderSnapshot#renderText()} / {@link RecorderSnapshot#renderJson()}, or expose
 * over JMX with {@link #registerMBean(String)}.
 *
 * <p>Attach explicitly — {@code root.addListener(recorder)} — or use the process-wide
 * {@link #shared()} instance, which is also what {@code root.addDiscoveredListeners()}
 * feeds (this jar registers a delegating entry under {@code META-INF/services}).</p>
 *
 * <p>Built strictly against core's public SPI. The recorder holds <b>no strong reference
 * to any executor, job or throwable</b>: node identity is the executor-id string, the only
 * back-reference is a {@code WeakReference} used for liveness, and failures are rendered
 * to strings at completion time.</p>
 *
 * <h2>Caveats (all inherent, all documented)</h2>
 * <ul>
 *   <li>A snapshot taken immediately after {@code waitForCompletion()} can miss
 *       {@code CANCELLED_AFTER_START} records: that hook fires when the cancelled body
 *       actually returns, which is later than the future settles.</li>
 *   <li>Snapshots are per-node consistent; cross-node totals are approximate while work
 *       is in flight.</li>
 *   <li>Calling {@code addDiscoveredListeners()} twice on one root double-feeds every
 *       discovered listener — a family-wide property, not specific to this recorder.</li>
 *   <li>{@link #shared()} is per-classloader, and discovery resolves through core's class
 *       loader; in layered-classloader containers prefer the explicit
 *       {@code addListener(recorder)} path.</li>
 * </ul>
 */
public final class ExecutionRecorder implements TaskExecutionListener<Void>, ExecutionRecorderMXBean {

    /** JMX domain and type under which {@link #registerMBean(String)} registers. */
    public static final String JMX_DOMAIN = "io.github.grishagrigorov.executors";

    private static final int ROOT_CAUSE_WALK_CAP = 64;

    private static final class SharedHolder {
        static final ExecutionRecorder INSTANCE = new ExecutionRecorder();
    }

    /**
     * The process-wide (strictly: per-classloader) recorder that ServiceLoader discovery
     * feeds. Explicitly constructed recorders are independent of it.
     */
    public static ExecutionRecorder shared() {
        return SharedHolder.INSTANCE;
    }

    private final RecorderConfig config;
    private final ConcurrentHashMap<String, ExecutorNode> index = new ConcurrentHashMap<>();
    private final CopyOnWriteArrayList<ExecutorNode> roots = new CopyOnWriteArrayList<>();
    private final AtomicLong submittedTotal = new AtomicLong();
    private final AtomicLong recordedTotal = new AtomicLong();
    private final AtomicLong failedTotal = new AtomicLong();

    /*
     * Admission accounting. Process-wide totals rather than per-node, because throttling is a
     * property of the harness (one per tree) rather than of any single executor in it — splitting
     * these per node would attribute one shared queue's waiting to whichever child happened to
     * submit.
     */
    private final AtomicLong throttledTotal = new AtomicLong();
    private final AtomicLong throttleWaitNanosTotal = new AtomicLong();
    private final AtomicLong floorAdmittedTotal = new AtomicLong();
    private final Set<ObjectName> registeredNames = new HashSet<>();

    public ExecutionRecorder() {
        this(RecorderConfig.defaults());
    }

    public ExecutionRecorder(RecorderConfig config) {
        this.config = config;
    }

    // ── The listener hooks ───────────────────────────────────────────────────────

    @Override
    public Void onSubmit(TaskDescriptor task) {
        submittedTotal.incrementAndGet();
        // Retry loop: nodeFor may hand back a node that a concurrent snapshot has just
        // folded away; recording into it would write into a detached node.
        while (!nodeFor(task.executor()).recordSubmit()) {
            // fall through and retry via the index — the folded node was removed there
        }
        return null;
    }

    @Override
    public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {

        throttledTotal.incrementAndGet();
        throttleWaitNanosTotal.addAndGet(waitedNanos);

        if (floorAdmitted) {
            floorAdmittedTotal.incrementAndGet();
        }
    }

    @Override
    public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {

        final Throwable failure = outcome.failure();
        final Throwable rootCause = failure == null ? null : rootCauseOf(failure);
        final TaskRecord record = new TaskRecord(
                task.sequence(),
                task.displayName(),
                task.sizeEstimate(),
                task.backend().name(),
                task.submittedAtEpochMillis(),
                outcome.bodyRan() ? outcome.startedAtNanos() - task.submittedAtNanos() : -1L,
                outcome.durationNanos(),
                outcome.status(),
                completionThreadName(),
                describe(failure, rootCause));

        recordedTotal.incrementAndGet();
        if (outcome.status() == TaskStatus.FAILED) {
            failedTotal.incrementAndGet();
        }

        final String rootCauseClass = rootCause == null ? null : rootCause.getClass().getName();
        while (!nodeFor(task.executor()).record(record, config, rootCauseClass)) {
            // folded under us — retry via the index
        }
    }

    // ── Nodes and the tree ───────────────────────────────────────────────────────

    /**
     * The live node for an executor, creating it (and any missing ancestor nodes) on first
     * contact. The returned node may still be folded by a concurrent snapshot before the
     * caller's write lands — mutators return {@code false} in that case and the caller
     * retries, landing on a fresh node.
     */
    private ExecutorNode nodeFor(ThrottledExecutor executor) {

        while (true) {
            final ExecutorNode existing = index.get(executor.executorId);
            if (existing != null && !existing.isFolded()) {
                return existing;
            }
            if (existing != null) {
                index.remove(executor.executorId, existing);
                continue;
            }

            // Ancestors first, so the child can link to a live parent node.
            final ExecutorNode parentNode = executor.parent() == null ? null : nodeFor(executor.parent());
            final ExecutorNode fresh = new ExecutorNode(
                    executor.executorId, executor.executorPath(), executor, parentNode);

            final ExecutorNode raced = index.putIfAbsent(executor.executorId, fresh);
            if (raced != null) {
                continue;   // another thread created it — loop re-validates the folded flag
            }

            if (parentNode == null) {
                roots.add(fresh);
            } else {
                parentNode.attachChild(fresh);
                foldExcessTerminalChildren(parentNode);
            }
            return fresh;
        }
    }

    /**
     * Folds collected children beyond the retention limit into {@code parent}.
     *
     * <p>Eligibility is <b>weak-ref-cleared only</b> — never {@code isClosed()}. A closed
     * executor still delivers events (late bodies, REJECTED submissions), and folding it
     * would double-count when a late event recreates the node. A collected executor cannot
     * deliver anything: an in-flight event holds the descriptor, which strongly holds the
     * executor, so the weak ref cannot clear while an event can still arrive.</p>
     *
     * <p>Locking: candidates are chosen from a copy of the child list; each fold takes the
     * child's monitor (freeze + mark folded), releases it, then the parent's monitor
     * (merge). Two node monitors are never held at once.</p>
     */
    private void foldExcessTerminalChildren(ExecutorNode parent) {

        final List<ExecutorNode> terminal = new ArrayList<>();
        for (ExecutorNode child : parent.childrenSnapshot()) {
            if (child.isCollected()) {
                terminal.add(child);
            }
        }

        final int excess = terminal.size() - config.terminalChildrenLimit();
        for (int i = 0; i < excess; i++) {
            foldSubtree(terminal.get(i), parent);   // oldest first — insertion order
        }
    }

    /** Depth-first: fold {@code node}'s children into it, then {@code node} into {@code into}. */
    private void foldSubtree(ExecutorNode node, ExecutorNode into) {

        for (ExecutorNode child : node.childrenSnapshot()) {
            // A collected parent implies collected children (a child executor strongly
            // references its parent, so the parent could not be collected otherwise).
            foldSubtree(child, node);
        }

        final ExecutorNode.FoldedState state = node.foldAway();
        if (state == null) {
            return;   // already folded by a concurrent snapshot/creation
        }
        into.mergeFoldedChild(node.executorId, state, config);
        index.remove(node.executorId, node);
        roots.remove(node);
    }

    /** Test support: the live node for an executor id, or {@code null}. */
    ExecutorNode node(String executorId) {
        return index.get(executorId);
    }

    // ── Snapshots ────────────────────────────────────────────────────────────────

    /** @return an immutable snapshot of the whole forest, folding overdue terminal children on the way */
    public RecorderSnapshot snapshot() {
        final List<ExecutorNodeSnapshot> rootSnapshots = new ArrayList<>();
        for (ExecutorNode root : roots) {
            if (!root.isFolded()) {
                rootSnapshots.add(snapshotTree(root));
            }
        }
        return new RecorderSnapshot(rootSnapshots);
    }

    private ExecutorNodeSnapshot snapshotTree(ExecutorNode node) {
        foldExcessTerminalChildren(node);
        final List<ExecutorNodeSnapshot> childSnapshots = new ArrayList<>();
        for (ExecutorNode child : node.childrenSnapshot()) {
            childSnapshots.add(snapshotTree(child));
        }
        return node.snapshotOwn(childSnapshots);
    }

    // ── Failure rendering (depth-capped — never trust a cause chain to be acyclic) ──

    private static Throwable rootCauseOf(Throwable failure) {
        Throwable current = failure;
        int hops = 0;
        while (current.getCause() != null && ++hops < ROOT_CAUSE_WALK_CAP) {
            current = current.getCause();
        }
        return current;
    }

    private static String describe(Throwable failure, Throwable rootCause) {
        if (failure == null) {
            return null;
        }
        final String top = oneLine(failure);
        if (rootCause == null || rootCause == failure) {
            return top;
        }
        return top + " (root cause: " + oneLine(rootCause) + ")";
    }

    private static String oneLine(Throwable t) {
        final String message = t.getMessage();
        return message == null ? t.getClass().getName() : t.getClass().getName() + ": " + message;
    }

    private static String completionThreadName() {
        final Thread thread = Thread.currentThread();
        final String name = thread.getName();
        // Virtual threads often have empty names; getId() rather than threadId() — the floor is 17.
        return name.isEmpty() ? "virtual-" + thread.getId() : name;
    }

    // ── JMX ──────────────────────────────────────────────────────────────────────

    @Override
    public long getSubmittedTotal() {
        return submittedTotal.get();
    }

    @Override
    public long getRecordedTotal() {
        return recordedTotal.get();
    }

    @Override
    public long getFailedTotal() {
        return failedTotal.get();
    }

    @Override
    public long getThrottledTotal() {
        return throttledTotal.get();
    }

    @Override
    public long getThrottleWaitNanosTotal() {
        return throttleWaitNanosTotal.get();
    }

    @Override
    public long getFloorAdmittedTotal() {
        return floorAdmittedTotal.get();
    }

    @Override
    public int getExecutorNodeCount() {
        return index.size();
    }

    @Override
    public String renderText() {
        // Admission first: it describes the whole recorder, and when a batch is slow it is the
        // line that says whether the throttle or the work is responsible.
        return "admission: throttled=" + throttledTotal.get()
                + " floor-admitted=" + floorAdmittedTotal.get()
                + " waited=" + throttleWaitNanosTotal.get() + "ns\n"
                + snapshot().renderText();
    }

    @Override
    public String renderJson() {
        return snapshot().renderJson(
                "\"throttledTotal\":" + throttledTotal.get() + ','
                        + "\"floorAdmittedTotal\":" + floorAdmittedTotal.get() + ','
                        + "\"throttleWaitNanosTotal\":" + throttleWaitNanosTotal.get() + ',');
    }

    /**
     * Registers this recorder on the platform MBean server as
     * {@code io.github.grishagrigorov.executors:type=ExecutionRecorder,name=<name>}.
     * Idempotent per recorder and name; the name is always quoted, so any string is safe.
     *
     * @return this recorder, for chaining
     * @throws IllegalStateException if the name is already taken by a different MBean
     */
    public synchronized ExecutionRecorder registerMBean(String name) {
        final ObjectName objectName = objectNameFor(name);
        if (registeredNames.contains(objectName)) {
            return this;
        }
        try {
            ManagementFactory.getPlatformMBeanServer().registerMBean(this, objectName);
            registeredNames.add(objectName);
        } catch (JMException e) {
            throw new IllegalStateException("Could not register " + objectName, e);
        }
        return this;
    }

    /** Unregisters a previously registered name; a name never registered here is a no-op. */
    public synchronized void unregisterMBean(String name) {
        final ObjectName objectName = objectNameFor(name);
        if (!registeredNames.remove(objectName)) {
            return;
        }
        try {
            final MBeanServer server = ManagementFactory.getPlatformMBeanServer();
            if (server.isRegistered(objectName)) {
                server.unregisterMBean(objectName);
            }
        } catch (JMException e) {
            throw new IllegalStateException("Could not unregister " + objectName, e);
        }
    }

    private static ObjectName objectNameFor(String name) {
        try {
            // Always quoted: ,=:"*? are ObjectName metacharacters and quoting is harmless otherwise.
            return new ObjectName(JMX_DOMAIN + ":type=ExecutionRecorder,name=" + ObjectName.quote(name));
        } catch (JMException e) {
            throw new IllegalArgumentException("Invalid MBean name: " + name, e);
        }
    }
}
