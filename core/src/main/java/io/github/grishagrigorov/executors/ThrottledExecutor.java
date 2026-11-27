package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;


import io.github.grishagrigorov.executors.exceptions.RootCauseExceptionUtil;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Throttled batch executor over a shared backing pool (see {@link ExecutorBackend}).
 * Supports submitting {@link Runnable}, {@link Callable}, and {@link Supplier} tasks, with
 * MDC context propagation and an explicit parent/child tree. Optionally throttles
 * submissions via a pluggable {@link AdmissionPolicy} (see {@link #submitThrottled}), and
 * exposes every task's lifecycle to {@link TaskExecutionListener}s (see {@link #addListener}).
 *
 * <p>Multiple {@code ThrottledExecutor} instances share the pools; each instance
 * tracks its own set of futures and can independently wait for completion or cancel its
 * tasks.</p>
 *
 * @see AdmissionPolicy
 * @see TaskExecutionListener
 * @see ThreadContextAwareCompletionException
 */
public class ThrottledExecutor extends AbstractExecutorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottledExecutor.class);

    /**
     * Listeners every task goes through before any user listener, in this order. Not part of
     * any executor's own list and therefore not removable: a caller who could remove
     * {@link AmbientExecutorListener} would silently break {@link #withAmbientDiscovery}. They
     * are still genuine listeners, dispatched through the same loops as user ones — the
     * proof that the SPI is sufficient for the library's own cross-cutting concerns.
     */
    private static final List<TaskExecutionListener<?>> BUILT_IN_LISTENERS =
            List.of(AmbientExecutorListener.INSTANCE, MdcPropagationListener.INSTANCE);

    /**
     * Listeners added to THIS executor. Effective listeners for a task = built-ins, then every
     * ancestor's own listeners root-first, then these. See {@link #effectiveListeners()}.
     */
    private final CopyOnWriteArrayList<TaskExecutionListener<?>> ownListeners = new CopyOnWriteArrayList<>();

    private final ConcurrentLinkedQueue<FuturePair> futures = new ConcurrentLinkedQueue<>();


    /**
     * Non-blocking sink for failures raised by this executor's tasks.
     *
     * <p>This was previously a {@code ThreadLocal<List<Throwable>>} paired with a queue of
     * every list it had ever created, on the reasoning that a per-thread list needs no
     * synchronisation. That reasoning does not survive contact with a thread-per-task
     * backend: the registry entry was never removed, so "one retained list per thread"
     * becomes "one retained list per task", growing without bound for as long as the
     * executor lives. A single lock-free queue is just as non-blocking and holds no
     * per-thread state at all.</p>
     *
     * @see #drainCollectedThrowables()
     */
    private final ConcurrentLinkedQueue<Throwable> collectedThrowables = new ConcurrentLinkedQueue<>();

    /**
     * Removes and returns every throwable collected so far.
     *
     * <p>Draining rather than copying keeps the executor reusable: a later batch on the
     * same instance reports its own failures only, instead of re-reporting every failure
     * this executor has ever seen.</p>
     *
     * @return the collected throwables, in completion order; empty if there were none
     */
    private List<Throwable> drainCollectedThrowables() {

        final List<Throwable> drained = new ArrayList<>();

        for (Throwable throwable = collectedThrowables.poll();
                throwable != null;
                throwable = collectedThrowables.poll()) {
            drained.add(throwable);
        }

        return drained;
    }

    /**
     * Cancellation dispatcher for {@link #cancelRemainingRunningTasks()}.
     *
     * <p>Daemon-threaded and lazily created: it used to be a non-daemon thread that
     * nothing ever shut down, so once anything had cancelled tasks it kept the JVM alive
     * after {@code main} returned. Lazy because most executors never cancel anything.</p>
     */
    private static volatile ExecutorService cancellationExecutor;

    private static synchronized ExecutorService cancellationExecutor() {

        if (cancellationExecutor == null) {
            cancellationExecutor = Executors.newSingleThreadExecutor(runnable -> {
                final Thread thread = new Thread(runnable, "executors-cancellation");
                thread.setDaemon(true);
                return thread;
            });
        }

        return cancellationExecutor;
    }

    /** Which pool this executor's tasks run on. See {@link ExecutorBackend}. */
    private final ExecutorBackend backend;


    public final String executorId;
    private static final AtomicInteger instanceCounter = new AtomicInteger(0);

    /**
     * Upper bound on a coordinator's blocking wait in {@link #submitThrottled}. Doubles as the
     * self-heal interval (recovery from any unforeseen missed signal) and the poll interval for
     * time-based policies (e.g. RateLimitPolicy), whose denials no task completion can lift.
     */
    private static final long THROTTLE_POLL_MILLIS = 100;

    /**
     * Sentinel for "wait as long as it takes", distinguishing the untimed
     * {@link #waitForCompletion()} from {@link #waitForCompletion(Duration)} without a second
     * parameter. A real {@link System#nanoTime()} deadline is never this value.
     */
    private static final long NO_DEADLINE = Long.MIN_VALUE;

    /**
     * How many candidates {@link #submitThrottled(Iterator, AdmissionPolicy)} holds at once.
     *
     * <p>Large enough that {@code selectionOrder()} has something to choose between, small
     * enough that the point of streaming — bounded memory — is not lost.</p>
     */
    private static final int DEFAULT_STREAM_WINDOW = 256;

    /**
     * Wait figure for a task that went through no policy at all — a plain {@code submit()}.
     *
     * <p>Distinct from zero, which means "throttled, and admitted on the first scan":
     * {@code onThrottled} fires for the second and not the first.</p>
     */
    private static final long NOT_THROTTLED = -1;

    /**
     * Framework-owned throttle mechanics, shared by every executor in a subtree.
     *
     * <p>A child delegates to its parent's harness, so the whole subtree admits against
     * ONE set of counters. Previously each executor owned its own, which meant every
     * executor in a nested tree independently believed the entire budget was available to
     * it — a tree of ten overcommitted tenfold. That only ever looked safe because the
     * fixed pool capped total concurrency regardless; it does not survive a
     * thread-per-task backend.</p>
     */
    private final ThrottleHarness throttleHarness;

    /** The executor that owns this one, or {@code null} for a root. */
    private final ThrottledExecutor parent;

    /**
     * Children, held WEAKLY.
     *
     * <p>A long-lived root in a desktop application would otherwise retain every child it
     * had ever created. Weak references are safe here: a child with work in flight is
     * strongly reachable anyway, through the task lambdas its own futures hold, so a
     * child that has become weakly unreachable provably has neither pending work nor a
     * live handle. Cleared entries are compacted whenever the registry is traversed or
     * added to, which bounds it without a reference queue.</p>
     */
    private final ConcurrentLinkedQueue<WeakReference<ThrottledExecutor>> children =
            new ConcurrentLinkedQueue<>();

    /**
     * Monotonic counters used to detect whether new work appeared during a wait.
     *
     * <p>Monotonic on purpose: the size of {@link #futures} also changes when completed
     * entries are compacted away, so it cannot distinguish "new work arrived" from
     * "old work was cleaned up".</p>
     */
    private final AtomicLong submissionCounter = new AtomicLong();
    private final AtomicLong childCounter = new AtomicLong();

    /** Compaction interval for {@link #futures}, in submissions. */
    private static final long FUTURES_COMPACTION_INTERVAL = 128;

    /** Safety bound on the transitive-wait fixpoint loop. */
    private static final int MAX_QUIESCENCE_PASSES = 1_000;

    public static ThrottledExecutor build(String idPrefix, ThrottledExecutor parentExecutor){
        return new ThrottledExecutor(idPrefix, parentExecutor);
    }

    /**
     * Creates an executor whose tasks run on {@code backend} rather than the default pool.
     *
     * <p>A named factory rather than a constructor overload on purpose: a
     * {@code (String, ExecutorBackend)} constructor would be ambiguous against the
     * existing {@code (String, ThrottledExecutor)} one at any {@code (id, null)} call
     * site, turning an opt-in feature into a compile break for existing callers.</p>
     *
     * @param idPrefix human-readable prefix for this executor's id
     * @param backend  which pool to submit to; see {@link ExecutorBackend} on choosing
     */
    public static ThrottledExecutor withBackend(String idPrefix, ExecutorBackend backend) {
        return new ThrottledExecutor(idPrefix, null, backend);
    }

    /**
     * Creates a ROOT executor.
     *
     * <p>Always a root, even when called from inside another executor's running task.
     * Construction site is not ownership: a field initialiser or an injected executor
     * would otherwise be silently adopted by whatever task happened to be on the stack.
     * Use {@link #newChild(String)} or {@link #withAmbientDiscovery(String)} to create a
     * child deliberately.</p>
     */
    public ThrottledExecutor(String idPrefix) {
        this(idPrefix, (ThrottledExecutor) null);
    }

    /**
     * Creates an executor owned by {@code parentExecutor}, or a root when it is
     * {@code null}.
     */
    public ThrottledExecutor(String idPrefix, ThrottledExecutor parentExecutor) {
        this(idPrefix, parentExecutor, null);
    }

    private ThrottledExecutor(String idPrefix, ThrottledExecutor parentExecutor, ExecutorBackend backend) {

        this.parent = parentExecutor;

        // A child inherits its parent's pool unless told otherwise; a subtree normally
        // wants to run on one backend.
        this.backend = backend != null ? backend
                : parentExecutor != null ? parentExecutor.backend
                : ExecutorBackend.defaultBackend();

        // The whole subtree admits against the root's counters.
        this.throttleHarness = parentExecutor != null ? parentExecutor.throttleHarness : new ThrottleHarness();

        // Listeners need no wiring here: inheritance falls out of the parent chain at
        // submission time (effectiveListeners()), so every construction path — newChild,
        // build(id, parent), withAmbientDiscovery — inherits identically.

        executorId = "ThrottledExecutor[" + idPrefix + "-" + instanceCounter.getAndIncrement() + "]";

        if (parentExecutor != null) {
            parentExecutor.registerChild(this);
        }
    }

    /**
     * Creates a child of this executor. The primary way to build a tree: explicit,
     * unmissable at the call site, and impossible to get wrong. The child inherits this
     * executor's backend and, live, its listeners.
     */
    public ThrottledExecutor newChild(String idPrefix) {
        return new ThrottledExecutor(idPrefix, this, backend);
    }

    /**
     * Creates an executor that adopts whichever executor's task is running on this thread,
     * falling back to a root when there is none.
     *
     * <p>The escape hatch for code that genuinely cannot reach its parent lexically —
     * framework callbacks, deep call stacks. Opting in per instance keeps the ownership
     * decision at the call site, where the knowledge actually is; the plain constructor
     * never consults ambient context.</p>
     */
    public static ThrottledExecutor withAmbientDiscovery(String idPrefix) {
        return new ThrottledExecutor(idPrefix, AmbientExecutorListener.current(), null);
    }

    /** @return the executor whose task is running on this thread, or {@code null} */
    public static ThrottledExecutor currentAmbient() {
        return AmbientExecutorListener.current();
    }

    // ── Listeners ────────────────────────────────────────────────────────────────────────

    /**
     * Registers a listener on this executor. It sees every task submitted to this executor
     * and, through live inheritance, to every descendant — including descendants created
     * before the listener was added. Tasks already submitted are not affected.
     *
     * @return this executor, for chaining
     * @see TaskExecutionListener
     */
    public ThrottledExecutor addListener(TaskExecutionListener<?> listener) {
        ownListeners.add(java.util.Objects.requireNonNull(listener, "listener"));
        return this;
    }

    /**
     * Attaches every {@link TaskExecutionListener} registered for {@link java.util.ServiceLoader}
     * discovery (a {@code META-INF/services/io.github.grishagrigorov.executors.spi.TaskExecutionListener}
     * entry on the classpath) to <em>this</em> executor, as if each had been passed to
     * {@link #addListener}. Satellite modules such as {@code executors-jfr} ship such an entry.
     *
     * <p><strong>Explicit and per root, never automatic.</strong> A jar's mere presence on the
     * classpath must not change an executor's behaviour; discovery is something a root asks
     * for, and the result is inherited by that root's subtree like any other listener. This
     * is the same stance as construction-site-≠-ownership: nothing is adopted by accident.</p>
     *
     * <p>Loaded through this class's own class loader, not the thread-context loader, so the
     * result does not depend on which thread happens to call it.</p>
     *
     * @return this executor, for chaining
     */
    public ThrottledExecutor addDiscoveredListeners() {

        final java.util.ServiceLoader<TaskExecutionListener> discovered = java.util.ServiceLoader.load(
                TaskExecutionListener.class, ThrottledExecutor.class.getClassLoader());

        for (TaskExecutionListener<?> listener : discovered) {
            addListener(listener);
        }

        return this;
    }

    /**
     * Removes a listener previously added to <em>this</em> executor.
     *
     * <p>Inherited listeners cannot be removed from a child: they represent a decision made
     * higher up the tree. Built-in listeners cannot be removed at all.</p>
     *
     * @return {@code true} if the listener was registered here and has been removed
     */
    public boolean removeListener(TaskExecutionListener<?> listener) {
        return ownListeners.remove(listener);
    }

    /**
     * The listeners a task submitted to this executor goes through, in {@code onSubmit} /
     * {@code onStart} order: built-ins, then each ancestor's own listeners root-first, then
     * this executor's own. Computed per submission — a walk up a tree that is rarely more
     * than a few levels deep — rather than cached, because a cache keyed on a tree-wide
     * generation is a stale-tag bug waiting to happen and buys nothing measurable.
     */
    private List<TaskExecutionListener<?>> effectiveListeners() {

        // Collect leaf-to-root, then emit root-to-leaf.
        final List<List<TaskExecutionListener<?>>> chain = new ArrayList<>();
        for (ThrottledExecutor current = this; current != null; current = current.parent) {
            if (!current.ownListeners.isEmpty()) {
                chain.add(current.ownListeners);
            }
        }

        if (chain.isEmpty()) {
            return BUILT_IN_LISTENERS;
        }

        final List<TaskExecutionListener<?>> effective = new ArrayList<>(BUILT_IN_LISTENERS);
        for (int i = chain.size() - 1; i >= 0; i--) {
            effective.addAll(chain.get(i));
        }
        return effective;
    }

    /** @return the pool this executor's tasks run on */
    public ExecutorBackend backend() {
        return backend;
    }

    /** @return the executor that owns this one, or {@code null} if this is a root */
    public ThrottledExecutor parent() {
        return parent;
    }

    /** @return whether this executor sits at the top of its tree */
    public boolean isRoot() {
        return parent == null;
    }

    /** @return the root of this executor's tree; {@code this} when it is already a root */
    public ThrottledExecutor root() {

        ThrottledExecutor current = this;

        while (current.parent != null) {
            current = current.parent;
        }

        return current;
    }

    /**
     * @return this executor's position in the tree, root first, e.g.
     *         {@code Voyage-0 → Leg-3 → FuelCalc-12}
     */
    public String executorPath() {

        final List<String> segments = new ArrayList<>();

        for (ThrottledExecutor current = this; current != null; current = current.parent) {
            segments.add(current.executorId);
        }

        Collections.reverse(segments);

        return String.join(" → ", segments);
    }

    private void registerChild(ThrottledExecutor child) {
        childCounter.incrementAndGet();
        compactChildren();
        children.add(new WeakReference<>(child));
    }

    /** Drops entries whose referent has been collected, or which have deregistered. */
    private void compactChildren() {
        children.removeIf(reference -> reference.get() == null);
    }

    /** @return this executor's currently-reachable children */
    private List<ThrottledExecutor> liveChildren() {

        final List<ThrottledExecutor> live = new ArrayList<>();

        children.removeIf(reference -> {
            final ThrottledExecutor child = reference.get();
            if (child == null) {
                return true;
            }
            live.add(child);
            return false;
        });

        return live;
    }

    /** @return how many children of this executor are still reachable. Test support. */
    int liveChildCount() {
        return liveChildren().size();
    }

    private void deregisterChild(ThrottledExecutor child) {
        children.removeIf(reference -> {
            final ThrottledExecutor candidate = reference.get();
            return candidate == null || candidate == child;
        });
    }

    /**
     * Shuts down every process-wide pool this class owns: whichever backends have been
     * used, plus the cancellation dispatcher.
     *
     * <p>A pool that was never created is left alone rather than being allocated purely
     * so it can be closed.</p>
     */
    public static void shutdownThreadExecutors() {

        LOGGER.info(LogMarkers.POOL_SHUTDOWN, "Shutting down ThrottledExecutor thread executors...");

        ExecutorBackend.shutdownAll();

        final ExecutorService cancellation = cancellationExecutor;
        if (cancellation != null) {
            ExecutorBackend.shutdownQuietly(cancellation, "cancellation executor");
        }
    }

    /** @return true when no pool owned by this class is still running. Test support. */
    static boolean allPoolsShutDown() {

        final ExecutorService cancellation = cancellationExecutor;
        final boolean cancellationDone = cancellation == null || cancellation.isShutdown();

        return ExecutorBackend.allPoolsShutDown() && cancellationDone;
    }

    public int totalJobs() {
        return futures.size();
    }

    public Stream<FuturePair> getFutures() {
        return futures.stream();
    }

    public CompletableFuture[] submitRunnables(Runnable[] list) {
        return submitRunnables(Arrays.asList(list));
    }

    /**
     * Submits {@code jobs} throttled by the fail-safe default policy for this executor's
     * backend ({@link AdmissionPolicy#defaultPolicy(ExecutorBackend)}).
     *
     * @see #submitThrottled(List, AdmissionPolicy, boolean)
     */
    public <V> List<CompletableFuture<?>> submitThrottled(List<ThrottledTask<V>> jobs) {
        // Sized for THIS executor's backend: the concurrency cap means very different
        // things on a fixed pool and on a thread-per-task one.
        return submitThrottled(jobs, AdmissionPolicy.defaultPolicy(backend), false);
    }

    /**
     * Submits {@code jobs} throttled by {@code policy}, keeping completed
     * futures in the internal list.
     *
     * @see #submitThrottled(List, AdmissionPolicy, boolean)
     */
    public <V> List<CompletableFuture<?>> submitThrottled(List<ThrottledTask<V>> jobs, AdmissionPolicy policy) {
        return submitThrottled(jobs, policy, false);
    }

    /**
     * Submits {@code jobs} throttled by {@code policy}, waits for all of them, and returns their
     * values in <b>submission order</b>.
     *
     * <p>The batch-shaped convenience over {@link #submitThrottled(List, AdmissionPolicy)}: it
     * blocks like {@link #waitForCompletion()} and throws the same aggregate exception if any
     * task failed, so a caller that just wants the results never touches a future.</p>
     *
     * <pre>{@code
     * List<ThrottledTask<Callable<Report>>> batch = List.of(
     *         ThrottledTask.of(() -> buildNoon(day), 40L << 20, "NoonReport"),
     *         ThrottledTask.of(() -> buildLeg(leg),  90L << 20, "LegReport"));
     *
     * List<Report> reports = executor.invokeAllThrottled(batch, AdmissionPolicy.defaultPolicy());
     * }</pre>
     *
     * <p>Two ways this does not return values. If any task failed, the aggregate is thrown, as
     * {@link #waitForCompletion()} throws it. If the submitting thread was <em>interrupted</em>
     * mid-batch, part of the work was never submitted and has no result, so a
     * {@link java.util.concurrent.CancellationException} naming this executor is thrown and the
     * interrupt flag is restored — the tasks that did run are not reported, because a partial
     * list silently missing entries is worse than an exception.</p>
     *
     * <p>It can also block for longer than the work takes: {@code waitForCompletion()} gives up
     * after {@code MAX_QUIESCENCE_PASSES} against tasks that keep spawning more, and this then
     * joins whatever is still running. Use {@link #submitThrottled(List, AdmissionPolicy)} with
     * {@link #waitForCompletion(Duration)} when a bound matters.</p>
     *
     * @param <R>    the value type the callables produce
     * @param jobs   tasks whose jobs are {@code Callable}s
     * @param policy admission/ordering policy
     * @return one value per task, in the order {@code jobs} listed them
     * @throws ThreadContextAwareCompletionException if any task in the subtree failed
     * @throws java.util.concurrent.CancellationException if the submitting thread was interrupted
     */
    public <R> List<R> invokeAllThrottled(List<ThrottledTask<Callable<R>>> jobs, AdmissionPolicy policy) {

        final List<CompletableFuture<?>> futures = submitThrottled(jobs, policy, false);

        /*
         * Checked BEFORE waiting, not after.
         *
         * These placeholders mean the submitting thread was interrupted and the tail of the
         * batch was never submitted — so there is no complete set of results to be had, however
         * long anyone waits. waitForCompletion() blocks on CompletableFuture.join(), which is
         * uninterruptible: an interrupt flag that is already set does nothing to shorten it. A
         * shutdown that interrupted a submitter half way through a long batch used to abandon
         * the rest and then sit waiting for everything it had already started, which is the
         * opposite of what interrupting it was for.
         *
         * Tested by type, NOT by isCancelled(): a real task cancelled by anyone else — another
         * thread's cancelSubtree(), a future taken from getFutures() — is also cancelled, and
         * treating that as "the submitter was interrupted" set an interrupt flag on a thread
         * nobody had interrupted, breaking its next blocking call.
         *
         * The already-submitted tasks keep running and stay tracked, so a later
         * waitForCompletion() or close() still accounts for them and their failures.
         */
        for (CompletableFuture<?> future : futures) {
            if (future instanceof AbandonedFuture) {
                Thread.currentThread().interrupt();
                throw new java.util.concurrent.CancellationException(executorPath()
                        + ": the submitting thread was interrupted, so part of the batch was "
                        + "never submitted and has no result");
            }
        }

        if (runningInsideSubtree()) {
            /*
             * From inside a task of this executor (or of a child), waitForCompletion() would
             * join every future the executor tracks — the calling task's own included, which
             * settles only when the caller returns, which it cannot until this returns. A
             * deadlock on every backend, and a hang of the submitting thread on CALLER_RUNS.
             * The batch's own futures are all this caller needs; the enclosing wait, the one
             * that can legitimately wait on the calling task, still covers the subtree.
             */
            awaitBatch(futures);
        } else {
            // Aggregates and throws for the whole subtree. It normally leaves every future here
            // settled — but not always: it gives up at MAX_QUIESCENCE_PASSES against work that
            // keeps spawning more.
            waitForCompletion();
        }

        final List<R> values = new ArrayList<>(futures.size());

        for (CompletableFuture<?> future : futures) {

            @SuppressWarnings("unchecked")
            final R value = (R) future.join();
            values.add(value);
        }
        return values;
    }

    /**
     * Whether the current thread is running a task of this executor or of one of its
     * descendants — the situation in which a transitive wait on this executor would wait on
     * the caller itself.
     */
    private boolean runningInsideSubtree() {
        for (ThrottledExecutor e = AmbientExecutorListener.current(); e != null; e = e.parent()) {
            if (e == this) {
                return true;
            }
        }
        return false;
    }

    /**
     * Waits for exactly {@code batch} and raises its failures as the usual aggregate — once.
     *
     * <p>The failures were recorded into {@link #collectedThrowables} when they happened, for
     * the enclosing {@code waitForCompletion()} to drain. Reporting them here as well would
     * report them twice, so the ones this aggregate carries are taken back out of the queue.</p>
     */
    private void awaitBatch(List<CompletableFuture<?>> batch) {

        final List<Throwable> failures = new ArrayList<>();
        final java.util.Set<CompletableFuture<?>> failed =
                java.util.Collections.newSetFromMap(new java.util.IdentityHashMap<>());

        for (CompletableFuture<?> future : batch) {
            try {
                future.join();
            } catch (CompletionException wrapped) {
                failures.add(wrapped.getCause() != null ? wrapped.getCause() : wrapped);
                failed.add(future);
            } catch (CancellationException cancelled) {
                failures.add(cancelled);
                failed.add(future);
            }
        }

        if (failures.isEmpty()) {
            return;
        }

        /*
         * Reported here means reported. The enclosing waitForCompletion() keeps a safety net —
         * "a future that completed exceptionally with nothing recorded is surfaced anyway" — so
         * merely draining these from the queue would have that net raise them a second time.
         * Dropping the settled pairs from the tracked set is what the periodic compaction does
         * to every completed future regardless; doing it now, for exactly the failures this
         * caller has taken responsibility for, keeps the net honest.
         */
        futures.removeIf(pair -> failed.contains(pair.getCompletableFuture()));

        /*
         * recordFailure() queued each of these WRAPPED in a ThreadContextAwareCompletionException
         * (or as-is, when it already was one), so the match is on the wrapper's cause, by
         * identity — the raw throwable is the one that settled the future. The wrappers, not
         * the raw failures, go into the aggregate, so it has the same shape as the one
         * waitForCompletion() builds.
         */
        final List<Throwable> reported = new ArrayList<>(failures.size());

        for (java.util.Iterator<Throwable> it = collectedThrowables.iterator(); it.hasNext(); ) {
            final Throwable queued = it.next();
            for (Throwable failure : failures) {
                if (queued == failure || queued.getCause() == failure) {
                    it.remove();
                    reported.add(queued);
                    break;
                }
            }
        }

        for (Throwable failure : failures) {
            // Belt and braces: a failure that somehow was not queued is still reported.
            if (reported.stream().noneMatch(r -> r == failure || r.getCause() == failure)) {
                reported.add(wrapThrowable(failure));
            }
        }

        throw aggregate(reported);
    }

    /**
     * Submits {@code jobs} throttled by the default policy for this executor's backend, waits,
     * and returns their values in submission order.
     *
     * @param <R>  the value type the callables produce
     * @param jobs tasks whose jobs are {@code Callable}s
     * @return one value per task, in the order {@code jobs} listed them
     * @see #invokeAllThrottled(List, AdmissionPolicy)
     */
    public <R> List<R> invokeAllThrottled(List<ThrottledTask<Callable<R>>> jobs) {
        return invokeAllThrottled(jobs, AdmissionPolicy.defaultPolicy(backend));
    }

    /**
     * Submits every task in {@code jobs} through the pluggable throttle {@code policy}.
     *
     * <p>Candidates are offered in {@code policy.selectionOrder()}; the first admitted
     * candidate is submitted. When nothing is running, the next candidate is admitted
     * unconditionally (liveness floor — the executor never throttles to zero). When no
     * candidate is admitted, the calling thread blocks until a task completes (or a
     * short poll interval elapses, so time-based policies re-evaluate) and re-scans.</p>
     *
     * <p>The caller's list is never mutated. If the calling thread is interrupted while
     * blocked, the method restores the interrupt flag and returns; tasks not yet
     * submitted are abandoned.</p>
     *
     * <p><b>The returned list is in submission order, not admission order.</b> Element {@code i}
     * is the future for {@code jobs.get(i)} however the policy chose to sequence the batch. Tasks
     * abandoned because the calling thread was interrupted come back already cancelled, so the
     * list always has exactly one entry per task.</p>
     *
     * @param jobs                  tasks with size estimates (bytes)
     * @param policy                admission/ordering policy; see {@link AdmissionPolicy}
     * @param clearCompletedFutures if {@code true}, completed futures are pruned from the
     *                              internal list as tasks finish
     * @return one future per task, in the order {@code jobs} listed them
     */
    public <V> List<CompletableFuture<?>> submitThrottled(List<ThrottledTask<V>> jobs, AdmissionPolicy policy, boolean clearCompletedFutures) {

        // Work on a private copy: sorting and draining must not mutate the caller's list.
        List<ThrottledTask<V>> sorted = new ArrayList<>(jobs);

        /*
         * Where each task's future belongs in the answer.
         *
         * Keyed on identity and holding a QUEUE of positions, not a single index, because the
         * same ThrottledTask instance may legitimately appear in the batch more than once — the
         * ledgers charge per claim, so two occurrences are two pieces of work. Each admission
         * takes the next position for that instance, in the order the caller listed them.
         */
        final java.util.IdentityHashMap<ThrottledTask<V>, java.util.ArrayDeque<Integer>> positions =
                new java.util.IdentityHashMap<>(sorted.size());
        for (int i = 0; i < sorted.size(); i++) {
            positions.computeIfAbsent(sorted.get(i), ignored -> new java.util.ArrayDeque<>()).add(i);
        }
        final CompletableFuture<?>[] handed = new CompletableFuture<?>[sorted.size()];

        // Validate job types BEFORE anything is admitted: failing on an unsupported job after
        // a claim would leave budget to unwind. Fail fast with nothing claimed.
        for (ThrottledTask<V> candidate : sorted) {
            if (!isSupportedJob(candidate.job)) {
                throw new IllegalArgumentException("Unsupported job type in " + candidate
                        + " — expected Callable, Runnable or Supplier");
            }
        }

        sorted.sort(policy.selectionOrder());

        /*
         * A deque, because admitFirst removes the admitted candidate by identity, through its iterator, and
         * under any permissive policy that candidate is the FRONT element: O(1) here, versus an
         * identity scan plus array shift over the remaining batch per task on an ArrayList —
         * quadratic in the batch size, and measurable from a few thousand tasks up.
         */
        java.util.ArrayDeque<ThrottledTask<V>> queue = new java.util.ArrayDeque<>(sorted);

        // Computed once: the coordinator thread does not change for the length of a batch.
        final boolean reentrant = holdsThrottleClaim();

        /*
         * How many of THIS batch's tasks are running right now.
         *
         * The re-entrancy exemption is granted only while this is zero. Being re-entrant is what
         * makes the exemption necessary — the thread holding the claim we would block on is this
         * one — but it is not a licence to ignore the policy for the whole batch: passed on
         * every scan, it admits a nested batch of ten thousand tasks in one go, past the memory
         * budget and the concurrency cap alike, which is the OOM this library exists to prevent.
         * One at a time is all deadlock-freedom needs, and keeps the overshoot to the single
         * task the liveness floor already allows.
         */
        final AtomicInteger ownInFlight = new AtomicInteger();

        // One ticket for the whole batch: it is the coordinator's place in the admission queue,
        // and closing it on the way out is what stops an abandoned batch holding others up.
        try (ThrottleHarness.Ticket ticket = throttleHarness.newTicket()) {

            /*
             * When these candidates started queuing. Every task in the batch is offered from
             * this moment, so "now - offeredSinceNanos" at admission is that task's own
             * time-to-admission.
             *
             * The earlier version timed from the coordinator's first REFUSAL and reset to zero
             * after each admission, so only the first task of a burst reported anything: free
             * four slots at once and three of the four tasks that genuinely waited reported
             * zero. It measured the coordinator's idle gap and charged it to one arbitrary task.
             */
            final long offeredSinceNanos = System.nanoTime();

            while (!queue.isEmpty()) {

                final ThrottledTask<V> nextTask = awaitAdmission(queue, policy, ticket, reentrant, ownInFlight,
                        () -> queue.size() + " un-submitted task(s)");

                if (nextTask == null) {
                    // Interrupted. The caller still gets one entry per task: what never ran comes
                    // back cancelled, so element i always describes jobs.get(i).
                    return cancelUnsubmitted(handed);
                }

                // Already counted as started by admitFirst(...) — and already removed from the
                // queue by it — so the accounting is in place before the task can possibly run.
                handed[positions.get(nextTask).poll()] = runAdmitted(nextTask, policy, clearCompletedFutures,
                        System.nanoTime() - offeredSinceNanos, ticket.lastAdmissionByFloor(), ownInFlight);
            }
        }

        return List.of(handed);
    }

    /**
     * One admission: scans and waits until the harness claims a candidate, or the thread is
     * interrupted.
     *
     * <p>The one coordinator loop, shared by the List and the Iterator overloads. It used to be
     * two hand-kept copies, and they had already drifted — one carried the comments explaining
     * why the version is captured before scanning and why the exemption is re-evaluated per
     * scan, the other neither; the re-entrancy gate had to be applied to both. Anything about
     * the poll/wake protocol or the exemption belongs here, once.</p>
     *
     * <p><b>Capture the state version BEFORE scanning.</b> If a task completes between the
     * failed scan and {@code awaitStateChange()}, the version mismatch makes the wait return
     * immediately — no signal can be lost.</p>
     *
     * <p><b>Scan and claim in ONE atomic step</b>, against ONE snapshot for the whole pass.
     * Splitting the check from the claim is not safe here: every executor in a subtree shares
     * this harness, so several coordinators can be scanning at once. If each merely observed
     * the state and then claimed, they would all see {@code runningTasks() == 0}, all take the
     * liveness floor, and all start together — overshooting the budget by the number of
     * coordinators.</p>
     *
     * <p><b>The exemption is re-evaluated per scan</b>, from {@code ownInFlight}: it is
     * granted only while none of this batch's tasks is running, one task at a time.</p>
     *
     * @param candidates  mutable, in policy order; the claimed candidate is removed from it
     * @param policy      the admission policy
     * @param ticket      this coordinator's place in the admission queue, held for the batch
     * @param reentrant   whether this coordinator's thread holds a throttle claim
     * @param ownInFlight this batch's running count — incremented here for the claimed task
     * @param abandoned   describes what is abandoned on interruption, for the log; only
     *                    evaluated then
     * @return the claimed task, counted in {@code ownInFlight}; or {@code null} if the thread
     *         was interrupted while waiting — the flag is restored and the caller must stop
     */
    private <V> ThrottledTask<V> awaitAdmission(java.util.Collection<ThrottledTask<V>> candidates, AdmissionPolicy policy,
            ThrottleHarness.Ticket ticket, boolean reentrant, AtomicInteger ownInFlight,
            java.util.function.Supplier<String> abandoned) {

        while (true) {

            final long observedVersion = throttleHarness.stateVersion();

            final ThrottledTask<V> admitted =
                    throttleHarness.admitFirst(candidates, policy, ticket, reentrant && ownInFlight.get() == 0);

            if (admitted != null) {
                ownInFlight.incrementAndGet();
                return admitted;
            }

            try {
                throttleHarness.awaitStateChange(observedVersion, THROTTLE_POLL_MILLIS, reentrant);
            } catch (InterruptedException e) {
                /*
                 * Restore the interrupt flag and EXIT. Looping again would make the next wait
                 * throw immediately, spinning forever.
                 */
                Thread.currentThread().interrupt();
                LOGGER.error("{} Interrupted while waiting for throttle admission; abandoning {}.",
                        executorId, abandoned.get(), e);
                return null;
            }
        }
    }

    /**
     * Submits a task the harness has already claimed, and arranges for the claim to be released.
     *
     * @param task                  the admitted task — already counted as started by the harness
     * @param policy                the policy that admitted it, to release against
     * @param clearCompletedFutures whether to prune completed futures as tasks finish
     * @return the task's future
     */
    private <V> CompletableFuture<?> runAdmitted(ThrottledTask<V> task, AdmissionPolicy policy,
            boolean clearCompletedFutures, long waitedNanos, boolean floorAdmitted,
            java.util.concurrent.atomic.AtomicInteger ownInFlight) {

        // The ThrottledTask's name and size travel with the submission so listeners see them.
        final CompletableFuture<?> completableFuture =
                submitDescribed(ThrottleHarness.holdingAClaim(toCallable(task.job)), task.job, task.name, task.size, waitedNanos, floorAdmitted);

        /*
         * Attached to the RUN's end, not the future's settlement. cancel() settles the future
         * at once, and on a pooled backend — where the interrupt is a no-op — the body then runs
         * to its natural end. Wired to the future, the release ran while the body was still
         * executing: the harness handed back the claim, a memory budget its bytes, an exclusive
         * holder its lock, and admitted new work beside a body that had not finished. A claim
         * is released when the work it covers has stopped, whichever way it stops.
         */
        runOf(completableFuture).onRunEnd(() -> {
            /*
             * Release this coordinator's own count BEFORE onEnd signals waiters. Registering it
             * as a second whenComplete on the same future left the order to the JDK: if the
             * decrement ran after onEnd, the coordinator this wakes would re-evaluate its
             * exemption as still-taken, be refused, and sleep another full poll interval — per
             * task, for the whole nested batch. It happened to work only because
             * CompletableFuture pops its dependents LIFO, which is an implementation detail.
             */
            ownInFlight.decrementAndGet();

            /*
             * DEBUG, not INFO: this runs once per task. At INFO it emitted one formatted log
             * line per throttled task — 60,000 lines per throughput round — costing about
             * 20% of throttled-path throughput (measured: 490k -> 617k tasks/s on removal).
             * A library must not log per task at INFO regardless. Failures are already
             * logged, once, by recordFailure(); nothing is lost here.
             */
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("{} throttled task {} completed{}", executorId, task.name,
                        completableFuture.isCompletedExceptionally() ? " exceptionally" : "");
            }
            // Frees capacity and signals waiting coordinators unconditionally.
            throttleHarness.onEnd(task, policy);

            if (clearCompletedFutures) {
                clearFuturesListFromCompletedTasks();
            }
        });

        return completableFuture;
    }

    /**
     * Submits a stream of tasks throttled by the default policy, with the default window.
     *
     * @param <V>  the job type
     * @param jobs the tasks, pulled a window at a time
     * @see #submitThrottled(Iterator, AdmissionPolicy, int)
     */
    public <V> void submitThrottled(Iterator<ThrottledTask<V>> jobs) {
        submitThrottled(jobs, AdmissionPolicy.defaultPolicy(backend), DEFAULT_STREAM_WINDOW);
    }

    /**
     * Submits a stream of tasks throttled by {@code policy}, with the default window.
     *
     * @param <V>    the job type
     * @param jobs   the tasks, pulled a window at a time
     * @param policy admission/ordering policy
     * @see #submitThrottled(Iterator, AdmissionPolicy, int)
     */
    public <V> void submitThrottled(Iterator<ThrottledTask<V>> jobs, AdmissionPolicy policy) {
        submitThrottled(jobs, policy, DEFAULT_STREAM_WINDOW);
    }

    /**
     * Submits tasks as they are produced, holding at most {@code window} of them at a time.
     *
     * <p>The {@link List} overload materialises the whole batch: a producer that discovers work
     * incrementally — paging a table, reading a feed — must build every task up front, so a
     * hundred thousand of them sit in memory before the first one runs. Here the coordinator
     * pulls only enough to keep the window full, and memory is bounded by the window rather than
     * by the size of the work.</p>
     *
     * <p>Two things differ from the {@code List} overload, both consequences of not knowing the
     * batch in advance:</p>
     *
     * <ul>
     *   <li><b>Ordering applies within the window only.</b> {@code policy.selectionOrder()} sorts
     *       the candidates currently held, not the whole stream, so a large task far down the
     *       stream cannot be pulled forward past a window of small ones. A window at least as
     *       large as the stream behaves exactly like the {@code List} overload.</li>
     *   <li><b>Validation is per candidate, as it is pulled.</b> The {@code List} overload
     *       rejects an unsupported job type before anything is claimed; a stream cannot promise
     *       that, so an unsupported job halfway down throws after the tasks before it have
     *       already been submitted and possibly run.</li>
     * </ul>
     *
     * <pre>{@code
     * try (Stream<Voyage> voyages = repository.streamAll()) {
     *     executor.submitThrottled(
     *             voyages.map(v -> ThrottledTask.of(() -> reindex(v), 8L << 20, "Reindex")).iterator(),
     *             AdmissionPolicy.defaultPolicy(), 64);
     * }
     * executor.waitForCompletion();
     * }</pre>
     *
     * <p>Returns nothing, unlike the {@code List} overload: a list of futures for an unbounded
     * stream would reintroduce exactly the retention this method exists to avoid. Use
     * {@link #getFutures()} or a listener to observe individual tasks.</p>
     *
     * @param <V>    the job type
     * @param jobs   the tasks, pulled a window at a time; consumed, never re-read
     * @param policy admission/ordering policy
     * @param window how many candidates to hold at once; at least 1
     * @throws IllegalArgumentException if {@code window} is below 1, or a pulled job is not a
     *                                  {@code Callable}, {@code Runnable} or {@code Supplier}
     */
    public <V> void submitThrottled(Iterator<ThrottledTask<V>> jobs, AdmissionPolicy policy, int window) {

        if (window < 1) {
            throw new IllegalArgumentException("window must be at least 1, was " + window);
        }

        /*
         * An ArrayList, not the ArrayDeque the List overload uses. That deque exists because
         * admitFirst removes the admitted candidate by identity through its iterator, and front-removal on a
         * list is O(n) — quadratic over a batch of thousands. Here n is the window, which is
         * small and fixed, and the window has to be re-sorted after every refill, which a deque
         * cannot do in place.
         */
        final List<ThrottledTask<V>> candidates = new ArrayList<>(window);

        /*
         * When each held candidate entered the window. Unlike the List overload these do not all
         * start queuing together — the window refills as tasks are admitted — so the stamp has to
         * be per candidate. Bounded by the window; entries are removed as they are admitted.
         *
         * A deque of stamps per identity, not one stamp, for the same reason the List overload
         * keeps a deque of positions: the same instance may legitimately appear twice in a
         * stream, and with a single stamp the first admission consumed it and the second
         * occurrence reported a wait of zero despite having queued just as long.
         */
        final java.util.IdentityHashMap<ThrottledTask<V>, java.util.ArrayDeque<Long>> offeredSinceNanos =
                new java.util.IdentityHashMap<>(window);

        refill(candidates, jobs, window, policy, offeredSinceNanos);

        final boolean reentrant = holdsThrottleClaim();
        final AtomicInteger ownInFlight = new AtomicInteger();

        try (ThrottleHarness.Ticket ticket = throttleHarness.newTicket()) {

            while (!candidates.isEmpty()) {

                final ThrottledTask<V> admitted = awaitAdmission(candidates, policy, ticket, reentrant, ownInFlight,
                        () -> candidates.size() + " held candidate(s) and the rest of the stream");

                if (admitted == null) {
                    return;     // interrupted; flag restored
                }

                final java.util.ArrayDeque<Long> offeredAt = offeredSinceNanos.get(admitted);
                final Long offeredAtNanos = offeredAt == null ? null : offeredAt.pollFirst();
                if (offeredAt != null && offeredAt.isEmpty()) {
                    offeredSinceNanos.remove(admitted);
                }

                runAdmitted(admitted, policy, false,
                        offeredAtNanos == null ? 0 : System.nanoTime() - offeredAtNanos,
                        ticket.lastAdmissionByFloor(), ownInFlight);

                // Refill only after an admission: the window is full at every other moment, so
                // pulling here is what keeps "at most `window` materialised" true.
                refill(candidates, jobs, window, policy, offeredSinceNanos);
            }
        }
    }

    /**
     * Tops the window up from the stream and re-sorts it if anything arrived.
     *
     * <p>Each candidate is validated as it is pulled — the earliest point at which a stream can
     * be checked at all.</p>
     */
    private static <V> void refill(List<ThrottledTask<V>> candidates, Iterator<ThrottledTask<V>> jobs,
            int window, AdmissionPolicy policy,
            java.util.IdentityHashMap<ThrottledTask<V>, java.util.ArrayDeque<Long>> offeredSinceNanos) {

        boolean added = false;

        while (candidates.size() < window && jobs.hasNext()) {
            final ThrottledTask<V> candidate = jobs.next();
            if (!isSupportedJob(candidate.job)) {
                throw new IllegalArgumentException("Unsupported job type in " + candidate
                        + " — expected Callable, Runnable or Supplier");
            }
            candidates.add(candidate);
            // One stamp per occurrence, oldest first: a repeated instance is two waits.
            offeredSinceNanos.computeIfAbsent(candidate, ignored -> new java.util.ArrayDeque<>(2))
                    .addLast(System.nanoTime());
            added = true;
        }

        if (added && candidates.size() > 1) {
            candidates.sort(policy.selectionOrder());
        }
    }

    /**
     * Fills the gaps left by an interrupted coordinator with already-cancelled futures.
     *
     * <p>Those tasks were never submitted — no descriptor, no listener ever saw them — so there is
     * no run to cancel; a plain cancelled future is the honest answer, and it keeps the returned
     * list positionally aligned with the caller's batch.</p>
     */
    private static List<CompletableFuture<?>> cancelUnsubmitted(CompletableFuture<?>[] handed) {

        for (int i = 0; i < handed.length; i++) {
            if (handed[i] == null) {
                final AbandonedFuture<Void> abandoned = new AbandonedFuture<>();
                abandoned.cancel(false);
                handed[i] = abandoned;
            }
        }
        return List.of(handed);
    }

    /**
     * The placeholder returned for a task an interrupted coordinator never submitted.
     *
     * <p>A distinct type rather than a plain cancelled future, so callers can tell "this was
     * abandoned before it existed" from "somebody cancelled a real task". They are not the same
     * thing and only the first says anything about the submitting thread — see
     * {@link #invokeAllThrottled(List, AdmissionPolicy)}, which used to infer the first from any
     * cancelled entry and set an interrupt flag on a thread nobody had interrupted.</p>
     */
    private static final class AbandonedFuture<T> extends CompletableFuture<T> {
    }

    public CompletableFuture[] submitRunnables(List<? extends Runnable> jobs) {

        CompletableFuture[] completableFutures = new CompletableFuture[jobs.size()];

        for (int i = 0; i < jobs.size(); i++) {
            completableFutures[i] = this.submit(jobs.get(i));
        }

        return completableFutures;
    }

    public <T> CompletableFuture[] submitCallables(List<? extends Callable<T>> jobs) {

        CompletableFuture[] completableFutures = new CompletableFuture[jobs.size()];

        for (int i = 0; i < jobs.size(); i++) {
            completableFutures[i] = this.submit(jobs.get(i));
        }

        return completableFutures;
    }

    // ── Submission ───────────────────────────────────────────────────────────────────────

    /**
     * Submits a {@link Callable}. A checked exception thrown by the body lands in the future
     * as-is (so {@code get()} reports {@code ExecutionException(checked)}); it is not wrapped
     * in a {@code RuntimeException}.
     */
    public <T> CompletableFuture<T> submit(Callable<T> job) {
        // The user's own callable is what gets tracked — no synthetic wrapper.
        return submitDescribed(job, job, null, TaskDescriptor.UNKNOWN_SIZE);
    }

    public <T> CompletableFuture<T> submit(Supplier<T> supplier) {
        return submitDescribed(supplier::get, supplier, null, TaskDescriptor.UNKNOWN_SIZE);
    }

    public CompletableFuture<Void> submit(Runnable runnable) {
        return submitDescribed(() -> {
            runnable.run();
            return null;
        }, runnable, null, TaskDescriptor.UNKNOWN_SIZE);
    }

    /** Whether {@link #toCallable} can adapt {@code job}. */
    private static boolean isSupportedJob(Object job) {
        return job instanceof Callable<?> || job instanceof Runnable || job instanceof Supplier<?>;
    }

    /** Adapts any supported job object to the internal body type. */
    @SuppressWarnings("unchecked")
    private static <T> Callable<T> toCallable(Object job) {
        if (job instanceof Callable<?> callable) {
            return (Callable<T>) callable;
        }
        if (job instanceof Runnable runnable) {
            return () -> {
                runnable.run();
                return null;
            };
        }
        if (job instanceof Supplier<?> supplier) {
            return () -> (T) supplier.get();
        }
        throw new IllegalArgumentException("Unsupported job type: " + job);
    }

    /**
     * The single submission path every overload funnels through.
     *
     * <p><strong>submit() never throws.</strong> Both ways a submission can be refused —
     * this executor being closed, or the shared pool having been shut down — are reported
     * through the returned future and to listeners as {@link TaskStatus#REJECTED}.</p>
     *
     * @param body the task, as a {@link Callable}
     * @param job  the original job object, kept for diagnostics in {@link FuturePair}
     * @param name the {@code ThrottledTask} name for throttled tasks, else {@code null}
     * @param size the {@code ThrottledTask} size estimate, else {@link TaskDescriptor#UNKNOWN_SIZE}
     */
    /** The run behind a future this executor handed out. Every path returns the run's own future. */
    private static TaskRun<?> runOf(CompletableFuture<?> future) {
        return ((TaskFuture<?>) future).run;
    }

    private <T> CompletableFuture<T> submitDescribed(Callable<T> body, Object job, String name, long size) {
        return submitDescribed(body, job, name, size, NOT_THROTTLED, false);
    }

    /**
     * As {@link #submitDescribed(Callable, Object, String, long)}, carrying what the throttle did
     * to this task so {@code onThrottled} can report it.
     *
     * @param waitedNanos   nanoseconds spent waiting for admission, or {@link #NOT_THROTTLED}
     * @param floorAdmitted whether the liveness floor overrode a refusing policy
     */
    private <T> CompletableFuture<T> submitDescribed(Callable<T> body, Object job, String name, long size,
            long waitedNanos, boolean floorAdmitted) {

        final TaskRun<T> run = newRun(body, job, name, size, waitedNanos, floorAdmitted);

        if (isClosed()) {
            futures.remove(run.pair);
            // A fresh exception per rejection, so the stack trace points at the rejected call site.
            run.reject(new IllegalStateException("Executor pool is closed"));
            return run.future;
        }

        try {
            backend.pool().execute(run::execute);
        } catch (RejectedExecutionException poolShutDown) {
            futures.remove(run.pair);
            run.reject(poolShutDown);
            return run.future;
        }

        /*
         * Compact on SUBMIT, not on wait.
         *
         * futures retains every completed future and, through FuturePair.job, the task
         * lambda and anything it captured — so a long-lived executor accumulates every task
         * it has ever run. Compacting when a NEW batch arrives bounds that, while leaving a
         * batch you have just waited on fully inspectable via getFutures(), which is a
         * documented and tested usage.
         */
        if (run.descriptor.sequence() % FUTURES_COMPACTION_INTERVAL == 0) {
            clearFuturesListFromCompletedTasks();
        }

        return run.future;
    }

    /**
     * Whether this submission is being made from inside a throttled task — one whose harness
     * claim only this thread can release.
     *
     * <p>Not the ambient executor binding, which is set for <em>every</em> task on every backend
     * and so said only "inside some task": that included plain {@code submit()} bodies holding
     * no claim at all, and each of them was then let past the policy — two hundred of them
     * submitting one 700-byte task apiece against a budget that fits one ran all two hundred at
     * once. Nothing they held was in the ledger, so there was no deadlock to exempt them from.</p>
     *
     * <p>Necessary, not sufficient: the harness also requires that nothing else in the policy's
     * ledger could release on its own (see {@code ThrottleHarness.parkedClaims}), and the
     * caller bounds it to one task at a time through {@code ownInFlight}. Deliberately not
     * narrowed to the same harness: under the shipped {@code defaultPolicy(...)} the ledger that
     * blocks admission is process-wide, so a task in one root holding bytes there and submitting
     * into a second root would otherwise wait forever on its own claim.</p>
     */
    private static boolean holdsThrottleClaim() {
        return ThrottleHarness.claimsHeldByCurrentThread() > 0;
    }


    /**
     * Prepares a task without scheduling it: builds the descriptor, runs every listener's
     * {@code onSubmit}, creates the future and registers the pair in {@link #futures}.
     *
     * <p>Registration happens here, BEFORE the task is handed to the pool, so a
     * {@code waitForCompletion()} racing the submission cannot miss a task that has already
     * started running. Callers that then fail to schedule must {@code futures.remove(run.pair)}.</p>
     *
     * <p>Package-private so the completion state machine can be driven deterministically
     * from tests, on the calling thread, without a pool in the way.</p>
     */
    <T> TaskRun<T> newRun(Callable<T> body, Object job, String name, long size) {
        return newRun(body, job, name, size, NOT_THROTTLED, false);
    }

    /**
     * As {@link #newRun(Callable, Object, String, long)}, recording what the throttle did.
     *
     * @param waitedNanos   nanoseconds spent waiting for admission, or {@link #NOT_THROTTLED}
     * @param floorAdmitted whether the liveness floor overrode a refusing policy
     */
    <T> TaskRun<T> newRun(Callable<T> body, Object job, String name, long size,
            long waitedNanos, boolean floorAdmitted) {

        final TaskDescriptor descriptor = new TaskDescriptor(
                this, submissionCounter.incrementAndGet(), name, size,
                System.nanoTime(), System.currentTimeMillis());

        final List<TaskExecutionListener<?>> listeners = effectiveListeners();
        final Object[] states = new Object[listeners.size()];
        for (int i = 0; i < states.length; i++) {
            states[i] = guardedOnSubmit(listeners.get(i), descriptor);
        }

        final TaskRun<T> run = new TaskRun<>(this, descriptor, body, listeners, states);
        run.throttleWaitNanos = waitedNanos;
        run.throttleFloorAdmitted = floorAdmitted;
        run.pair = new FuturePair(run.future, job, descriptor);
        futures.add(run.pair);
        return run;
    }

    // ── Per-task completion state machine ────────────────────────────────────────────────

    /**
     * One submitted task: its body, descriptor, listener snapshot, and the state machine that
     * arbitrates between the worker and a canceller.
     *
     * <pre>
     *   SUBMITTED ──execute()──▶ STARTED ──body returns──▶ DONE
     *       │                       │
     *    cancel()                cancel()
     *       ▼                       ▼
     *     DONE                CANCEL_REQUESTED ──body returns──▶ (onComplete, future untouched)
     * </pre>
     *
     * <p>A single CAS decides every transition. There is deliberately no
     * {@code isDone()}-then-flag sequence: a cancel landing between those two steps would
     * fire {@code onComplete} on the canceller before the worker fired {@code onStart}, and
     * the worker would then install listener thread-state it never restores.</p>
     *
     * <p>Completion order on every path is <b>record → onComplete (reverse) → settle the
     * future</b>, so a task's scope is closed before any waiter or dependent observes its
     * future as complete, and "future done ⇒ failure recorded" holds without a separate
     * settled stage.</p>
     */
    static final class TaskRun<T> {

        private static final int SUBMITTED = 0;
        private static final int STARTED = 1;
        private static final int CANCEL_REQUESTED = 2;
        private static final int DONE = 3;

        private final ThrottledExecutor owner;
        final TaskDescriptor descriptor;
        private final Callable<T> body;
        final TaskFuture<T> future;
        FuturePair pair;

        /** Snapshot taken at onSubmit; nulled after onComplete so per-task state is not retained until compaction. */
        private List<TaskExecutionListener<?>> listeners;
        private Object[] states;

        private final AtomicInteger state = new AtomicInteger(SUBMITTED);
        private volatile long startedAtNanos = TaskOutcome.NEVER_STARTED;

        /**
         * Whether the cancellation that moved this run to {@code CANCEL_REQUESTED} asked for an
         * interrupt. The state alone cannot say: {@code cancel(false)} makes the same transition,
         * and the post-fan-out re-check that reads the state was interrupting for it too.
         */
        private volatile boolean interruptWanted;

        /**
         * Runs when this run has ENDED — the body has returned, or it never will. Guarded by
         * {@code this}. Distinct from the future settling: a cancellation settles the future
         * while a body on a pooled worker keeps running, and what the executor attaches here (the
         * harness release) has to wait for the body.
         */
        private Runnable onRunEnd;
        private boolean ended;

        /**
         * Registers the run-end hook, or runs it at once if the run already ended — a rejection
         * ends the run inside {@code submitDescribed}, before the caller gets to attach anything.
         */
        void onRunEnd(Runnable hook) {
            final boolean runNow;
            synchronized (this) {
                runNow = ended;
                if (!ended) {
                    onRunEnd = hook;
                }
            }
            if (runNow) {
                hook.run();
            }
        }

        /** Every terminal path calls this exactly once; the state CAS that got it there is unique. */
        private void runEnded() {
            final Runnable hook;
            synchronized (this) {
                ended = true;
                hook = onRunEnd;
                onRunEnd = null;
            }
            if (hook != null) {
                hook.run();
            }
        }

        /**
         * The thread running the body, while one is. Guarded by {@link #runnerLock}.
         *
         * <p>Only read to interrupt it, and only when the thread is the task's own — see
         * {@link #interruptIfInterruptible()}. Cleared when the body returns so a cancellation
         * arriving late cannot interrupt whatever that thread does next.</p>
         */
        private Thread runner;

        /**
         * Makes "deliver the interrupt" and "the body has finished" mutually exclusive.
         *
         * <p>A volatile field is not enough. The canceller reads {@code runner} and then calls
         * {@code interrupt()}; with only volatile reads it can read a live thread an instant
         * before the body returns and deliver the interrupt after it, landing inside
         * {@code fireOnComplete} — that is, inside user {@code onComplete} listener code, which
         * is exactly what moving the self-interrupt after the {@code onStart} fan-out was meant
         * to avoid. Holding this monitor across both the read-and-interrupt and the clear closes
         * the window: once the body has cleared it, no interrupt can still be in flight.</p>
         */
        private final Object runnerLock = new Object();

        /**
         * What the throttle did to this task, or {@link #NOT_THROTTLED} for a plain submission.
         *
         * <p>Carried from the coordinator rather than measured here: the wait happens before the
         * descriptor exists, so nothing on this side of the submission could observe it.</p>
         */
        private long throttleWaitNanos = NOT_THROTTLED;
        private boolean throttleFloorAdmitted;

        TaskRun(ThrottledExecutor owner, TaskDescriptor descriptor, Callable<T> body,
                List<TaskExecutionListener<?>> listeners, Object[] states) {
            this.owner = owner;
            this.descriptor = descriptor;
            this.body = body;
            this.listeners = listeners;
            this.states = states;
            this.future = new TaskFuture<>(this);
        }

        /** @return the future handed to the submitter */
        TaskFuture<T> future() {
            return future;
        }

        /** Runs the body on the current (worker) thread. What the pool executes. */
        void execute() {

            if (!state.compareAndSet(SUBMITTED, STARTED)) {
                return;   // cancelled while queued: the body never runs, the canceller already completed us
            }

            startedAtNanos = System.nanoTime();

            /*
             * Publish the running thread BEFORE the listener fan-out, not just before the body.
             *
             * A cancellation landing in between wins the STARTED -> CANCEL_REQUESTED CAS and
             * calls interruptIfInterruptible(); if `runner` were still null it would find
             * nothing to interrupt and the body would go on to run to its natural end,
             * contradicting what cancelSubtree() and CANCELLED_AFTER_START promise. That window
             * is not narrow — it spans every registered listener's onStart, which is arbitrary
             * user code (JFR, micrometer and the adaptive sizing all hook it).
             *
             * Publishing early leaves the mirror-image race: a cancellation that read `runner`
             * as null just before this assignment. The state re-check further down closes it —
             * by then the canceller has already CAS'd, so this thread sees CANCEL_REQUESTED and
             * interrupts itself.
             */
            synchronized (runnerLock) {
                runner = Thread.currentThread();
            }

            final List<TaskExecutionListener<?>> snapshot = listeners;
            final Object[] snapshotStates = states;

            // Before onStart: the throttle decision precedes the body, and a listener that
            // installs thread state in onStart should already know why it waited.
            if (throttleWaitNanos != NOT_THROTTLED) {
                for (int i = 0; i < snapshot.size(); i++) {
                    guardedOnThrottled(snapshot.get(i), descriptor, throttleWaitNanos, throttleFloorAdmitted);
                }
            }

            for (int i = 0; i < snapshot.size(); i++) {
                guardedOnStart(snapshot.get(i), descriptor, snapshotStates[i]);
            }

            /*
             * The self-interrupt for that race goes HERE — after the fan-out, immediately before
             * the body — not before onStart. Listener code is user code (JFR, micrometer and the
             * adaptive sizing all hook onStart); an interruptible call in it would throw
             * InterruptedException, be swallowed by the guard, and consume the flag along with
             * that listener's per-task state. Setting it now gives the same "the body's first
             * blocking call throws" behaviour without putting listeners in the way.
             */
            if (interruptWanted) {
                // Not `state == CANCEL_REQUESTED`: cancel(false) reaches that state as well, and
                // honouring its promise means this re-check must know which cancel it was.
                interruptIfInterruptible();
            }

            T value = null;
            Throwable failure = null;
            try {
                value = body.call();
            } catch (Throwable t) {
                failure = t;
            } finally {
                /*
                 * Clear before settling, so a cancellation racing the end of the body cannot
                 * interrupt this thread after it has moved on to something else. Under the
                 * monitor, so a canceller that has already read `runner` finishes delivering
                 * before this returns rather than landing in fireOnComplete. The interrupt flag
                 * may still be set from an interrupt that did land; that belongs to the body,
                 * and on the virtual backend the thread dies with the task anyway.
                 */
                synchronized (runnerLock) {
                    runner = null;
                }
            }

            try {
                if (state.compareAndSet(STARTED, DONE)) {
                    if (failure != null) {
                        owner.recordFailure(failure);
                        fireOnComplete(new TaskOutcome(TaskStatus.FAILED, failure, startedAtNanos, System.nanoTime()));
                        future.settleExceptionally(failure);
                    } else {
                        fireOnComplete(new TaskOutcome(TaskStatus.COMPLETED, null, startedAtNanos, System.nanoTime()));
                        future.settle(value);
                    }
                } else {
                    // CANCEL_REQUESTED: the canceller already recorded and settled the future.
                    // The worker only reports that the body has now actually finished.
                    fireOnComplete(new TaskOutcome(TaskStatus.CANCELLED_AFTER_START,
                            new CancellationException("cancelled while running"), startedAtNanos, System.nanoTime()));
                }
            } catch (Throwable fatal) {
                // Only a VirtualMachineError escapes the guards. Nothing may wait on a dead task.
                future.settleExceptionally(fatal);
                throw fatal;
            } finally {
                runEnded();
            }
        }

        /**
         * Called by {@link TaskFuture#cancel}. Returns {@code true} if this call performed the
         * cancellation (the future transitions to cancelled), {@code false} if the task was
         * already done or already cancelled.
         *
         * @param mayInterruptIfRunning the caller's answer to {@link java.util.concurrent.Future#cancel},
         *                              honoured rather than assumed: {@code false} means the body
         *                              keeps running to its own end. The future is cancelled
         *                              either way — the flag governs the interrupt, not the
         *                              cancellation
         */
        boolean cancelRequested(boolean mayInterruptIfRunning) {

            if (state.compareAndSet(SUBMITTED, DONE)) {
                final CancellationException cause = new CancellationException("cancelled before start");
                owner.recordFailure(cause);
                fireOnComplete(new TaskOutcome(TaskStatus.CANCELLED_BEFORE_START, cause,
                        TaskOutcome.NEVER_STARTED, System.nanoTime()));
                future.settleCancelled();
                runEnded();     // the body never runs, so this is the end of the run
                return true;
            }

            if (state.compareAndSet(STARTED, CANCEL_REQUESTED)) {
                /*
                 * Record NOW, not when the body eventually returns: a waiter that joins the
                 * (cancelled) future must find the cancellation already in the collected
                 * throwables, or it leaks into the next batch. onComplete is the worker's,
                 * when the body actually ends — listener thread-state lives on that thread.
                 */
                owner.recordFailure(new CancellationException("cancelled while running"));
                future.settleCancelled();

                if (mayInterruptIfRunning) {
                    interruptWanted = true;         // published before the interrupt, for the re-check
                    interruptIfInterruptible();
                }

                return true;
            }

            return false;
        }

        /**
         * Interrupts the thread running the body — but only where that thread is the task's own.
         *
         * <p>On {@link ExecutorBackend#VIRTUAL_THREAD_PER_TASK} it is: the thread was created for
         * this task and dies with it, so setting its interrupt flag affects nothing else, and a
         * body blocked on IO or a latch actually stops.</p>
         *
         * <p>On the pooled backends it is not. A ForkJoin worker outlives the task and goes on to
         * run other people's work, so an interrupt set here would surface inside an unrelated
         * task later — the classic leaked-interrupt bug. Clearing the flag after the body
         * returned would not fix it either, since the body may return long after the interrupt
         * was delivered. So those backends keep the documented behaviour: cancellation stops
         * queued work from starting, and lets running work finish.</p>
         *
         * <p>{@link ExecutorBackend#CALLER_RUNS} is excluded for the same reason and one more:
         * the thread being interrupted would be the canceller's own.</p>
         */
        private void interruptIfInterruptible() {

            if (!owner.backend.threadDiesWithTheTask()) {
                return;
            }

            /*
             * Read and interrupt under the monitor. Self-interrupt is legitimate here and is the
             * point of the state re-check in execute(): the body has not started yet, so setting
             * the flag now means the very first blocking call it makes throws
             * InterruptedException, which is what a cancellation that arrived a moment earlier
             * should have done.
             */
            synchronized (runnerLock) {

                if (runner == null) {
                    return;
                }

                runner.interrupt();
            }
        }

        /** Rejected at submission (closed executor or shut-down pool): never scheduled. */
        void reject(Throwable cause) {
            if (state.compareAndSet(SUBMITTED, DONE)) {
                fireOnComplete(new TaskOutcome(TaskStatus.REJECTED, cause, TaskOutcome.NEVER_STARTED, System.nanoTime()));
                future.settleExceptionally(cause);
                runEnded();
            }
        }

        /** Reverse order — LIFO scope semantics — then drop the snapshot. */
        private void fireOnComplete(TaskOutcome outcome) {
            final List<TaskExecutionListener<?>> snapshot = listeners;
            final Object[] snapshotStates = states;
            if (snapshot != null) {
                for (int i = snapshot.size() - 1; i >= 0; i--) {
                    guardedOnComplete(snapshot.get(i), descriptor, snapshotStates[i], outcome);
                }
            }
            listeners = null;
            states = null;
        }
    }

    /**
     * The future handed back from {@code submit}. Its {@link #cancel} goes through the
     * {@link TaskRun} state machine instead of completing the future directly, so the worker
     * and the canceller can never disagree about the outcome. Only the run settles it.
     *
     * <p>Externally completing this future by other means ({@code complete},
     * {@code obtrude*}, {@code orTimeout}, ...) bypasses the state machine and is unsupported;
     * {@code cancel()} is the one supported external transition.</p>
     */
    static final class TaskFuture<T> extends CompletableFuture<T> {

        private final TaskRun<T> run;

        TaskFuture(TaskRun<T> run) {
            this.run = run;
        }

        /**
         * {@inheritDoc}
         *
         * <p>{@code mayInterruptIfRunning} means what {@link java.util.concurrent.Future#cancel}
         * says it means: {@code false} allows an in-progress body to complete. It used to be
         * accepted and dropped, which was harmless only while nothing was ever interrupted —
         * once cancellation learned to interrupt, {@code cancel(false)} started interrupting
         * too, and a caller who chose {@code false} precisely so a mid-write or mid-statement
         * body could close cleanly got a {@code ClosedByInterruptException} instead.</p>
         *
         * <p>The flag governs only the interrupt. Either way the future is cancelled, the
         * cancellation is recorded, and the body is reported {@code CANCELLED_AFTER_START} when
         * it ends. {@link #cancelSubtree()} passes {@code true}.</p>
         */
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return run.cancelRequested(mayInterruptIfRunning) || isCancelled();
        }

        void settle(T value) {
            if (!super.complete(value)) {
                LOGGER.warn("{} was completed externally before the task settled it; outcome may be inconsistent.",
                        run.descriptor);
            }
        }

        void settleExceptionally(Throwable t) {
            if (!super.completeExceptionally(t)) {
                LOGGER.warn("{} was completed externally before the task settled it; outcome may be inconsistent.",
                        run.descriptor);
            }
        }

        void settleCancelled() {
            super.cancel(false);
        }
    }

    // ── Listener dispatch, each call guarded individually ────────────────────────────────

    /*
     * A listener can never fail the task, and one misbehaving listener must not suppress the
     * others — so each invocation is wrapped on its own. VirtualMachineError is not swallowed:
     * OOM or SOE inside a hook is not something to log and carry on from.
     */

    @SuppressWarnings("unchecked")
    private static Object guardedOnSubmit(TaskExecutionListener<?> listener, TaskDescriptor task) {
        try {
            return ((TaskExecutionListener<Object>) listener).onSubmit(task);
        } catch (Throwable t) {
            rethrowIfFatal(t);
            LOGGER.error("Listener {} threw from onSubmit for {} — ignored.", listener.getClass().getName(), task, t);
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private static void guardedOnStart(TaskExecutionListener<?> listener, TaskDescriptor task, Object state) {
        try {
            ((TaskExecutionListener<Object>) listener).onStart(task, state);
        } catch (Throwable t) {
            rethrowIfFatal(t);
            LOGGER.error("Listener {} threw from onStart for {} — ignored.", listener.getClass().getName(), task, t);
        }
    }

    private static void guardedOnThrottled(TaskExecutionListener<?> listener, TaskDescriptor task,
            long waitedNanos, boolean floorAdmitted) {
        try {
            listener.onThrottled(task, waitedNanos, floorAdmitted);
        } catch (Throwable t) {
            rethrowIfFatal(t);
            LOGGER.error("Listener {} threw from onThrottled for {} — ignored.", listener.getClass().getName(), task, t);
        }
    }

    @SuppressWarnings("unchecked")
    private static void guardedOnComplete(TaskExecutionListener<?> listener, TaskDescriptor task, Object state, TaskOutcome outcome) {
        try {
            ((TaskExecutionListener<Object>) listener).onComplete(task, state, outcome);
        } catch (Throwable t) {
            rethrowIfFatal(t);
            LOGGER.error("Listener {} threw from onComplete for {} ({}) — ignored.",
                    listener.getClass().getName(), task, outcome.status(), t);
        }
    }

    private static void rethrowIfFatal(Throwable t) {
        if (t instanceof VirtualMachineError vmError) {
            throw vmError;
        }
    }

    // ── Failure recording (hardwired — the aggregate exception is core semantics) ────────

    /**
     * Records a task failure for {@code waitForCompletion()} to aggregate.
     *
     * <p>Not a listener on purpose: {@code waitForCompletion}'s contract depends on it, so it
     * must not be removable. Runs BEFORE the future settles on every path.</p>
     */
    private void recordFailure(Throwable failure) {
        collectedThrowables.add(wrapThrowable(failure));
        LOGGER.error("[{}] {} task failed: [{}]", Thread.currentThread().getName(), executorId,
                RootCauseExceptionUtil.getRootCause(failure));
    }

    /**
     * Wraps a task failure, without double-wrapping one that already came from a nested
     * executor (a child's {@code waitForCompletion()} throws one of these on the parent's
     * task thread).
     */
    private ThreadContextAwareCompletionException wrapThrowable(Throwable e) {
        if (e instanceof ThreadContextAwareCompletionException alreadyWrapped) {
            return alreadyWrapped;
        }
        return new ThreadContextAwareCompletionException(executorId, e);
    }

    // Concurrent: waitForCompletion() may be called from any thread; this set exists to detect exactly that.
    Set<String> setOfThreadNames = ConcurrentHashMap.newKeySet();

    /**
     * Waits for this executor's batch AND everything its tasks spawned, then reports any
     * failure from anywhere in the subtree.
     *
     * <p>Waiting only on this executor's own futures was not enough: a task is free to
     * create a child executor and submit to it without waiting, in which case the parent
     * would report the batch finished while that work was still running.</p>
     *
     * <p>Implemented as a fixpoint loop rather than a single pass, because a child can be
     * created <em>during</em> the wait — and a child's task may in turn submit back to an
     * ancestor. Each pass waits on the whole subtree and then checks whether anything new
     * appeared; when a full pass adds nothing, the subtree is quiescent. Termination
     * therefore assumes task spawning is well-founded, and the loop is bounded regardless.</p>
     *
     * @return {@code true} when the subtree completed without failures
     * @throws ThreadContextAwareCompletionException bundling every failure in the subtree
     * @see #waitForOwnCompletion()
     */
    public boolean waitForCompletion() throws RuntimeException {

        warnIfWaitingFromSeveralThreads();

        final List<Throwable> subtreeFailures = new ArrayList<>();

        int pass = 0;
        while (awaitSubtreeOnce(subtreeFailures)) {

            if (++pass >= MAX_QUIESCENCE_PASSES) {
                LOGGER.warn("{} waitForCompletion() gave up after {} passes — tasks are still "
                        + "spawning new work. Check for unbounded task recursion.", executorId, pass);
                break;
            }
        }

        if (!subtreeFailures.isEmpty()) {
            throw aggregate(subtreeFailures);
        }

        return true;
    }

    /**
     * Waits for the whole subtree like {@link #waitForCompletion()}, but gives up after
     * {@code timeout} instead of blocking indefinitely.
     *
     * <p>A timeout is <b>not</b> an error: nothing is cancelled, the tasks keep running, and the
     * result names the ones that were still unsettled. Only failures throw — if a task has
     * already failed when the budget expires, the aggregate is raised exactly as the untimed wait
     * would raise it, because a deadline should not swallow a real failure.</p>
     *
     * <p>A zero or negative budget is a single non-blocking check.</p>
     *
     * <pre>{@code
     * WaitResult result = executor.waitForCompletion(Duration.ofMinutes(2));
     *
     * if (!result.complete()) {
     *     LOGGER.warn("{} still running", result.outstanding().size());
     *     executor.cancelSubtree();
     * }
     * }</pre>
     *
     * @param timeout how long to wait in total, across every pass over the subtree
     * @return whether the subtree settled, and what was left if it did not
     * @throws ThreadContextAwareCompletionException bundling every failure in the subtree
     * @see #waitForCompletion()
     */
    public WaitResult waitForCompletion(Duration timeout) {

        java.util.Objects.requireNonNull(timeout, "timeout");

        warnIfWaitingFromSeveralThreads();

        // A single deadline for the WHOLE wait, not per pass: otherwise a subtree that keeps
        // spawning work could renew the budget on every quiescence pass and never return.
        final long deadlineNanos = deadlineFor(timeout);
        final List<Throwable> subtreeFailures = new ArrayList<>();
        final boolean[] expired = {false};

        int pass = 0;
        boolean gaveUp = false;

        while (awaitSubtreeOnce(subtreeFailures, deadlineNanos, expired)) {

            if (expired[0]) {
                break;
            }
            if (++pass >= MAX_QUIESCENCE_PASSES) {
                LOGGER.warn("{} waitForCompletion(Duration) gave up after {} passes — tasks are still "
                        + "spawning new work. Check for unbounded task recursion.", executorId, pass);
                /*
                 * Giving up is not completing. Reporting complete=true here told the caller the
                 * subtree had settled and handed back an empty outstanding() list, so the
                 * "if (!result.complete()) cancelSubtree()" branch this method's own javadoc
                 * recommends never fired, and the runaway work was neither cancelled nor logged.
                 */
                gaveUp = true;
                break;
            }
        }

        if (!subtreeFailures.isEmpty()) {
            throw aggregate(subtreeFailures);
        }

        if (!expired[0] && !gaveUp) {
            return new WaitResult(true, List.of());
        }

        /*
         * The scan decides, not the flag. `expired` is latched during the pass, and the last
         * tasks can settle between the deadline firing and this scan — reporting
         * WaitResult(false, []) then contradicts both WaitResult's javadoc ("empty when
         * complete") and this method's own example, which logs the outstanding count and
         * cancels a subtree that had in fact finished cleanly. Nothing outstanding IS complete,
         * whatever the clock did.
         */
        final List<TaskDescriptor> outstanding = outstandingInSubtree();

        return outstanding.isEmpty() ? new WaitResult(true, List.of()) : new WaitResult(false, outstanding);
    }

    /**
     * Records the waiting thread and warns if more than one has ever waited on this executor.
     *
     * <p>Two threads waiting on one subtree is not made safe by either wait being bounded, so
     * both {@code waitForCompletion} overloads go through here.</p>
     */
    private void warnIfWaitingFromSeveralThreads() {

        setOfThreadNames.add(Thread.currentThread().getName());

        if (setOfThreadNames.size() > 1) {
            LOGGER.warn("{} waitForCompletion() has been called from more than one thread. It is not "
                    + "intended to be called on multiple threads. Review your logic and proceed with "
                    + "caution; you may put this system into deadlock.", executorId);
            LOGGER.warn("{} Threads which have called waitForCompletion(): {}", executorId, setOfThreadNames);
        }

        LOGGER.debug("{} Waiting for completion on thread: [{}]", executorId, Thread.currentThread().getName());
    }

    /**
     * A {@link System#nanoTime()} deadline {@code timeout} from now, saturating rather than
     * throwing or wrapping.
     *
     * <p>{@code Duration.toNanos()} overflows past about 292 years — which
     * {@code ChronoUnit.FOREVER.getDuration()} and a "practically no limit"
     * {@code Duration.ofDays(400_000)} both exceed — and even below that the addition can wrap
     * to a negative deadline, which would make every pass take the already-expired branch and
     * report a timeout immediately. Both are silly inputs meaning "wait indefinitely", and both
     * now do that.</p>
     *
     * <p>Overflow is not the same thing as "too large", which is what treating every
     * {@code ArithmeticException} as a saturating budget assumed. {@code toNanos()} overflows on
     * large NEGATIVE durations too — {@code Duration.ofDays(-400_000)}, the exact mirror of the
     * case above — and reading that as "wait indefinitely" skipped the clamp and parked the
     * caller for ~292 years, contradicting this method's own contract that a zero or negative
     * budget is a single non-blocking check. The sign decides which end it saturates to.</p>
     *
     * <p>{@link System#nanoTime()} is explicitly allowed to be negative, and is on some
     * platforms, so the overflow test cannot be {@code timeoutNanos > Long.MAX_VALUE - now}:
     * that subtraction overflows for a negative {@code now}, saturating every finite timeout and
     * turning a two-minute wait into a single non-blocking check. {@link Math#addExact} is the
     * arithmetic that actually knows when it has overflowed.</p>
     */
    /**
     * A deadline already in the past, however the clock is behaving.
     *
     * <p>Not {@code System.nanoTime()}, which would be a deadline the very next call could find
     * itself on the right side of, and not {@link Long#MIN_VALUE}, which is {@link #NO_DEADLINE}
     * and means the opposite.</p>
     */
    private static final long NO_TIME_LEFT = Long.MIN_VALUE + 1;

    /**
     * How long is left of a deadline, saturating instead of wrapping.
     *
     * <p>Package-private and taking the clock reading as an argument so it can be driven
     * directly: {@link System#nanoTime()} is permitted to be negative — and is, on some
     * platforms — but a test cannot arrange for that.</p>
     *
     * <p>{@code deadlineNanos - nowNanos} is the obvious spelling and the wrong one. With the
     * saturated {@code Long.MAX_VALUE} deadline that {@link #deadlineFor} hands back for "wait
     * indefinitely", and a negative {@code now}, that subtraction overflows to a negative value:
     * the caller reads it as an expired budget, does one non-blocking check and reports a
     * timeout — which is precisely the failure the saturation upstream exists to prevent,
     * reintroduced one line further down.</p>
     *
     * @param deadlineNanos the {@link System#nanoTime()} value to stop at
     * @param nowNanos      the current {@link System#nanoTime()} reading
     * @return the nanoseconds remaining, saturating at {@link Long#MAX_VALUE}; zero or negative
     *         when the budget is spent
     */
    static long remainingNanos(long deadlineNanos, long nowNanos) {

        try {
            return Math.subtractExact(deadlineNanos, nowNanos);
        } catch (ArithmeticException wrapped) {
            // The sign of the deadline says which end it ran off: a far-future deadline stays a
            // long wait, a far-past one stays expired.
            return deadlineNanos > 0 ? Long.MAX_VALUE : NO_TIME_LEFT;
        }
    }

    private static long deadlineFor(Duration timeout) {

        if (timeout.isNegative()) {
            // Checked before toNanos(), which cannot represent this one either way.
            return NO_TIME_LEFT;
        }

        final long timeoutNanos;

        try {
            timeoutNanos = Math.max(0L, timeout.toNanos());
        } catch (ArithmeticException tooLargeToCount) {
            return Long.MAX_VALUE;
        }

        try {
            return Math.addExact(System.nanoTime(), timeoutNanos);
        } catch (ArithmeticException pastTheEndOfTheClock) {
            return Long.MAX_VALUE;
        }
    }

    /** Descriptors of every task in this subtree whose future has not settled. */
    private List<TaskDescriptor> outstandingInSubtree() {

        final List<TaskDescriptor> outstanding = new ArrayList<>();

        for (FuturePair pair : futures) {
            if (!pair.getCompletableFuture().isDone()) {
                outstanding.add(pair.getDescriptor());
            }
        }

        for (ThrottledExecutor child : liveChildren()) {
            outstanding.addAll(child.outstandingInSubtree());
        }

        return outstanding;
    }

    /**
     * One pass over the subtree: wait on this executor, then on every live child.
     *
     * @param failures accumulator for failures found anywhere in the subtree
     * @return {@code true} if new work appeared while waiting, so another pass is needed
     */
    private boolean awaitSubtreeOnce(List<Throwable> failures) {
        return awaitSubtreeOnce(failures, NO_DEADLINE, new boolean[1]);
    }

    /**
     * One pass over the subtree, bounded by {@code deadlineNanos}.
     *
     * @param failures      accumulator for failures found anywhere in the subtree
     * @param deadlineNanos {@link System#nanoTime()} value to stop at, or {@link #NO_DEADLINE}
     * @param expired       one-element flag, set when the deadline passed with work unsettled
     * @return {@code true} if new work appeared while waiting, so another pass is needed
     */
    private boolean awaitSubtreeOnce(List<Throwable> failures, long deadlineNanos, boolean[] expired) {

        final long submissionsBefore = submissionCounter.get();
        final long childrenBefore = childCounter.get();

        failures.addAll(awaitOwnFutures(deadlineNanos, expired));

        for (ThrottledExecutor child : liveChildren()) {
            /*
             * Descend even once the clock has run out. An expired pass does not block — it is a
             * single non-blocking check — but it still DRAINS that child's collected failures,
             * and draining is not optional: a failure left in a child's buffer is neither
             * reported by this wait (which would then claim a clean timeout) nor cleared, so it
             * surfaces later as part of somebody else's batch. That is the same leak
             * cancelRequested() records failures early to avoid, and it would contradict this
             * method's own promise that failures are raised even when the budget expires.
             */
            child.awaitSubtreeOnce(failures, deadlineNanos, expired);
        }

        return submissionCounter.get() != submissionsBefore || childCounter.get() != childrenBefore;
    }

    /** Builds the aggregate, stamped with this executor's position in the tree. */
    private ThreadContextAwareCompletionException aggregate(List<Throwable> failures) {

        LOGGER.error("{} List Of Exceptions size: {}", executorId, failures.size());

        final ThreadContextAwareCompletionException aggregateException =
                new ThreadContextAwareCompletionException(this.executorId, failures.toArray(new Throwable[0]));

        aggregateException.executorPath(executorPath());

        return aggregateException;
    }

    /**
     * Waits for this executor's own futures only, without descending into children and
     * without throwing.
     *
     * @return the failures this executor collected, drained
     */
    private List<Throwable> awaitOwnFutures() {
        return awaitOwnFutures(NO_DEADLINE, new boolean[1]);
    }

    /**
     * Waits for this executor's own futures, optionally bounded.
     *
     * @param deadlineNanos {@link System#nanoTime()} value to stop at, or {@link #NO_DEADLINE}
     * @param expired       one-element flag, set when the deadline passed with work unsettled
     * @return the failures this executor collected, drained
     */
    private List<Throwable> awaitOwnFutures(long deadlineNanos, boolean[] expired) {

        final CompletableFuture<?> allOf = CompletableFuture.allOf(futures.stream()
                .map(FuturePair::getCompletableFuture)
                .toArray(CompletableFuture[]::new));

        try {
            if (deadlineNanos == NO_DEADLINE) {
                allOf.join();
            } else {
                final long remainingNanos = remainingNanos(deadlineNanos, System.nanoTime());
                if (remainingNanos <= 0) {
                    // Budget already spent: one non-blocking check, no wait at all.
                    expired[0] |= !allOf.isDone();
                } else {
                    allOf.get(remainingNanos, TimeUnit.NANOSECONDS);
                }
            }
        } catch (TimeoutException timedOut) {
            expired[0] = true;
        } catch (InterruptedException interrupted) {
            // Someone wants this thread to stop waiting; restore the flag and report the work
            // as unfinished rather than pretending the batch completed.
            Thread.currentThread().interrupt();
            expired[0] = true;
        } catch (CompletionException | CancellationException | ExecutionException ignore) {
            LOGGER.debug("{} Batch completed exceptionally; reported by the caller.", executorId);
        }

        final List<Throwable> collected = drainCollectedThrowables();

        if (collected.isEmpty() && allOf.isCompletedExceptionally()) {
            /*
             * Exceptional with nothing recorded: the failure came from a stage the handler
             * did not see (a cancellation, for instance). Surface it rather than reporting
             * a clean batch.
             */
            collected.add(allOf.handle((ignored, throwable) -> throwable).join());
        }

        return collected;
    }

    /**
     * Waits for this executor's own batch only, ignoring any children.
     *
     * <p>The pre-tree behaviour of {@link #waitForCompletion()}, kept for callers that
     * deliberately want to join their own work without being held up by fire-and-forget
     * children.</p>
     */
    public boolean waitForOwnCompletion() throws RuntimeException {

        final List<Throwable> failures = awaitOwnFutures();

        if (!failures.isEmpty()) {
            throw aggregate(failures);
        }

        return true;
    }


    // volatile: close() may be called from a different thread than the submitters checking isClosed().
    private volatile boolean isClosed = false;

    public boolean isClosed() {
        return isClosed;
    }

    /**
     * Closes this executor to further submissions and detaches it from its parent.
     *
     * <p>Deregistering here gives deterministic cleanup of the parent's child registry,
     * rather than leaving it to wait for the collector.</p>
     */
    public void close() {

        isClosed = true;

        if (parent != null) {
            parent.deregisterChild(this);
        }
    }

    // ── ExecutorService implementation (shallow shutdown — shared pool is left running) ──

    @Override
    public void execute(Runnable command) {
        submit(command);
    }

    @Override
    public void shutdown() {
        close();
    }

    /**
     * Closes this executor and cancels every pending task in the whole subtree.
     *
     * <p>{@link java.util.concurrent.ExecutorService#shutdownNow()} promises to stop
     * executing tasks. Cancelling only this executor's own futures while work its children
     * are running carries on would not honour that.</p>
     */
    @Override
    public List<Runnable> shutdownNow() {
        cancelSubtree();
        return List.of();
    }

    /**
     * Closes and cancels this executor and, depth-first, every live descendant.
     *
     * <p>Cancellation reliably prevents queued work from starting. Whether it stops work
     * <em>in flight</em> depends on the backend: on {@link ExecutorBackend#VIRTUAL_THREAD_PER_TASK}
     * the running thread is interrupted, because it belongs to that task alone; on the pooled
     * backends the body runs to its natural end, because a worker outlives the task and an
     * interrupt set on it would surface inside unrelated work.</p>
     */
    public void cancelSubtree() {

        for (ThrottledExecutor child : liveChildren()) {
            child.cancelSubtree();
        }

        close();
        cancelAllRunningFutures();
    }

    @Override
    public boolean isShutdown() {
        return isClosed();
    }

    @Override
    public boolean isTerminated() {
        return isClosed() && futures.stream()
            .allMatch(fp -> fp.getCompletableFuture().isDone());
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
        if (futures.isEmpty()) return true;
        CompletableFuture<Void> allOf = CompletableFuture.allOf(
            futures.stream()
                .map(FuturePair::getCompletableFuture)
                .toArray(CompletableFuture[]::new)
        );
        try {
            allOf.get(timeout, unit);
        } catch (TimeoutException e) {
            return false;
        } catch (ExecutionException e) {
            // tasks completed exceptionally — counts as done
        }
        return isTerminated();
    }

    private AtomicBoolean isCancelRemainingRunningTasksAlreadyCalled = new AtomicBoolean(false);


    public synchronized void cancelRemainingRunningTasks() {

        if (isCancelRemainingRunningTasksAlreadyCalled.get()) {
            return;
        }

        isCancelRemainingRunningTasksAlreadyCalled.set(true);

        TimerTask task = new TimerTask() {
            @Override
            public void run() {


                LOGGER.info(LogMarkers.POOL_SHUTDOWN, "{} Cancel Remaining Running Tasks: {}", executorId, futures.size());
                    // Add your task logic here
                    cancelAllRunningFutures();

            }
        };

        cancellationExecutor().submit(task);
    }

    public void clearFuturesListFromCompletedTasks() {

        futures.removeIf(new Predicate<FuturePair>() {
            @Override
            public boolean test(FuturePair futurePair) {
                // Settled, not raw: dropping a future before its failure is recorded
                // would lose that failure from the batch report.
                return futurePair.getCompletableFuture().isDone();
            }
        });
    }

    public void cancelAllRunningFutures() {

        for (FuturePair pair : futures) {

            CompletableFuture completableFuture = pair.getCompletableFuture();

            // cancel() on an already-done future is a no-op, so pending is the only state worth acting on.
            if (!completableFuture.isDone()) {
                completableFuture.cancel(true);
            }
        }
    }

}
