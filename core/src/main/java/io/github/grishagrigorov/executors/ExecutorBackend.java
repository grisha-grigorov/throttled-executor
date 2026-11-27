package io.github.grishagrigorov.executors;

import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Selects which thread pool a {@link ThrottledExecutor} submits its tasks to.
 *
 * <p>Two production backends, solving opposite problems, and one for tests:</p>
 *
 * <ul>
 *   <li>{@link #FORK_JOIN} — a fixed, core-sized work-stealing pool. Exists to <i>stop</i>
 *       you running more compute-bound work than the machine has cores for. The default,
 *       so behaviour is unchanged unless a caller asks otherwise.</li>
 *   <li>{@link #VIRTUAL_THREAD_PER_TASK} — one virtual thread per task. Exists to
 *       <i>let</i> you run many blocking tasks at once, because a parked virtual thread
 *       costs a continuation rather than an OS thread. Appropriate for IO-bound batches
 *       (socket round-trips, database calls, file work).</li>
 *   <li>{@link #CALLER_RUNS} — no pool at all; bodies run on the submitting thread. Exists to
 *       make code that <i>uses</i> the executor testable without latches or polling.</li>
 * </ul>
 *
 * <p><strong>Choosing.</strong> Misclassification is not symmetric. Blocking work left on
 * {@code FORK_JOIN} parks one of only a handful of workers, and the ForkJoinPool cannot
 * see plain socket blocking so it never compensates for it — a few such tasks can stall
 * the pool. Compute work placed on the virtual backend merely loses work-stealing, since
 * the carrier pool is already core-sized. When unsure, the virtual backend is the
 * cheaper mistake. Note also that virtual threads are <em>not</em> preemptive: a
 * CPU-bound task holds its carrier until it finishes, so the virtual backend is not a
 * remedy for long computations.</p>
 *
 * <p><strong>Java version.</strong> This module compiles against the Java 17 API, so
 * there is deliberately no compile-time reference to virtual threads anywhere here — the
 * executor is obtained by reflection and the backend degrades to {@link #FORK_JOIN} on
 * older runtimes. Compilation is pinned by {@code options.release = 17} in the build, so
 * an accidental direct call would fail to compile rather than fail on a consumer's JVM.</p>
 *
 * @see ThrottledExecutor#withBackend(String, ExecutorBackend)
 */
public enum ExecutorBackend {

    /** Fixed, core-sized ForkJoinPool. The default. */
    FORK_JOIN,

    /** One virtual thread per task, where the runtime supports it. */
    VIRTUAL_THREAD_PER_TASK,

    /**
     * No pool at all: every task body runs synchronously on the thread that submitted it.
     *
     * <p>For tests of code that <em>uses</em> the executor. Everything except the dispatch is
     * unchanged — the completion state machine, the listener fan-out, failure aggregation and
     * the throttle all still run — but a batch is finished by the time {@code submitThrottled}
     * returns, so assertions need no latches, no {@code Awaitility}, and no ordering guesses.</p>
     *
     * <p>Two consequences worth knowing. Concurrency is 1, so the harness only ever sees zero or
     * one task in flight and a policy cannot be exercised at its limits here. And because no
     * other thread can complete anything, a policy that refuses a lone candidate would leave the
     * coordinator polling forever — it is the harness liveness floor (nothing running ⇒ admit
     * the next candidate regardless) that turns that into serial execution instead of a
     * livelock.</p>
     */
    CALLER_RUNS;

    private static final Logger LOGGER = LoggerFactory.getLogger(ExecutorBackend.class);

    /** System property selecting the default backend: {@code forkJoin}, {@code virtual} or {@code callerRuns}. */
    public static final String BACKEND_PROPERTY = "io.github.grishagrigorov.executors.backend";

    /** System property overriding {@link #defaultMaxConcurrency()} for the virtual backend. */
    public static final String VIRTUAL_CONCURRENCY_PROPERTY = "io.github.grishagrigorov.executors.virtualConcurrency";

    /**
     * Default in-flight cap for the virtual backend.
     *
     * <p>Deliberately a flat number rather than a multiple of {@code availableProcessors()}:
     * when work is blocking, the binding constraint is the resource on the other end —
     * sockets, file handles, database connections — and that has nothing to do with how
     * many cores this machine has.</p>
     */
    static final int DEFAULT_VIRTUAL_CONCURRENCY = 256;

    private static final String EXECUTOR_THREAD_PREFIX = "executors-worker-thread";

    /*
     * Creation flags live on the enum, NOT inside the holders, so they can be read without
     * triggering the holder's class initialisation. That is what lets shutdown skip a pool
     * that was never used instead of allocating one just to close it.
     */
    private static final AtomicBoolean FORK_JOIN_CREATED = new AtomicBoolean(false);
    private static final AtomicBoolean VIRTUAL_CREATED = new AtomicBoolean(false);

    /** Lazy holder: the ForkJoinPool is built on first use, not on class load. */
    private static final class ForkJoinHolder {

        static final ForkJoinPool POOL = create();

        private static ForkJoinPool create() {

            final int poolSize = forkJoinPoolSize();

            final ForkJoinPool pool = new ForkJoinPool(
                    poolSize,
                    forkJoinPool -> {
                        final ForkJoinWorkerThread worker =
                                ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(forkJoinPool);
                        // getId(), not threadId(): Thread.threadId() is Java 19, the floor is 17.
                        worker.setName(EXECUTOR_THREAD_PREFIX + worker.getId());
                        return worker;
                    },
                    (thread, throwable) ->
                            LOGGER.error("UncaughtExceptionHandler for thread: {}", thread.getName(), throwable),
                    /* asyncMode */ true,
                    /* corePoolSize */ poolSize,
                    /* maximumPoolSize */ Integer.MAX_VALUE,
                    /* minimumRunnable */ poolSize,
                    /* saturate */ null,
                    /* keepAliveTime */ 1, TimeUnit.MINUTES);

            FORK_JOIN_CREATED.set(true);
            return pool;
        }
    }

    /**
     * Lazy holder: obtains a virtual-thread executor on first use, or {@code null} when
     * the runtime cannot provide one.
     *
     * <p>How it is obtained is {@link VirtualThreadSupport}'s concern — the core jar's
     * multi-release seam: a reflective probe in the base (Java 17) variant, a direct
     * compile-checked call in the {@code META-INF/versions/21} variant that 21+ JVMs
     * select automatically when running from the jar.</p>
     */
    private static final class VirtualHolder {

        /** {@code null} when the runtime predates Java 21. */
        static final ExecutorService POOL = create();

        private static ExecutorService create() {
            final ExecutorService pool = VirtualThreadSupport.createOrNull();
            if (pool != null) {
                VIRTUAL_CREATED.set(true);
            }
            return pool;
        }
    }

    private static int forkJoinPoolSize() {

        final int available = Runtime.getRuntime().availableProcessors();
        final int reduced = available - 2;

        return reduced <= 2 ? available : reduced;
    }

    /**
     * The backend used when a caller does not name one, from the
     * {@value #BACKEND_PROPERTY} system property.
     *
     * @return {@link #FORK_JOIN} unless the property explicitly selects otherwise
     */
    public static ExecutorBackend defaultBackend() {

        final String configured = System.getProperty(BACKEND_PROPERTY);

        if (configured == null || configured.isBlank()) {
            return FORK_JOIN;
        }

        final String normalised = configured.trim().toLowerCase();

        if (normalised.equals("virtual") || normalised.equals("virtual_thread_per_task")) {
            return VIRTUAL_THREAD_PER_TASK;
        }

        if (normalised.equals("forkjoin") || normalised.equals("fork_join")) {
            return FORK_JOIN;
        }

        if (normalised.equals("callerruns") || normalised.equals("caller_runs")) {
            return CALLER_RUNS;
        }

        LOGGER.warn("Unrecognised {}={} — using {}.", BACKEND_PROPERTY, configured, FORK_JOIN);
        return FORK_JOIN;
    }

    /**
     * @return whether this backend can actually be provided by the running JVM. Always
     *         {@code true} for {@link #FORK_JOIN}; {@code true} for the virtual backend
     *         only on Java 21+
     */
    public boolean isAvailable() {

        switch (this) {
            case FORK_JOIN:
            case CALLER_RUNS:
                return true;
            default:
                return VirtualHolder.POOL != null;
        }
    }

    /**
     * @return whether this backend's pool has actually been instantiated in this JVM.
     *         Reading this never causes instantiation.
     */
    public boolean isCreated() {

        switch (this) {
            case FORK_JOIN:
                return FORK_JOIN_CREATED.get();
            case VIRTUAL_THREAD_PER_TASK:
                return VIRTUAL_CREATED.get();
            default:
                // CALLER_RUNS holds no pool, so there is nothing that could have been created.
                return false;
        }
    }

    /**
     * The in-flight task cap a default policy should apply for this backend.
     *
     * <p>Under {@link #FORK_JOIN} the pool size is the real bound and this figure is
     * largely decorative. Under the virtual backend nothing else bounds concurrency, so
     * this becomes the only limit that exists — which is why it is sized by expected
     * downstream capacity rather than by core count.</p>
     */
    public int defaultMaxConcurrency() {

        switch (this) {
            case FORK_JOIN:
                return Runtime.getRuntime().availableProcessors();
            case CALLER_RUNS:
                // One thread runs everything, so anything above 1 would be a fiction.
                return 1;
            default:
                return Integer.getInteger(VIRTUAL_CONCURRENCY_PROPERTY, DEFAULT_VIRTUAL_CONCURRENCY);
        }
    }

    /**
     * Whether a task submitted to this backend gets a thread of its own that dies with it.
     *
     * <p>The question cancellation has to ask before interrupting a running body. Interrupting
     * is only ever safe when the thread belongs to the task: a pooled worker outlives it, so a
     * flag set late — or set at all, if the body has already moved on — surfaces inside an
     * unrelated task later.</p>
     *
     * <p>Deliberately phrased as "what {@link #pool()} will actually return", not "what this
     * enum constant is called", and deliberately sitting next to it so the two cannot drift.
     * That distinction is the whole point: {@code pool()} falls back to the shared ForkJoinPool
     * when virtual threads are missing, while the executor's declared backend still reads
     * {@code VIRTUAL_THREAD_PER_TASK}. Gating on the declared value meant that on the Java 17
     * floor this library advertises — where the fallback is not an edge case but the only
     * behaviour — cancelling a running task interrupted a shared worker. Pinned on a real 17
     * JVM by {@code Java17JarSmokeTests}.</p>
     *
     * @return {@code true} only for the virtual backend on a runtime that actually provides it
     */
    boolean threadDiesWithTheTask() {
        return this == VIRTUAL_THREAD_PER_TASK && VirtualHolder.POOL != null;
    }

    /**
     * The pool tasks are submitted to, falling back to {@link #FORK_JOIN} when the virtual
     * backend is unavailable so work still runs on an older runtime.
     */
    ExecutorService pool() {

        if (this == CALLER_RUNS) {
            // A real ExecutorService, so the single dispatch site in ThrottledExecutor and
            // everything downstream of it — the state machine, the listeners, rejection
            // handling — stay exactly as they are for the pooled backends.
            return CallerRunsHolder.POOL;
        }

        if (this == VIRTUAL_THREAD_PER_TASK) {

            final ExecutorService virtual = VirtualHolder.POOL;

            if (virtual != null) {
                return virtual;
            }
        }

        return ForkJoinHolder.POOL;
    }

    /**
     * The caller-runs "pool": a stateless {@link ExecutorService} that runs each command on the
     * calling thread.
     *
     * <p>Deliberately outside {@link #shutdownAll()} and {@link #allPoolsShutDown()}. Those exist
     * to release <em>threads</em>, and this holds none — no pool, no queue, nothing that could
     * keep a JVM alive or need draining. Wiring it in would also mean touching this holder from
     * shutdown, forcing the class initialisation the created-flags exist to avoid.</p>
     *
     * <p>The visible consequence, which is intended: after
     * {@link ThrottledExecutor#shutdownThreadExecutors()} a {@code CALLER_RUNS} executor
     * keeps working, where the pooled backends start rejecting. Closing the executor itself is
     * what stops a caller-runs executor — {@code shutdown()} here only flips a flag so this
     * remains a well-behaved {@code ExecutorService}.</p>
     */
    private static final class CallerRunsHolder {

        static final ExecutorService POOL = new AbstractExecutorService() {

            private volatile boolean shutdown;

            @Override
            public void execute(Runnable command) {
                if (shutdown) {
                    throw new RejectedExecutionException("caller-runs executor is shut down");
                }
                command.run();
            }

            @Override
            public void shutdown() {
                shutdown = true;
            }

            @Override
            public List<Runnable> shutdownNow() {
                shutdown = true;
                // Nothing can be queued: every task ran on the thread that submitted it.
                return List.of();
            }

            @Override
            public boolean isShutdown() {
                return shutdown;
            }

            @Override
            public boolean isTerminated() {
                // Terminated the moment it is shut down: no task can still be running that this
                // executor started, because whoever started one is the thread asking.
                return shutdown;
            }

            @Override
            public boolean awaitTermination(long timeout, TimeUnit unit) {
                return shutdown;
            }
        };
    }

    /**
     * Shuts down every pool that has actually been created, leaving untouched any that
     * was never used.
     */
    static void shutdownAll() {

        if (FORK_JOIN_CREATED.get()) {
            shutdownQuietly(ForkJoinHolder.POOL, "ForkJoinPool");
        }

        if (VIRTUAL_CREATED.get()) {
            shutdownQuietly(VirtualHolder.POOL, "virtual-thread executor");
        }
    }

    /** @return true when no pool is outstanding — either never created, or shut down */
    static boolean allPoolsShutDown() {

        final boolean forkJoinDone = !FORK_JOIN_CREATED.get() || ForkJoinHolder.POOL.isShutdown();
        final boolean virtualDone = !VIRTUAL_CREATED.get() || VirtualHolder.POOL.isShutdown();

        return forkJoinDone && virtualDone;
    }

    static void shutdownQuietly(ExecutorService pool, String description) {

        if (pool == null) {
            return;
        }

        try {
            final List<Runnable> discarded = pool.shutdownNow();

            if (!discarded.isEmpty()) {
                LOGGER.info(LogMarkers.POOL_SHUTDOWN, "Shutting down {}. Discarded task count: {}",
                        description, discarded.size());
            }

        } catch (Exception e) {
            LOGGER.error(LogMarkers.POOL_SHUTDOWN, "Failed shutting down {}: {}", description, e.getMessage(), e);
        }
    }
}
