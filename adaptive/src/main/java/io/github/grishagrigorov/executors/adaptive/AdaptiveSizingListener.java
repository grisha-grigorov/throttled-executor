package io.github.grishagrigorov.executors.adaptive;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.time.InstantSource;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The measuring half: for every <em>named</em> task that completes normally, records what
 * its body allocated on the worker thread and — when nothing else ran meanwhile — how far
 * heap-in-use rose, and hands the {@link Sample} to the {@link ProfileStore}. Obtained from
 * {@link AdaptiveSizing#listener()}; attach the one instance to every executor root that
 * should learn.
 *
 * <h2>Two measurements</h2>
 * <ul>
 *   <li><b>Allocation</b> — the worker thread's allocated-bytes counter at start and end.
 *       Exact and attributable, {@code -1} on virtual-thread workers and non-HotSpot JVMs.</li>
 *   <li><b>Solo heap peak</b> — the largest rise of heap-in-use above the lowest reading
 *       seen during the run ({@link HeapRise}; a sampler thread reads every
 *       {@link AdaptiveConfig#heapSamplePeriod()}; with {@link java.time.Duration#ZERO}
 *       only the start and end readings exist). Attributable only when the task was
 *       <em>solo</em>: nothing was in flight when it started and nothing started before it
 *       ended. Unnamed plain submissions count as "something".</li>
 * </ul>
 * Every sample is stamped with the facade's clock at completion and with
 * {@link AdaptiveConfig#version()}, which is what lets the sizing stop trusting it later.
 * A run that measured neither records nothing; statuses other than {@code COMPLETED} record
 * nothing. Nothing here ever throws into the task: every reading and the store call are
 * guarded.
 */
public final class AdaptiveSizingListener implements TaskExecutionListener<AdaptiveSizingListener.Run>, AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveSizingListener.class);

    /** Per-task state between {@code onStart} and {@code onComplete}; only named tasks get one. */
    public static final class Run {
        long allocAtStart = -1L;
        long heapAtStart = -1L;
        boolean solo;
        long startSeq;
        long session = HeapSampler.NO_SESSION;   // the sampler session this run opened, if any

        Run() {
        }
    }

    private final ProfileStore store;
    private final LongSupplier allocatedBytes;
    private final LongSupplier heapUsed;
    private final HeapSampler sampler;                  // null when the period is ZERO
    private final InstantSource clock;
    private final String version;                       // stamped on every sample; null = unversioned
    private final Set<String> warnedUnlearned = ConcurrentHashMap.newKeySet();

    /**
     * Two counters in one word: the number of bodies in flight (low 32 bits) and the number
     * of starts ever (high 32 bits). One atomic update per start keeps them consistent —
     * as two separate atomics, a pair of simultaneous starts could be ordered one way by the
     * in-flight count and the other way by the start sequence, leaving the first task "solo"
     * with a start sequence that already included the second: at its end it saw no later
     * start and stored a heap peak that measured both. The start count wraps at 2³²
     * harmlessly (it is only ever compared for equality); the in-flight count is bounded by
     * concurrency, so it never carries into the high word.
     */
    private final AtomicLong state = new AtomicLong();

    private static final long ONE_START = (1L << 32) + 1L;

    private static int inFlightOf(long state) {
        return (int) state;
    }

    private static long startsOf(long state) {
        return state >>> 32;
    }

    AdaptiveSizingListener(ProfileStore store, AdaptiveConfig config, LongSupplier allocatedBytes, LongSupplier heapUsed,
            InstantSource clock) {
        this.store = Objects.requireNonNull(store, "store");
        Objects.requireNonNull(config, "config");
        this.allocatedBytes = Objects.requireNonNull(allocatedBytes, "allocatedBytes");
        this.heapUsed = Objects.requireNonNull(heapUsed, "heapUsed");
        this.clock = Objects.requireNonNull(clock, "clock");
        this.version = config.version();
        this.sampler = config.heapSamplePeriod().isZero() ? null : new HeapSampler(heapUsed, config.heapSamplePeriod());
    }

    /** Unnamed submissions get no state — but they still count as in flight (see {@link #onStart}). */
    @Override
    public Run onSubmit(TaskDescriptor task) {
        return task.name() == null ? null : new Run();
    }

    /** On the worker thread. Counters first, so overlap detection errs on "not solo". */
    @Override
    public void onStart(TaskDescriptor task, Run run) {
        final long before = state.getAndAdd(ONE_START);
        if (run == null) {
            return;
        }
        run.solo = inFlightOf(before) == 0;
        run.startSeq = startsOf(before) + 1;
        run.allocAtStart = read(allocatedBytes);
        run.heapAtStart = run.solo ? read(heapUsed) : -1L;    // a heap reading is only ever used for a solo run
        if (run.solo && sampler != null && run.heapAtStart >= 0) {
            run.session = sampler.begin(run.heapAtStart);
        }
    }

    @Override
    public void onComplete(TaskDescriptor task, Run run, TaskOutcome outcome) {
        try {
            if (!outcome.bodyRan()) {
                return;                                 // never started: the counter was never bumped
            }
            final long allocNow = run == null ? -1L : read(allocatedBytes);
            final long heapNow = run != null && run.solo ? read(heapUsed) : -1L;
            /*
             * End the sampler session BEFORE leaving the in-flight count. The other order
             * opened a window in which a task starting right now found nothing in flight,
             * declared itself solo and began a session of its own — which this run's end()
             * then consumed: both runs stored a heap peak, neither of them real.
             */
            long sampledRise = -1L;
            if (run != null && run.solo && sampler != null && run.heapAtStart >= 0) {
                sampledRise = sampler.end(run.session, heapNow);     // always end what was begun
            }
            final long startsNow = startsOf(state.getAndDecrement());
            if (run == null) {
                return;
            }
            if (outcome.status() != TaskStatus.COMPLETED) {
                return;
            }
            final long alloc = run.allocAtStart >= 0 && allocNow >= 0 ? Math.max(0L, allocNow - run.allocAtStart) : -1L;
            final boolean soloStill = run.solo && startsNow == run.startSeq;
            long heapPeak = -1L;
            if (soloStill && run.heapAtStart >= 0) {
                if (sampledRise >= 0) {
                    heapPeak = sampledRise;
                } else if (heapNow >= 0) {
                    final HeapRise inline = new HeapRise(run.heapAtStart);
                    inline.observe(heapNow);
                    heapPeak = inline.value();
                }
            }
            // Any negative declared size means "none" to the sizing; Sample only knows -1.
            final long declared = Math.max(-1L, task.sizeEstimate());
            /*
             * The duration comes free — TaskOutcome has carried it all along, and after
             * bodyRan() it is never negative — so every completed run records a sample. It is
             * recorded on its own terms, not behind a memory gate: behind one, a run whose
             * memory could not be measured recorded nothing, which on the virtual backend is
             * every run but the solo quarantine one, so DURATION ordering ranked on a single
             * cold-start measurement for ever. A duration-only sample teaches the sizing
             * nothing (the estimators read memory fields only, so the task stays unknown until
             * a memory sample lands) and never evicts the ring's last memory measurement — see
             * TaskProfile.append — which is what makes recording it safe.
             */
            store.record(task.name(), new Sample(
                    declared, alloc, heapPeak, clock.millis(), version, outcome.durationNanos()));

            if (alloc < 0 && heapPeak < 0 && declared < 0
                    && !hasMemoryEvidence(store.find(task.name())) && warnedUnlearned.add(task.name())) {
                /*
                 * An unknown's quarantine exists so that its first run is measurable. This run
                 * was not: no allocation counter on this worker (virtual thread) and something
                 * else was in flight — a plain submit() on this root, or work on a root that
                 * does not carry this listener — so nothing was learned and the policy will
                 * quarantine it again. Say so once per name; silently repeating the serialised
                 * run on every batch is the failure mode this guards against.
                 */
                LOG.warn("executors-adaptive: {} ran with nothing to measure (no per-thread allocation counter, and not alone) "
                        + "— nothing learned; it stays unknown and quarantined until a run is solo", task.displayName());
            }
        } catch (RuntimeException e) {
            LOG.warn("executors-adaptive: could not record a sample for {} — ignored", task.displayName(), e);
        }
    }

    /** @return whether any sample in the profile measured memory — what makes a task known */
    private static boolean hasMemoryEvidence(TaskProfile profile) {
        // absolute() reads the constructor-cached maxima and falls back across both memory
        // measurements, so it is empty exactly when no sample measured memory: the one
        // definition of "known", not a second copy of it.
        return profile != null && profile.absolute(Estimator.ALLOCATION).isPresent();
    }

    /** @return whether the "ran with nothing to measure" warning was issued for {@code name} */
    boolean warnedUnlearned(String name) {
        return warnedUnlearned.contains(name);
    }

    /** @return tasks whose body has started and not yet ended, as this listener counts them */
    int inFlight() {
        return inFlightOf(state.get());
    }

    /** Stops the heap sampler thread, if one was started. Tasks are still measured inline afterwards. */
    @Override
    public void close() {
        if (sampler != null) {
            sampler.close();
        }
    }

    private static long read(LongSupplier supplier) {
        try {
            return supplier.getAsLong();
        } catch (RuntimeException e) {
            return -1L;
        }
    }
}
