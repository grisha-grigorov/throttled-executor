package io.github.grishagrigorov.executors.throttling;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryType;
import java.lang.management.MemoryUsage;
import java.time.Duration;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.List;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refuses admission while the heap is actually full, regardless of what the batch declared.
 *
 * <p>Every other policy here is <em>predictive</em>: {@link MemoryBudgetPolicy} and
 * {@link GlobalMemoryBudgetPolicy} sum the sizes callers declared, and {@code executors-adaptive}
 * learns those sizes from measurement. All of them are blind to memory the model does not know
 * about — a task that under-declared, a task submitted with no estimate at all, and everything in
 * the JVM that is not a task. This one looks at the heap itself, so it catches what the others
 * cannot, and it needs no declarations to work.</p>
 *
 * <h2>The signal</h2>
 *
 * <p>The sum of {@link MemoryPoolMXBean#getCollectionUsage()} over the heap pools, divided by
 * {@link Runtime#maxMemory()}. Collection usage is the occupancy <em>after</em> the last
 * collection of each pool, so it measures what is actually retained rather than what happens to
 * be uncollected garbage at the moment of asking — a plain {@code getUsage()} reading would
 * refuse admission on a heap that is merely due a GC.</p>
 *
 * <p>Plain {@code java.lang.management} only: no {@code com.sun.management}, no GC notification
 * listeners, no matching on pool names. Every heap pool on the collectors this library is
 * exercised against (generational Shenandoah, plain Shenandoah, G1, ZGC) reports collection
 * usage, so nothing here is collector-specific. Before the first collection the reading is zero,
 * which reads as "no pressure" — correct, since nothing has filled up yet.</p>
 *
 * <h2>Hysteresis</h2>
 *
 * <p>One threshold would chatter: admit a task, push the heap back over the line, refuse, watch
 * it drop, admit again. So there are two. Above {@code high} the policy starts refusing, and it
 * keeps refusing until occupancy falls below {@code low}.</p>
 *
 * <h2>Cost</h2>
 *
 * <p>{@code admit} runs on the coordinator thread, possibly many times per task, so the reading
 * is memoised for {@code samplePeriod} (50&nbsp;ms by default). {@link Duration#ZERO} reads on
 * every call. There is no background thread and nothing to shut down.</p>
 *
 * <h2>Liveness</h2>
 *
 * <p>{@link #inFlightTaskCount()} is deliberately left at {@code -1}, so the harness liveness
 * floor keeps reading the real in-flight count. Under sustained pressure with nothing running,
 * the floor admits one task anyway: the batch degrades to serial execution rather than
 * deadlocking on a heap that only that batch can drain.</p>
 *
 * <pre>{@code
 * AdmissionPolicy policy = CompositePolicy.allOf(
 *         GlobalMemoryBudgetPolicy.processWideHeapBudget(),   // what the batch says it needs
 *         HeapPressurePolicy.defaults(),                      // what the heap says it has
 *         new MaxConcurrencyPolicy(backend.defaultMaxConcurrency()));
 *
 * executor.submitThrottled(batch, policy);
 * }</pre>
 *
 * <p>Not part of {@link AdmissionPolicy#defaultPolicy(io.github.grishagrigorov.executors.ExecutorBackend)}:
 * a reactive brake changes the behaviour of every existing caller, and what the default should be
 * is a decision to make with field data rather than in advance.</p>
 */
public final class HeapPressurePolicy implements AdmissionPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(HeapPressurePolicy.class);

    /** Default high-water mark: refuse above 85% post-collection occupancy. */
    public static final double DEFAULT_HIGH = 0.85;

    /** How far below {@code high} the low-water mark sits when it is not given explicitly. */
    public static final double DEFAULT_HYSTERESIS = 0.10;

    /** Default memo lifetime for a heap reading. */
    public static final Duration DEFAULT_SAMPLE_PERIOD = Duration.ofMillis(50);

    private final double high;
    private final double low;
    private final long samplePeriodMillis;
    private final HeapReader reader;
    private final InstantSource clock;

    /** Everything mutable is guarded by this — {@code admit} is called from many threads. */
    private final Object stateLock = new Object();

    private double lastPressure = -1;
    private long lastReadMillis;
    /** Separate from a sentinel timestamp: {@code now - Long.MIN_VALUE} overflows negative. */
    private boolean everRead;
    private boolean underPressure;

    /**
     * @param high         refuse above this fraction of max heap, in {@code (0, 1]}
     * @param low          resume below this fraction; must be at most {@code high}
     * @param samplePeriod how long a reading is reused for; {@link Duration#ZERO} to read every
     *                     time
     * @param reader       where the occupancy figure comes from
     * @param clock        time source; inject a fake for deterministic tests
     * @throws IllegalArgumentException if the thresholds are outside {@code (0, 1]} or inverted
     */
    public HeapPressurePolicy(double high, double low, Duration samplePeriod, HeapReader reader, InstantSource clock) {

        if (!(high > 0) || high > 1) {
            throw new IllegalArgumentException("high must be in (0, 1], was " + high);
        }
        if (!(low > 0) || low > high) {
            throw new IllegalArgumentException("low must be in (0, high], was " + low + " with high " + high);
        }
        if (samplePeriod.isNegative()) {
            throw new IllegalArgumentException("samplePeriod must not be negative, was " + samplePeriod);
        }

        this.high = high;
        this.low = low;
        this.samplePeriodMillis = samplePeriod.toMillis();
        this.reader = java.util.Objects.requireNonNull(reader, "reader");
        this.clock = java.util.Objects.requireNonNull(clock, "clock");
    }

    /**
     * A policy on the real heap with the default sample period and the usual hysteresis.
     *
     * @param high refuse above this fraction of max heap; resumes 10 points below
     * @return the policy
     */
    public static HeapPressurePolicy of(double high) {
        return new HeapPressurePolicy(high, lowFor(high),
                DEFAULT_SAMPLE_PERIOD, platformReader(), InstantSource.system());
    }

    /**
     * The resume threshold for a given refuse threshold.
     *
     * <p>Ten points below, except where that would leave a low mark occupancy cannot actually
     * fall under. A low mark near zero is a latch, not a hysteresis band: post-collection
     * occupancy on a live JVM never drops to one percent of max heap, so the first time the
     * policy refuses is the last time it admits, and every later batch runs serially for the
     * life of the JVM. The old guard only caught {@code high <= 0.10} — where the flat band
     * would go to or below zero — and {@code of(0.11)} sailed through to {@code low = 0.01},
     * which is that latch by another route. The band is proportional (half of {@code high})
     * until a flat ten points first leaves at least that much room, which the two rules agree
     * on at {@code high = 0.20}.</p>
     *
     * @param high the refuse threshold
     * @return the resume threshold, always in {@code [high / 2, high)}
     */
    private static double lowFor(double high) {
        return high >= 2 * DEFAULT_HYSTERESIS ? high - DEFAULT_HYSTERESIS : high / 2;
    }

    /**
     * A policy on the real heap: refuse above 85%, resume below 75%.
     *
     * @return the policy
     */
    public static HeapPressurePolicy defaults() {
        return of(DEFAULT_HIGH);
    }

    /**
     * The reading over the running JVM's heap pools.
     *
     * <p>The beans are resolved once, here, rather than on every read: the platform list is fixed
     * for the life of the JVM, and {@code ManagementFactory} lookups are not free.</p>
     *
     * @return a reader summing post-collection usage over the heap pools
     */
    public static HeapReader platformReader() {

        final List<MemoryPoolMXBean> heapPools = new ArrayList<>();

        for (MemoryPoolMXBean pool : ManagementFactory.getMemoryPoolMXBeans()) {
            if (pool.getType() == MemoryType.HEAP) {
                heapPools.add(pool);
            }
        }

        return () -> {

            final long max = Runtime.getRuntime().maxMemory();

            if (max <= 0 || heapPools.isEmpty()) {
                return -1;                       // unknown; treated as "no pressure"
            }

            long used = 0;

            for (MemoryPoolMXBean pool : heapPools) {
                // null until the pool has been collected at least once, and some pools report
                // max == -1 — neither is a reason to refuse anything.
                final MemoryUsage usage = pool.getCollectionUsage();
                if (usage != null) {
                    used += usage.getUsed();
                }
            }

            return (double) used / (double) max;
        };
    }

    /** @return the high-water mark this policy refuses above */
    public double high() {
        return high;
    }

    /** @return the low-water mark this policy resumes below */
    public double low() {
        return low;
    }

    /** @return the occupancy fraction this policy last acted on, or {@code -1} before the first */
    public double lastPressure() {
        synchronized (stateLock) {
            return lastPressure;
        }
    }

    /** @return whether the policy is currently in its refusing state */
    public boolean isUnderPressure() {
        synchronized (stateLock) {
            return underPressure;
        }
    }

    /**
     * Reads the heap now, bypassing the memo.
     *
     * @return the current occupancy fraction, or negative if it cannot be determined
     */
    public double readPressure() {
        return reader.occupancyFraction();
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        return !pressured();
    }

    /**
     * The hysteresis state machine, on a reading no older than the sample period.
     *
     * @return whether admission should currently be refused
     */
    private boolean pressured() {

        synchronized (stateLock) {

            final long now = clock.millis();

            /*
             * `now < lastReadMillis` is a clock that stepped backwards — an NTP correction, a
             * manual change, a VM resume; InstantSource.system() is wall-clock time. Read as
             * "not yet elapsed", a step back froze the reading, and a latched refusal with it,
             * for the whole size of the step. Read as "elapsed" it costs one extra sample. The
             * memo may under-cache on a bad clock; it must never over-cache.
             */
            final boolean elapsed = now - lastReadMillis >= samplePeriodMillis || now < lastReadMillis;

            if (!everRead || samplePeriodMillis == 0 || elapsed) {
                lastPressure = reader.occupancyFraction();
                lastReadMillis = now;
                everRead = true;
                updateState(lastPressure);
            }

            return underPressure;
        }
    }

    /** Crosses the thresholds and logs the transitions — never the steady state. */
    private void updateState(double pressure) {

        if (pressure < 0 || Double.isNaN(pressure)) {
            /*
             * NaN as well as negative: every comparison against NaN is false, so without this
             * it fell through both threshold checks below and left the state exactly as it was
             * — a latch by omission, for a reader that computes 0.0/0.0.
             *
             * The reader could not tell. HeapReader documents "unknown" as meaning no pressure,
             * and a policy that cannot see the heap must not be the reason a batch stops — so
             * clear the state rather than leaving it alone. Leaving it alone latched: a reader
             * that started failing while the policy was refusing would keep it refusing for the
             * life of the JVM, which is the opposite of the documented reading.
             */
            if (underPressure) {
                underPressure = false;
                LOGGER.info("executors: heap occupancy is no longer readable — admitting again.");
            }
            return;
        }

        if (!underPressure && pressure > high) {
            underPressure = true;
            LOGGER.warn("executors: heap at {}% (> {}%) — refusing admission until it falls below {}%.",
                    percent(pressure), percent(high), percent(low));
            return;
        }

        if (underPressure && pressure < low) {
            underPressure = false;
            LOGGER.info("executors: heap back to {}% (< {}%) — admitting again.", percent(pressure), percent(low));
        }
    }

    private static long percent(double fraction) {
        return Math.round(fraction * 100);
    }

    @Override
    public String toString() {
        synchronized (stateLock) {
            return "HeapPressurePolicy[high=" + high + ", low=" + low
                    + ", lastPressure=" + lastPressure + ", underPressure=" + underPressure + "]";
        }
    }
}
