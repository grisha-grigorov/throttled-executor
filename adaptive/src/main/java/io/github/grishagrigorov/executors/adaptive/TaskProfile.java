package io.github.grishagrigorov.executors.adaptive;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * What has been learned about one task name: a bounded ring of the most recent
 * {@link Sample}s plus the maxima derived from them, precomputed so that estimating on the
 * admission path is O(1).
 *
 * <p>Two quantities are learned, each through two measurements:</p>
 * <ul>
 *   <li><b>ratio</b> — measured ÷ declared, from samples whose declared size was {@code > 0}.
 *       For tasks that know their input size (bytes to read) and learn the multiplier.</li>
 *   <li><b>absolute</b> — the measurement itself, from every measured sample. For tasks
 *       submitted with no size at all.</li>
 * </ul>
 * <p>Both are <em>maxima</em> over the ring, not averages: under-throttling costs an
 * {@code OutOfMemoryError}, over-throttling costs a little parallelism.</p>
 *
 * <p>Every sample also says <em>when</em> and <em>under which application version</em> it
 * was measured; {@link #fresh(String, long)} is the view restricted to the samples that
 * still count, and is what the sizing reads.</p>
 *
 * <p>Instances are immutable; {@link #append(Sample, int)} and {@link #fresh(String, long)}
 * return new profiles.</p>
 */
public final class TaskProfile {

    private static final long NONE = -1L;

    private final String name;
    private final List<Sample> samples;
    private final double maxAllocRatio;      // NaN when no sample qualifies
    private final long maxAllocAbsolute;     // NONE when nothing was measured
    private final double maxHeapRatio;
    private final long maxHeapAbsolute;
    private final long maxDurationNanos;     // NONE when nothing was timed
    private final long oldestMillis;         // Long.MAX_VALUE when empty

    /**
     * The last filtered view {@link #fresh} built, and the exact range of questions it answers.
     *
     * <p>Not a cache in the usual sense — there is no staleness to trade away. The view is a
     * function of {@code (version, notBeforeMillis)} alone, and a profile is immutable, so a
     * view computed for one cutoff stays <em>exactly</em> correct for every cutoff between that
     * one and the moment the cutoff passes the oldest sample it kept. That window is what the
     * entry records, and outside it this recomputes.</p>
     *
     * <p>It earns its keep because of who calls {@code fresh}: the harness asks
     * {@code requiresExclusive} for every candidate in the window on every scan and then
     * {@code admit} for the candidates ahead of the chosen one, all under one lock. The
     * allocation-free fast path only fires while the whole ring qualifies, so with
     * {@code maxSampleAge} configured a profiled name in steady state missed it every time and
     * paid a list allocation, a ring copy and a full re-aggregation per question.</p>
     *
     * <p>Plain {@code volatile}, deliberately unsynchronised: two threads racing here both
     * compute the same value and one write wins, which costs a duplicate copy and never a wrong
     * answer. {@link FreshView} is immutable and safely published by the volatile write.</p>
     */
    private volatile FreshView freshView;

    /**
     * One filtered view plus the cutoff range over which it is the right answer.
     *
     * @param version    the version it was filtered for, {@code null} for unfiltered
     * @param from       the cutoff it was computed for; an earlier one could let a dropped
     *                   sample back in, so it is the lower bound rather than a starting point
     * @param through    the oldest sample the view KEPT — the last cutoff that still keeps it
     *                   ({@link Long#MAX_VALUE} when the view kept nothing, since a later cutoff
     *                   can only keep nothing too)
     * @param view       the filtered profile
     */
    private record FreshView(String version, long from, long through, TaskProfile view) {

        boolean answers(String askedVersion, long notBeforeMillis) {
            return notBeforeMillis >= from
                    && notBeforeMillis <= through
                    && (askedVersion == null ? version == null : askedVersion.equals(version));
        }
    }
    private final long newestMillis;         // Long.MIN_VALUE when empty
    /** The one version every sample carries, or {@link #MIXED} when they differ (null = all unversioned). */
    private final String uniformVersion;

    @SuppressWarnings("StringOperationCanBeSimplified")
    private static final String MIXED = new String("<mixed>");   // identity-compared sentinel, never equal to a real version

    private TaskProfile(String name, List<Sample> samples) {
        this.name = name;
        this.samples = samples;
        double allocRatio = Double.NaN;
        double heapRatio = Double.NaN;
        long allocAbsolute = NONE;
        long heapAbsolute = NONE;
        long duration = NONE;
        long oldest = Long.MAX_VALUE;
        long newest = Long.MIN_VALUE;
        String version = null;
        boolean first = true;
        for (Sample s : samples) {
            oldest = Math.min(oldest, s.epochMillis());
            newest = Math.max(newest, s.epochMillis());
            duration = Math.max(duration, s.durationNanos());
            if (first) {
                version = s.version();
                first = false;
            } else if (version != MIXED && !Objects.equals(version, s.version())) {
                version = MIXED;
            }
            if (s.allocatedBytes() >= 0) {
                allocAbsolute = Math.max(allocAbsolute, s.allocatedBytes());
                if (s.declaredSize() > 0) {
                    allocRatio = maxIgnoringNaN(allocRatio, (double) s.allocatedBytes() / s.declaredSize());
                }
            }
            if (s.soloHeapPeakBytes() >= 0) {
                heapAbsolute = Math.max(heapAbsolute, s.soloHeapPeakBytes());
                if (s.declaredSize() > 0) {
                    heapRatio = maxIgnoringNaN(heapRatio, (double) s.soloHeapPeakBytes() / s.declaredSize());
                }
            }
        }
        this.maxAllocRatio = allocRatio;
        this.maxAllocAbsolute = allocAbsolute;
        this.maxHeapRatio = heapRatio;
        this.maxHeapAbsolute = heapAbsolute;
        this.maxDurationNanos = duration;
        this.oldestMillis = oldest;
        this.newestMillis = newest;
        this.uniformVersion = version;
    }

    private static double maxIgnoringNaN(double current, double candidate) {
        return Double.isNaN(current) ? candidate : Math.max(current, candidate);
    }

    /**
     * A profile with no samples.
     *
     * @param name the task name
     * @return an empty profile
     */
    public static TaskProfile empty(String name) {
        return new TaskProfile(Objects.requireNonNull(name, "name"), List.of());
    }

    /** @return the task name */
    public String name() {
        return name;
    }

    /** @return the samples, oldest first, at most the ring size; unmodifiable */
    public List<Sample> samples() {
        return samples;
    }

    /**
     * A new profile with {@code sample} appended and trimmed to {@code ringSize}.
     *
     * <p>What is dropped depends on what arrived. A sample that measured memory evicts the
     * oldest sample, as a ring does. A sample carrying only a duration is kept on stricter
     * terms: memory measurements own the ring's first half — the
     * {@linkplain #memoryFloor(int) floor}, at least one — and a timing may never evict one
     * of those. On the virtual backend a task's memory is measured once — on its solo run —
     * and its duration on every run after; a timing that could push that one memory sample
     * out would make the task unknown again every {@code ringSize} runs, to be quarantined
     * and re-measured on a cycle. Above the floor a timing evicts the oldest memory sample,
     * and once only the floor remains, the oldest timing: a profile written where every run
     * measured memory would otherwise never take another timing, and its {@link #duration()}
     * would stay at whatever those runs recorded, for ever. It is still the maximum over the
     * ring, so a duration in the memory half persists until {@link #fresh} ages it out.</p>
     *
     * <p>A timing the ring cannot make room for — the floor is all that is left to evict — is
     * dropped, and this same instance is returned, untrimmed, so a store can tell that nothing
     * changed. Whenever a sample is placed, the result holds at most {@code ringSize} samples,
     * whatever the ring size was when the earlier ones were recorded.</p>
     *
     * @param sample   the new sample
     * @param ringSize the maximum number of samples to keep, {@code >= 1}
     * @return the new profile, or this one when the sample could not be placed
     */
    public TaskProfile append(Sample sample, int ringSize) {
        Objects.requireNonNull(sample, "sample");
        if (ringSize < 1) {
            throw new IllegalArgumentException("ringSize must be >= 1, got " + ringSize);
        }

        final List<Sample> next = new ArrayList<>(samples.size() + 1);
        next.addAll(samples);

        while (next.size() >= ringSize) {
            final int evict = evictable(next, sample.measuresMemory(), ringSize);
            if (evict < 0) {
                return this;                                        // nothing a timing may evict
            }
            next.remove(evict);
        }

        next.add(sample);
        return new TaskProfile(name, Collections.unmodifiableList(next));
    }

    /** How many memory measurements a timing may never evict: half the ring, at least one. */
    static int memoryFloor(int ringSize) {
        return Math.max(1, ringSize / 2);
    }

    /**
     * The index a new sample may evict, or {@code -1} when a timing may evict nothing. A
     * memory sample evicts the oldest of anything; a timing evicts the oldest memory sample
     * while more than the floor remain, else the oldest timing. Only a ring of one can
     * refuse: anywhere larger, a full ring holds either a timing or more memory samples than
     * the floor.
     */
    private static int evictable(List<Sample> ring, boolean memoryArrived, int ringSize) {

        if (memoryArrived) {
            return 0;
        }

        int memorySamples = 0;
        int oldestMemory = -1;
        int oldestTiming = -1;
        for (int i = 0; i < ring.size(); i++) {
            if (ring.get(i).measuresMemory()) {
                memorySamples++;
                if (oldestMemory < 0) {
                    oldestMemory = i;
                }
            } else if (oldestTiming < 0) {
                oldestTiming = i;
            }
        }

        return memorySamples > memoryFloor(ringSize) ? oldestMemory : oldestTiming;
    }

    /**
     * When the newest sample in the ring completed.
     *
     * @return the instant, or empty for a profile with no samples
     */
    public Optional<Instant> lastMeasuredAt() {
        return samples.isEmpty() ? Optional.empty() : Optional.of(Instant.ofEpochMilli(newestMillis));
    }

    /**
     * The longest run recorded for this task.
     *
     * <p>The maximum rather than the mean, matching the memory aggregates and for the same
     * reason: when scheduling, what a task <em>can</em> cost is what matters. A task that
     * usually takes a second and occasionally takes a minute should be started early, and an
     * average would hide the minute.</p>
     *
     * <p>Empty when no sample carries a duration — every sample written before durations were
     * learned, and every sample read from a v1 or v2 profile file.</p>
     *
     * @return the longest measured body time in nanoseconds, or empty
     */
    public OptionalLong duration() {
        return maxDurationNanos == NONE ? OptionalLong.empty() : OptionalLong.of(maxDurationNanos);
    }

    /**
     * The samples that still count: measured under {@code version} (any version when
     * {@code null}; an unversioned sample never matches a named version) and completed at
     * {@code notBeforeMillis} or later. This is what {@link AdaptiveSizing#effectiveSize}
     * reads, so a profile whose samples have all gone stale estimates nothing — the task is
     * treated as never seen and measured again.
     *
     * <p>Returns {@code this} when every sample qualifies — two comparisons against values
     * precomputed at construction, no allocation on the admission path. Only a profile that
     * has actually gone partly stale pays for the filtered copy, and it heals itself as new
     * samples push the stale ones off the ring.</p>
     *
     * <p>A profile that does not qualify keeps the last view it built and hands it back for
     * every cutoff it still answers — see {@link #freshView}. That is a memo, not a cache:
     * the answer it returns is the one a recomputation would produce, so nothing goes stale.
     * It matters because with {@code maxSampleAge} set the fast path above stops firing for a
     * profiled name in steady state, and the admission path asks this question once per
     * candidate per scan under the harness lock.</p>
     *
     * @param version         the configured application version, or {@code null} for none
     * @param notBeforeMillis the oldest completion time still trusted ({@link Long#MIN_VALUE}
     *                        for no age limit)
     * @return this profile, or a new one over the qualifying samples (possibly none)
     */
    public TaskProfile fresh(String version, long notBeforeMillis) {

        final boolean versionOk = version == null || (uniformVersion != MIXED && version.equals(uniformVersion));
        if (versionOk && oldestMillis >= notBeforeMillis) {
            return this;
        }

        final FreshView memo = freshView;
        if (memo != null && memo.answers(version, notBeforeMillis)) {
            return memo.view();
        }

        final List<Sample> kept = new ArrayList<>(samples.size());
        for (Sample s : samples) {
            if (s.epochMillis() >= notBeforeMillis && (version == null || version.equals(s.version()))) {
                kept.add(s);
            }
        }

        final TaskProfile view = new TaskProfile(name, Collections.unmodifiableList(kept));
        freshView = new FreshView(version, notBeforeMillis, view.oldestMillis, view);       // the constructor found the oldest already
        return view;
    }

    /**
     * The largest measured/declared ratio over samples with a declared size {@code > 0},
     * read through {@code estimator}: its own measurement if any sample in the ring carries
     * it, otherwise the other measurement.
     *
     * @param estimator which measurement to read
     * @return the ratio, or empty when no sample qualifies
     */
    public OptionalDouble ratio(Estimator estimator) {
        final double own = estimator == Estimator.ALLOCATION ? maxAllocRatio : maxHeapRatio;
        final double other = estimator == Estimator.ALLOCATION ? maxHeapRatio : maxAllocRatio;
        final double value = Double.isNaN(own) ? other : own;
        return Double.isNaN(value) ? OptionalDouble.empty() : OptionalDouble.of(value);
    }

    /**
     * The largest measurement over all samples, read through {@code estimator}: its own
     * measurement if any sample in the ring carries it, otherwise the other measurement.
     *
     * @param estimator which measurement to read
     * @return the bytes, or empty when nothing was measured
     */
    public OptionalLong absolute(Estimator estimator) {
        final long own = estimator == Estimator.ALLOCATION ? maxAllocAbsolute : maxHeapAbsolute;
        final long other = estimator == Estimator.ALLOCATION ? maxHeapAbsolute : maxAllocAbsolute;
        final long value = own == NONE ? other : own;
        return value == NONE ? OptionalLong.empty() : OptionalLong.of(value);
    }

    @Override
    public String toString() {
        return "TaskProfile[" + name + ", samples=" + samples.size()
                + ", allocRatio=" + maxAllocRatio + ", allocAbs=" + maxAllocAbsolute
                + ", heapRatio=" + maxHeapRatio + ", heapAbs=" + maxHeapAbsolute + "]";
    }
}
