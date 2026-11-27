package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The learning model in isolation: a bounded ring of {@link Sample}s per task name and the
 * maxima read from it through an {@link Estimator}. Pure data, no executor involved.
 *
 * <p>Vocabulary: a <em>ratio</em> sample has a declared size {@code > 0} (it teaches
 * measured/declared); every measured sample teaches an <em>absolute</em>. Each estimator
 * reads its own measurement first and falls back to the other one only when its own was
 * never recorded in the ring.</p>
 */
class TaskProfileTests {

    private static final long NM = Sample.NOT_MEASURED;

    private static Sample sample(long declared, long alloc, long heap) {
        return new Sample(declared, alloc, heap, 1_000L);
    }

    /** A sample measured at {@code at} under {@code version}; alloc only. */
    private static Sample measured(long at, String version) {
        return new Sample(-1, 10, NM, at, version);
    }

    private static TaskProfile profileOf(int ringSize, Sample... samples) {
        TaskProfile profile = TaskProfile.empty("task");
        for (Sample s : samples) {
            profile = profile.append(s, ringSize);
        }
        return profile;
    }

    @Nested
    @DisplayName("duration-only samples")
    class DurationOnly {

        @Test
        @DisplayName("never evict a memory sample — they take only the ring's spare capacity")
        void neverEvictAMemorySample() {

            /*
             * On the virtual backend a task's memory is measured once, on its solo run, and
             * its duration on every run after. If a duration-only sample could push the memory
             * sample out of the ring, the task would become unknown again every ringSize runs
             * and be quarantined and re-measured on a cycle. Timings fill what the ring has
             * spare and evict only each other.
             */
            final Sample memory = new Sample(-1, 800, NM, 1_000L, null, 5L);
            final Sample t1 = new Sample(-1, NM, NM, 2_000L, null, 9L);
            final Sample t2 = new Sample(-1, NM, NM, 3_000L, null, 7L);
            final Sample t3 = new Sample(-1, NM, NM, 4_000L, null, 3L);

            final TaskProfile profile = profileOf(2, memory, t1, t2, t3);

            assertThat(profile.samples()).hasSize(2).contains(memory);
            assertThat(profile.samples()).as("the newest timing survived").contains(t3);
            assertThat(profile.absolute(Estimator.ALLOCATION)).as("still learned").hasValue(800L);
            assertThat(profile.duration()).as("the longest timing still in the ring").hasValue(5L);
        }

        @Test
        @DisplayName("a ring full of memory samples drops an incoming timing rather than a measurement")
        void ringFullOfMemorySamplesDropsTheTiming() {

            // Every run here measures memory, so every run's duration arrives on a memory
            // sample anyway; the one that cannot is the one that costs nothing to lose.
            final Sample m1 = new Sample(-1, 800, NM, 1_000L, null, 5L);
            final Sample timing = new Sample(-1, NM, NM, 2_000L, null, 99L);

            final TaskProfile profile = profileOf(1, m1, timing);

            assertThat(profile.samples()).containsExactly(m1);
            assertThat(profile.duration()).hasValue(5L);
        }

        @Test
        @DisplayName("a ring full of memory samples gives timings up to half of itself, so durations keep learning")
        void timingsTakeHalfOfAFullMemoryRing() {

            /*
             * A profile written under ForkJoin holds ringSize memory samples. Run the same task
             * on the virtual backend and every run measures only a duration: dropped, its
             * duration() froze at whatever the ForkJoin runs recorded, for ever. Memory samples
             * still own the ring's first half — the solo-run measurement can never be pushed
             * out — but past that, timings evict the oldest memory sample and then rotate
             * among themselves.
             */
            TaskProfile profile = TaskProfile.empty("task");
            for (int i = 0; i < 16; i++) {
                profile = profile.append(new Sample(-1, 800, NM, 1_000L + i, null, 5L), 16);
            }
            final Sample first = profile.samples().get(0);

            final Sample timing = new Sample(-1, NM, NM, 2_000L, null, 99L);
            profile = profile.append(timing, 16);

            assertThat(profile.samples()).hasSize(16).doesNotContain(first).endsWith(timing);
            assertThat(profile.duration()).as("the fresh timing counts").hasValue(99L);

            for (int i = 0; i < 20; i++) {
                profile = profile.append(new Sample(-1, NM, NM, 3_000L + i, null, 10L + i), 16);
            }
            assertThat(profile.samples()).hasSize(16);
            assertThat(profile.samples().stream().filter(Sample::measuresMemory).count())
                    .as("memory keeps half the ring, whatever arrives")
                    .isEqualTo(8);
            assertThat(profile.samples()).as("timings rotate among themselves").doesNotContain(timing);
            assertThat(profile.absolute(Estimator.ALLOCATION)).as("still learned").hasValue(800L);
        }

        @Test
        @DisplayName("the memory floor is at least one: a single measurement is never evicted by a timing")
        void singleMemorySampleStaysWhateverTheRing() {

            final Sample m1 = new Sample(-1, 800, NM, 1_000L, null, 5L);
            final Sample m2 = new Sample(-1, 900, NM, 1_500L, null, 6L);

            TaskProfile two = profileOf(2, m1, m2).append(new Sample(-1, NM, NM, 2_000L, null, 99L), 2);
            assertThat(two.samples()).as("ring 2: half is one, the other half is the timing").hasSize(2).contains(m2);
            assertThat(two.duration()).hasValue(99L);

            two = two.append(new Sample(-1, NM, NM, 3_000L, null, 1L), 2);
            assertThat(two.samples()).as("the one memory sample is the floor now").contains(m2);
            assertThat(two.samples().stream().filter(Sample::measuresMemory).count()).isEqualTo(1);
        }

        @Test
        @DisplayName("a timing offered to a shrinking ring that ends up unplaceable is dropped, never a crash")
        void unplaceableTimingIntoAShrinkingRingIsDropped() {

            /*
             * Profiles built under a larger ring reach append() with more samples than the new
             * ring holds (append is public API; profiles escape via find()/snapshot()). The
             * trim loop must notice mid-trim that nothing more may be evicted and give up,
             * not index the list with -1.
             */
            final Sample mem = new Sample(-1, 800, NM, 1_000L, null, 5L);
            final Sample timing = new Sample(-1, NM, NM, 2_000L, null, 9L);
            final Sample late = new Sample(-1, NM, NM, 3_000L, null, 7L);

            final TaskProfile memAndTiming = profileOf(2, mem, timing);
            assertThat(memAndTiming.append(late, 1))
                    .as("after evicting the timing only the floor is left: dropped, unchanged")
                    .isSameAs(memAndTiming);

            final TaskProfile twoMems = profileOf(2, mem, new Sample(-1, 900, NM, 1_500L, null, 6L));
            assertThat(twoMems.append(late, 1))
                    .as("one memory sample above the floor, then the floor: dropped, unchanged")
                    .isSameAs(twoMems);
        }

        @Test
        @DisplayName("a timing the ring cannot take returns the same profile, unchanged")
        void droppedTimingReturnsThis() {

            final TaskProfile profile = profileOf(1, new Sample(-1, 800, NM, 1_000L, null, 5L));

            assertThat(profile.append(new Sample(-1, NM, NM, 2_000L, null, 99L), 1))
                    .as("a store keys dirtiness on identity, so a dropped sample must not look like a change")
                    .isSameAs(profile);
        }

        @Test
        @DisplayName("a sample carrying only a duration is a valid sample")
        void durationOnlyIsValid() {
            assertThatCode(() -> new Sample(-1, NM, NM, 1_000L, null, 42L)).doesNotThrowAnyException();
            assertThatThrownBy(() -> new Sample(-1, NM, NM, 1_000L, null, NM))
                    .as("but a sample that measured nothing at all still teaches nothing")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("empty profile")
    class EmptyProfile {

        @Test
        @DisplayName("carries the name, no samples, and estimates nothing")
        void carriesNameNoSamplesEstimatesNothing() {
            final TaskProfile empty = TaskProfile.empty("VoyageReport");

            assertThat(empty.name()).isEqualTo("VoyageReport");
            assertThat(empty.samples()).isEmpty();
            for (Estimator estimator : Estimator.values()) {
                assertThat(empty.ratio(estimator)).as("ratio via %s", estimator).isEmpty();
                assertThat(empty.absolute(estimator)).as("absolute via %s", estimator).isEmpty();
            }
        }
    }

    @Nested
    @DisplayName("the ring")
    class Ring {

        @Test
        @DisplayName("keeps the newest ringSize samples, oldest first")
        void keepsNewestSamplesOldestFirst() {
            final TaskProfile profile = profileOf(3,
                    sample(-1, 10, NM), sample(-1, 20, NM), sample(-1, 30, NM), sample(-1, 40, NM), sample(-1, 50, NM));

            assertThat(profile.samples()).extracting(Sample::allocatedBytes).containsExactly(30L, 40L, 50L);
        }

        @Test
        @DisplayName("a profile larger than the ring it is appended to is trimmed to that ring")
        void appendTrimsToTheRing() {

            // append is public API on a public SPI: a store is free to change its ring size, and
            // "the maximum number of samples to keep" has to mean that on every call.
            final TaskProfile sixteen = profileOf(16,
                    java.util.stream.IntStream.range(0, 16).mapToObj(i -> sample(-1, i, NM)).toArray(Sample[]::new));

            final TaskProfile four = sixteen.append(sample(-1, 100, NM), 4);
            assertThat(four.samples()).extracting(Sample::allocatedBytes).containsExactly(13L, 14L, 15L, 100L);

            final TaskProfile timed = sixteen.append(new Sample(-1, NM, NM, 1_000L, null, 7L), 4);
            assertThat(timed.samples()).as("trimmed to fit, then the timing takes the spare slot")
                    .extracting(Sample::allocatedBytes).containsExactly(13L, 14L, 15L, NM);
        }

        @Test
        @DisplayName("append returns a new profile and leaves the old one untouched")
        void appendIsPersistent() {
            final TaskProfile one = profileOf(16, sample(-1, 10, NM));
            final TaskProfile two = one.append(sample(-1, 20, NM), 16);

            assertThat(one.samples()).hasSize(1);
            assertThat(two.samples()).hasSize(2);
            assertThat(two.name()).isEqualTo(one.name());
        }

        @Test
        @DisplayName("a maximum that scrolls off the ring is forgotten")
        void maximaFollowTheRing() {
            final TaskProfile profile = profileOf(2,
                    sample(-1, 900, NM), sample(-1, 10, NM), sample(-1, 20, NM));

            assertThat(profile.absolute(Estimator.ALLOCATION)).hasValue(20L);
        }

        @Test
        @DisplayName("the sample list is unmodifiable")
        void samplesAreUnmodifiable() {
            final TaskProfile profile = profileOf(16, sample(-1, 10, NM));

            assertThatThrownBy(() -> profile.samples().add(sample(-1, 1, NM)))
                    .isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("rejects a ring smaller than one and a null sample")
        void rejectsBadArguments() {
            final TaskProfile empty = TaskProfile.empty("task");

            assertThatThrownBy(() -> empty.append(sample(-1, 1, NM), 0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> empty.append(null, 16)).isInstanceOf(NullPointerException.class);
        }
    }

    @Nested
    @DisplayName("ratio maxima")
    class RatioMaxima {

        @Test
        @DisplayName("are the largest measured/declared over samples with declared > 0 and a measurement")
        void largestRatioOverQualifyingSamples() {
            final TaskProfile profile = profileOf(16,
                    sample(100, 200, NM),   // 2.0
                    sample(100, 400, NM),   // 4.0  <- max
                    sample(-1, 900, NM),    // no declared size: absolute only
                    sample(0, 500, NM),     // declared 0: never a divisor
                    sample(100, NM, 800));  // allocation not measured: no allocation ratio

            assertThat(profile.ratio(Estimator.ALLOCATION)).hasValueCloseTo(4.0, within(1e-9));
        }

        @Test
        @DisplayName("a measured zero is a ratio of zero, not a missing ratio")
        void measuredZeroIsZeroRatio() {
            final TaskProfile profile = profileOf(16, sample(100, 0, NM));

            assertThat(profile.ratio(Estimator.ALLOCATION)).hasValueCloseTo(0.0, within(1e-9));
        }

        @Test
        @DisplayName("are empty when every sample lacks a declared size")
        void emptyWithoutDeclaredSizes() {
            final TaskProfile profile = profileOf(16, sample(-1, 200, 300), sample(0, 200, 300));

            assertThat(profile.ratio(Estimator.ALLOCATION)).isEmpty();
            assertThat(profile.ratio(Estimator.SOLO_HEAP_PEAK)).isEmpty();
        }
    }

    @Nested
    @DisplayName("absolute maxima")
    class AbsoluteMaxima {

        @Test
        @DisplayName("are the largest measurement over every sample, declared or not")
        void largestOverEverySample() {
            final TaskProfile profile = profileOf(16,
                    sample(100, 200, NM), sample(-1, 900, NM), sample(0, 500, NM), sample(100, NM, 800));

            assertThat(profile.absolute(Estimator.ALLOCATION)).hasValue(900L);
            assertThat(profile.absolute(Estimator.SOLO_HEAP_PEAK)).hasValue(800L);
        }

        @Test
        @DisplayName("a measured zero is an absolute of zero")
        void measuredZeroIsZero() {
            final TaskProfile profile = profileOf(16, sample(-1, 0, NM));

            assertThat(profile.absolute(Estimator.ALLOCATION)).hasValue(0L);
        }
    }

    @Nested
    @DisplayName("estimator fallback")
    class Fallback {

        @Test
        @DisplayName("each estimator prefers its own measurement when both were recorded")
        void prefersOwnMeasurement() {
            final TaskProfile profile = profileOf(16, sample(100, 200, 800));

            assertThat(profile.ratio(Estimator.ALLOCATION)).hasValueCloseTo(2.0, within(1e-9));
            assertThat(profile.ratio(Estimator.SOLO_HEAP_PEAK)).hasValueCloseTo(8.0, within(1e-9));
            assertThat(profile.absolute(Estimator.ALLOCATION)).hasValue(200L);
            assertThat(profile.absolute(Estimator.SOLO_HEAP_PEAK)).hasValue(800L);
        }

        @Test
        @DisplayName("ALLOCATION falls back to heap when no allocation was ever measured (virtual-thread workers)")
        void allocationFallsBackToHeap() {
            final TaskProfile profile = profileOf(16, sample(100, NM, 800), sample(-1, NM, 1_600));

            assertThat(profile.ratio(Estimator.ALLOCATION)).hasValueCloseTo(8.0, within(1e-9));
            assertThat(profile.absolute(Estimator.ALLOCATION)).hasValue(1_600L);
        }

        @Test
        @DisplayName("SOLO_HEAP_PEAK falls back to allocation when no solo heap peak was ever measured")
        void heapFallsBackToAllocation() {
            final TaskProfile profile = profileOf(16, sample(100, 200, NM), sample(-1, 900, NM));

            assertThat(profile.ratio(Estimator.SOLO_HEAP_PEAK)).hasValueCloseTo(2.0, within(1e-9));
            assertThat(profile.absolute(Estimator.SOLO_HEAP_PEAK)).hasValue(900L);
        }

        @Test
        @DisplayName("fallback is per quantity: a ratio may fall back while the absolute does not")
        void fallbackIsPerQuantity() {
            // Allocation was measured only on an unsized run; the sized run measured heap only.
            final TaskProfile profile = profileOf(16, sample(-1, 900, NM), sample(100, NM, 800));

            assertThat(profile.absolute(Estimator.ALLOCATION)).as("own absolute exists").hasValue(900L);
            assertThat(profile.ratio(Estimator.ALLOCATION)).as("no own ratio -> heap ratio").hasValueCloseTo(8.0, within(1e-9));
        }
    }

    @Nested
    @DisplayName("Sample validation")
    class SampleValidation {

        @Test
        @DisplayName("needs at least one measurement")
        void needsAMeasurement() {
            assertThatThrownBy(() -> new Sample(100, NM, NM, 0L))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("measure");
        }

        @Test
        @DisplayName("rejects values below -1")
        void rejectsValuesBelowMinusOne() {
            assertThatThrownBy(() -> new Sample(-2, 1, NM, 0L)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new Sample(1, -2, NM, 0L)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new Sample(1, NM, -2, 0L)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("accepts -1 as 'none' for the declared size and either measurement")
        void acceptsSentinels() {
            assertThat(new Sample(-1, -1, 0, 0L).soloHeapPeakBytes()).isZero();
            assertThat(new Sample(-1, 0, -1, 0L).allocatedBytes()).isZero();
            assertThat(List.of(new Sample(0, 0, 0, 0L))).hasSize(1);
        }
    }
    @Nested
    @DisplayName("when it was measured")
    class WhenMeasured {

        @Test
        @DisplayName("an empty profile was never measured")
        void emptyWasNeverMeasured() {
            assertThat(TaskProfile.empty("task").lastMeasuredAt()).isEmpty();
        }

        @Test
        @DisplayName("lastMeasuredAt is the newest sample's completion instant, whatever the ring order")
        void lastMeasuredAtIsTheNewestSample() {
            final TaskProfile profile = profileOf(16,
                    measured(5_000, null), measured(9_000, null), measured(7_000, null));

            assertThat(profile.lastMeasuredAt()).contains(Instant.ofEpochMilli(9_000));
        }

        @Test
        @DisplayName("a sample remembers the version it was measured under; the 4-argument form means 'unversioned'")
        void sampleCarriesItsVersion() {
            assertThat(new Sample(-1, 10, NM, 1L, "2026.08").version()).isEqualTo("2026.08");
            assertThat(new Sample(-1, 10, NM, 1L).version()).isNull();
            assertThat(new Sample(-1, 10, NM, 1L)).isEqualTo(new Sample(-1, 10, NM, 1L, null));
        }

        @Test
        @DisplayName("an empty version string is rejected — it would be indistinguishable from 'unversioned' in the file")
        void emptyVersionRejected() {
            assertThatThrownBy(() -> new Sample(-1, 10, NM, 1L, ""))
                    .isInstanceOf(IllegalArgumentException.class)
                    .hasMessageContaining("version");
        }
    }

    @Nested
    @DisplayName("fresh(version, notBefore) — the view the sizing actually reads")
    class Freshness {

        private static final long NO_AGE_LIMIT = Long.MIN_VALUE;

        @Test
        @DisplayName("no version and no age limit: every sample is fresh, and it is the same instance (no allocation on the admission path)")
        void everythingFreshIsTheSameInstance() {
            final TaskProfile profile = profileOf(16, measured(1_000, "a"), measured(2_000, null), measured(3_000, "b"));

            assertThat(profile.fresh(null, NO_AGE_LIMIT)).isSameAs(profile);
        }

        @Test
        @DisplayName("a whole ring under the configured version is the same instance too")
        void uniformVersionIsTheSameInstance() {
            final TaskProfile profile = profileOf(16, measured(1_000, "a"), measured(2_000, "a"));

            assertThat(profile.fresh("a", NO_AGE_LIMIT)).isSameAs(profile);
            assertThat(profile.fresh("a", 1_000)).as("oldest exactly at the cutoff still counts").isSameAs(profile);
        }

        @Test
        @DisplayName("samples older than the cutoff drop out, and the maxima follow")
        void ageFiltersSamplesAndMaxima() {
            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, null),     // old, the largest
                    new Sample(-1, 100, NM, 2_000L, null),
                    new Sample(-1, 200, NM, 3_000L, null));

            final TaskProfile fresh = profile.fresh(null, 2_000);

            assertThat(fresh.samples()).extracting(Sample::epochMillis).containsExactly(2_000L, 3_000L);
            assertThat(fresh.absolute(Estimator.ALLOCATION)).hasValue(200L);
            assertThat(fresh.name()).isEqualTo(profile.name());
            assertThat(profile.samples()).as("the original is untouched").hasSize(3);
        }

        @Test
        @DisplayName("a filtered view is computed once and reused until the cutoff actually moves past a sample")
        void filteredViewIsReusedWhileItStaysCorrect() {

            /*
             * The admission path asks for this view once per candidate per scan, and with
             * maxSampleAge configured the ring's oldest sample is stale in steady state — so
             * the fast "everything qualifies" return never fires and every call allocated a
             * list, copied the ring and re-ran the whole aggregation, under the harness lock.
             *
             * The view depends only on (version, notBefore), and notBefore moves with the
             * clock, so a computed view stays EXACTLY correct until the cutoff passes the
             * oldest sample it kept. That window is what makes memoising it sound rather than
             * approximate: no staleness, not even a millisecond's worth.
             */
            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, null),
                    new Sample(-1, 100, NM, 2_000L, null),
                    new Sample(-1, 200, NM, 3_000L, null));

            final TaskProfile first = profile.fresh(null, 1_500);
            assertThat(first).as("a stale sample is present, so this is a real filtered copy").isNotSameAs(profile);
            assertThat(first.samples()).extracting(Sample::epochMillis).containsExactly(2_000L, 3_000L);

            assertThat(profile.fresh(null, 1_500)).as("the same question").isSameAs(first);
            assertThat(profile.fresh(null, 1_900)).as("the clock moved, the answer did not").isSameAs(first);
            assertThat(profile.fresh(null, 2_000))
                    .as("the oldest kept sample is exactly at the cutoff — still in")
                    .isSameAs(first);

            final TaskProfile narrower = profile.fresh(null, 2_001);
            assertThat(narrower).as("now a sample really did fall out").isNotSameAs(first);
            assertThat(narrower.samples()).extracting(Sample::epochMillis).containsExactly(3_000L);
        }

        @Test
        @DisplayName("a reused view is never handed back for a question it does not answer")
        void reuseNeverWidensTheView() {

            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, null),
                    new Sample(-1, 100, NM, 2_000L, null));

            assertThat(profile.fresh(null, 1_500).samples()).hasSize(1);

            assertThat(profile.fresh(null, 500).samples())
                    .as("a cutoff EARLIER than the one it was computed for lets a dropped sample back in")
                    .extracting(Sample::epochMillis)
                    .containsExactly(1_000L, 2_000L);

            assertThat(profile.fresh("v2", 1_500).samples())
                    .as("a different version is a different question")
                    .isEmpty();
        }

        @Test
        @DisplayName("recording pushes a new profile, so nothing carries the old view forward")
        void recordingDoesNotInheritTheView() {

            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, null),
                    new Sample(-1, 100, NM, 2_000L, null));

            assertThat(profile.fresh(null, 1_500).samples()).hasSize(1);

            final TaskProfile grown = profile.append(new Sample(-1, 300, NM, 3_000L, null), 16);

            assertThat(grown.fresh(null, 1_500).samples())
                    .as("append returns a new profile; the memo belongs to the old one")
                    .extracting(Sample::epochMillis)
                    .containsExactly(2_000L, 3_000L);
        }

        @Test
        @DisplayName("samples from another version drop out; unversioned samples never match a configured version")
        void versionFiltersSamples() {
            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, "build-1"),
                    new Sample(-1, 800, NM, 2_000L, null),
                    new Sample(-1, 100, NM, 3_000L, "build-2"));

            final TaskProfile fresh = profile.fresh("build-2", NO_AGE_LIMIT);

            assertThat(fresh.samples()).extracting(Sample::allocatedBytes).containsExactly(100L);
            assertThat(fresh.absolute(Estimator.ALLOCATION)).hasValue(100L);
        }

        @Test
        @DisplayName("both filters apply together")
        void bothFiltersApply() {
            final TaskProfile profile = profileOf(16,
                    new Sample(-1, 900, NM, 1_000L, "b"),      // right version, too old
                    new Sample(-1, 800, NM, 5_000L, "a"),      // recent, wrong version
                    new Sample(-1, 100, NM, 5_000L, "b"));

            assertThat(profile.fresh("b", 4_000).samples()).extracting(Sample::allocatedBytes).containsExactly(100L);
        }

        @Test
        @DisplayName("nothing fresh: an empty profile that estimates nothing, not null")
        void nothingFreshIsEmpty() {
            final TaskProfile profile = profileOf(16, measured(1_000, "old"));

            final TaskProfile fresh = profile.fresh("new", NO_AGE_LIMIT);

            assertThat(fresh.samples()).isEmpty();
            assertThat(fresh.lastMeasuredAt()).isEmpty();
            assertThat(fresh.absolute(Estimator.ALLOCATION)).isEmpty();
            assertThat(fresh.ratio(Estimator.SOLO_HEAP_PEAK)).isEmpty();
        }

        @Test
        @DisplayName("a fresh view can keep learning: append works on it like on any profile")
        void freshViewStillAppends() {
            final TaskProfile fresh = profileOf(16, measured(1_000, "old")).fresh("new", NO_AGE_LIMIT);

            assertThat(fresh.append(measured(2_000, "new"), 16).samples()).hasSize(1);
        }
    }
}
