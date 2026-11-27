package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The resolution rule — {@link AdaptiveSizing#effectiveSize(String, long)} — that joins what
 * was learned about a task name to the size the delegate policy is shown. Pure: a store, a
 * config, no executor. Samples are recorded straight into the store.
 *
 * <p>The rule, branch by branch:</p>
 * <pre>
 *   no profile            : declared &gt;= 0 -&gt; declared (unchanged, NO headroom)   declared &lt; 0 -&gt; UNKNOWN
 *   declared &gt; 0, ratio   : ceil(declared * maxRatio * headroom)                  no ratio -&gt; declared
 *   declared == 0         : ceil(absolute * headroom)                              no absolute -&gt; 0
 *   declared &lt; 0          : ceil(absolute * headroom)                              no absolute -&gt; UNKNOWN
 *   always                : clamp to [0, Long.MAX_VALUE / 4]
 * </pre>
 * <p>"No profile" includes a profile none of whose samples are <em>fresh</em> — measured
 * under the configured version and within {@code maxSampleAge} of the clock.</p>
 */
class EffectiveSizeTests {

    private static final long NM = Sample.NOT_MEASURED;
    private static final long CLAMP = Long.MAX_VALUE / 4;

    private static AdaptiveConfig config(double headroom, Estimator estimator) {
        return new AdaptiveConfig(16, headroom, estimator, 1L << 30, Duration.ZERO);
    }

    private static AdaptiveSizing sizing(AdaptiveConfig config) {
        return AdaptiveSizing.of(new InMemoryProfileStore(config.ringSize()), config);
    }

    private static AdaptiveSizing sizing() {
        return sizing(config(1.25, Estimator.ALLOCATION));
    }

    private static void learn(AdaptiveSizing sizing, String name, long declared, long alloc, long heap) {
        sizing.store().record(name, new Sample(declared, alloc, heap, 1L));
    }

    @Nested
    @DisplayName("no profile")
    class NoProfile {

        @Test
        @DisplayName("a declared size passes through unchanged — no headroom, identity with an undecorated policy")
        void declaredPassesThroughUnchanged() {
            final AdaptiveSizing sizing = sizing();

            assertThat(sizing.effectiveSize("never-seen", 1_000)).isEqualTo(1_000);
            assertThat(sizing.effectiveSize("never-seen", 0)).isZero();
        }

        @Test
        @DisplayName("no declared size is UNKNOWN")
        void undeclaredIsUnknown() {
            assertThat(sizing().effectiveSize("never-seen", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
            assertThat(AdaptiveSizing.UNKNOWN).isEqualTo(-1L);
        }

        @Test
        @DisplayName("a null name resolves to the declared size (for -1 that value is UNKNOWN's — the policy, not this method, keeps unnamed tasks out of the quarantine)")
        void nullNameResolvesToDeclared() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "other", -1, 900, NM);

            assertThat(sizing.effectiveSize(null, 1_000)).isEqualTo(1_000);
            assertThat(sizing.effectiveSize(null, -1)).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("declared > 0")
    class DeclaredPositive {

        @Test
        @DisplayName("with a learned ratio: ceil(declared * ratio * headroom)")
        void ratioTimesHeadroom() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "read", 100, 200, NM);     // ratio 2.0

            assertThat(sizing.effectiveSize("read", 1_000)).isEqualTo(2_500);
        }

        @Test
        @DisplayName("rounds up, never down")
        void roundsUp() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "read", 3, 10, NM);        // ratio 3.333..; 7 * 3.333.. * 1.25 = 29.17

            assertThat(sizing.effectiveSize("read", 7)).isEqualTo(30);
        }

        @Test
        @DisplayName("the largest ratio in the ring wins")
        void largestRatioWins() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "read", 100, 200, NM);     // 2.0
            learn(sizing, "read", 100, 400, NM);     // 4.0
            learn(sizing, "read", 100, 300, NM);     // 3.0

            assertThat(sizing.effectiveSize("read", 1_000)).isEqualTo(5_000);
        }

        @Test
        @DisplayName("a profile that only ever saw unsized runs teaches no ratio: declared passes through")
        void absoluteOnlyProfilePassesDeclaredThrough() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 900, NM);

            assertThat(sizing.effectiveSize("job", 1_000)).isEqualTo(1_000);
        }
    }

    @Nested
    @DisplayName("declared == 0")
    class DeclaredZero {

        @Test
        @DisplayName("uses the absolute — zero is never a divisor")
        void usesAbsolute() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", 0, 900, NM);

            assertThat(sizing.effectiveSize("job", 0)).isEqualTo(1_125);
        }

        @Test
        @DisplayName("a ratio sample also carries an absolute, which is what declared 0 reads")
        void ratioSampleContributesItsAbsolute() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", 100, 200, NM);

            assertThat(sizing.effectiveSize("job", 0)).isEqualTo(250);
        }
    }

    @Nested
    @DisplayName("declared < 0")
    class DeclaredNegative {

        @Test
        @DisplayName("with a learned absolute: ceil(absolute * headroom)")
        void absoluteTimesHeadroom() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 900, NM);

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(1_125);
        }

        @Test
        @DisplayName("the largest absolute in the ring wins, from sized and unsized runs alike")
        void largestAbsoluteWins() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 900, NM);
            learn(sizing, "job", 100, 1_000, NM);
            learn(sizing, "job", -1, 300, NM);

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(1_250);
        }

        @Test
        @DisplayName("every profile has an absolute: a Sample cannot exist without a measurement")
        void everyProfileHasAnAbsolute() {
            final TaskProfile profile = TaskProfile.empty("x").append(new Sample(100, NM, 0, 1L), 16);

            assertThat(profile.absolute(Estimator.ALLOCATION)).isPresent();
            assertThat(profile.absolute(Estimator.SOLO_HEAP_PEAK)).isPresent();
        }
    }

    @Nested
    @DisplayName("estimator")
    class EstimatorChoice {

        @Test
        @DisplayName("the configured estimator picks which measurement is read")
        void configuredEstimatorIsRead() {
            final AdaptiveSizing viaAlloc = sizing(config(1.25, Estimator.ALLOCATION));
            final AdaptiveSizing viaHeap = sizing(config(1.25, Estimator.SOLO_HEAP_PEAK));
            learn(viaAlloc, "job", 100, 200, 800);
            learn(viaHeap, "job", 100, 200, 800);

            assertThat(viaAlloc.effectiveSize("job", 1_000)).isEqualTo(2_500);
            assertThat(viaHeap.effectiveSize("job", 1_000)).isEqualTo(10_000);
            assertThat(viaAlloc.effectiveSize("job", -1)).isEqualTo(250);
            assertThat(viaHeap.effectiveSize("job", -1)).isEqualTo(1_000);
        }

        @Test
        @DisplayName("ALLOCATION falls back to heap when allocation was never measured (virtual-thread workers)")
        void fallbackFlowsThrough() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION));
            learn(sizing, "job", 100, NM, 800);

            assertThat(sizing.effectiveSize("job", 1_000)).isEqualTo(10_000);
            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(1_000);
        }
    }

    @Nested
    @DisplayName("edges")
    class Edges {

        @Test
        @DisplayName("a measured zero estimates zero, not UNKNOWN")
        void measuredZeroIsZero() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "cheap", -1, 0, NM);
            learn(sizing, "ratio-zero", 100, 0, NM);

            assertThat(sizing.effectiveSize("cheap", -1)).isZero();
            assertThat(sizing.effectiveSize("ratio-zero", 1_000)).isZero();
        }

        @Test
        @DisplayName("headroom 1.0 yields the exact learned values")
        void headroomOneIsExact() {
            final AdaptiveSizing sizing = sizing(config(1.0, Estimator.ALLOCATION));
            learn(sizing, "job", 100, 200, NM);
            learn(sizing, "job", -1, 900, NM);

            assertThat(sizing.effectiveSize("job", 1_000)).isEqualTo(2_000);
            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(900);
        }

        @Test
        @DisplayName("overflowing estimates clamp to Long.MAX_VALUE / 4 — never negative, never wrapped")
        void overflowClamps() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "huge-absolute", -1, Long.MAX_VALUE, NM);
            learn(sizing, "huge-ratio", 1, 4, NM);

            assertThat(sizing.effectiveSize("huge-absolute", -1)).isEqualTo(CLAMP);
            assertThat(sizing.effectiveSize("huge-ratio", Long.MAX_VALUE / 2)).isEqualTo(CLAMP);
            assertThat(sizing.effectiveSize("huge-ratio", Long.MAX_VALUE)).isEqualTo(CLAMP);
        }

        @Test
        @DisplayName("a declared size above the clamp passes through only when nothing was learned")
        void declaredAboveClampPassesThroughWithoutProfile() {
            assertThat(sizing().effectiveSize("never-seen", Long.MAX_VALUE)).isEqualTo(Long.MAX_VALUE);
        }
    }

    @Nested
    @DisplayName("AdaptiveConfig")
    class Config {

        @Test
        @DisplayName("defaults: 16 samples, 1.25 headroom, ALLOCATION, the whole max heap reserved, 10 ms sampler")
        void defaultsArePinned() {
            final AdaptiveConfig defaults = AdaptiveConfig.defaults();

            assertThat(defaults.ringSize()).isEqualTo(16);
            assertThat(defaults.headroom()).isEqualTo(1.25);
            assertThat(defaults.estimator()).isEqualTo(Estimator.ALLOCATION);
            assertThat(defaults.unknownReserveBytes()).isEqualTo(Runtime.getRuntime().maxMemory());
            assertThat(defaults.heapSamplePeriod()).isEqualTo(Duration.ofMillis(10));
            assertThat(defaults.version()).as("unversioned by default").isNull();
            assertThat(defaults.maxSampleAge()).as("samples never expire by default").isNull();
        }

        @Test
        @DisplayName("the five-argument constructor is the seven-argument one with no version and no age limit")
        void shortConstructorMeansUnversionedAndUnbounded() {
            assertThat(new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1, Duration.ZERO))
                    .isEqualTo(new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1, Duration.ZERO, null, null));
        }

        @Test
        @DisplayName("withVersion / withMaxSampleAge return a copy with just that knob changed")
        void withersChangeOneKnob() {
            final AdaptiveConfig base = AdaptiveConfig.defaults();

            final AdaptiveConfig versioned = base.withVersion("2026.08.22");
            assertThat(versioned.version()).isEqualTo("2026.08.22");
            assertThat(versioned.withVersion(null).version()).isNull();
            assertThat(versioned.maxSampleAge()).isNull();
            assertThat(versioned.ringSize()).isEqualTo(base.ringSize());

            final AdaptiveConfig aged = versioned.withMaxSampleAge(Duration.ofDays(30));
            assertThat(aged.maxSampleAge()).isEqualTo(Duration.ofDays(30));
            assertThat(aged.version()).as("the other knob is kept").isEqualTo("2026.08.22");
            assertThat(aged.withMaxSampleAge(null).maxSampleAge()).isNull();
            assertThat(base).as("withers never mutate").isEqualTo(AdaptiveConfig.defaults());
        }

        @Test
        @DisplayName("rejects an empty version (null means unversioned)")
        void rejectsEmptyVersion() {
            assertThatThrownBy(() -> AdaptiveConfig.defaults().withVersion(""))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("version");
        }

        @Test
        @DisplayName("rejects a zero or negative sample age (null means no limit)")
        void rejectsNonPositiveAge() {
            assertThatThrownBy(() -> AdaptiveConfig.defaults().withMaxSampleAge(Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxSampleAge");
            assertThatThrownBy(() -> AdaptiveConfig.defaults().withMaxSampleAge(Duration.ofSeconds(-1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("maxSampleAge");
        }

        @Test
        @DisplayName("rejects a ring smaller than one")
        void rejectsRingBelowOne() {
            assertThatThrownBy(() -> new AdaptiveConfig(0, 1.25, Estimator.ALLOCATION, 1, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("ringSize");
        }

        @Test
        @DisplayName("rejects headroom below one or NaN")
        void rejectsHeadroomBelowOne() {
            assertThatThrownBy(() -> new AdaptiveConfig(16, 0.99, Estimator.ALLOCATION, 1, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("headroom");
            assertThatThrownBy(() -> new AdaptiveConfig(16, Double.NaN, Estimator.ALLOCATION, 1, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("headroom");
        }

        @Test
        @DisplayName("rejects a null estimator")
        void rejectsNullEstimator() {
            assertThatThrownBy(() -> new AdaptiveConfig(16, 1.25, null, 1, Duration.ZERO))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("estimator");
        }

        @Test
        @DisplayName("rejects a negative unknown reserve")
        void rejectsNegativeReserve() {
            assertThatThrownBy(() -> new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, -1, Duration.ZERO))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("unknownReserveBytes");
        }

        @Test
        @DisplayName("rejects a null or negative sample period; ZERO is allowed (sampler off)")
        void rejectsBadSamplePeriod() {
            assertThatThrownBy(() -> new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1, null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("heapSamplePeriod");
            assertThatThrownBy(() -> new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1, Duration.ofMillis(-1)))
                    .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("heapSamplePeriod");
            assertThat(new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1, Duration.ZERO).heapSamplePeriod()).isZero();
        }
    }

    @Nested
    @DisplayName("factories")
    class Factories {

        @Test
        @DisplayName("of() rejects a null store or config")
        void ofRejectsNulls() {
            assertThatThrownBy(() -> AdaptiveSizing.of(null, AdaptiveConfig.defaults())).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> AdaptiveSizing.of(new InMemoryProfileStore(16), null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("of() with a clock rejects a null clock; without one it reads the system clock")
        void ofWithClock() {
            assertThatThrownBy(() -> AdaptiveSizing.of(new InMemoryProfileStore(16), AdaptiveConfig.defaults(), null))
                    .isInstanceOf(NullPointerException.class).hasMessageContaining("clock");
            assertThat(AdaptiveSizing.of(new InMemoryProfileStore(16), AdaptiveConfig.defaults()).clock())
                    .isSameAs(InstantSource.system());
        }

        @Test
        @DisplayName("inMemory() uses the defaults over an in-memory store")
        void inMemoryUsesDefaults() {
            final AdaptiveSizing sizing = AdaptiveSizing.inMemory();

            assertThat(sizing.config()).isEqualTo(AdaptiveConfig.defaults());
            assertThat(sizing.store()).isInstanceOf(InMemoryProfileStore.class);
        }

        @Test
        @DisplayName("of() exposes exactly what it was given, and profile() reads through the store")
        void ofExposesItsArguments() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveConfig config = config(1.5, Estimator.SOLO_HEAP_PEAK);
            final AdaptiveSizing sizing = AdaptiveSizing.of(store, config);

            assertThat(sizing.store()).isSameAs(store);
            assertThat(sizing.config()).isSameAs(config);
            assertThat(sizing.profile("job")).isNull();
            store.record("job", new Sample(-1, 10, NM, 1L));
            assertThat(sizing.profile("job")).isNotNull();
            assertThat(sizing.profile("job").samples()).hasSize(1);
        }
    }
    @Nested
    @DisplayName("freshness — an outdated profile is no profile")
    class Freshness {

        private final AtomicLong now = new AtomicLong(100_000L);
        private final InstantSource clock = () -> Instant.ofEpochMilli(now.get());

        private AdaptiveSizing sizing(AdaptiveConfig config) {
            return AdaptiveSizing.of(new InMemoryProfileStore(config.ringSize()), config, clock);
        }

        private static void learnAt(AdaptiveSizing sizing, String name, long alloc, long at, String version) {
            sizing.store().record(name, new Sample(-1, alloc, NM, at, version));
        }

        @Test
        @DisplayName("older than maxSampleAge: declared passes through, undeclared is UNKNOWN — as if never seen")
        void staleByAgeIsNeverSeen() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withMaxSampleAge(Duration.ofSeconds(10)));
            learnAt(sizing, "job", 800, 80_000L, null);                      // 20 s ago

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
            assertThat(sizing.effectiveSize("job", 1_000)).isEqualTo(1_000);
            assertThat(sizing.profile("job")).as("the raw profile is still there to look at").isNotNull();
        }

        @Test
        @DisplayName("exactly maxSampleAge old still counts; one millisecond more does not")
        void theCutoffIsInclusive() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withMaxSampleAge(Duration.ofSeconds(10)));
            learnAt(sizing, "job", 800, 90_000L, null);

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(1_000);
            now.set(100_001L);
            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
        }

        @Test
        @DisplayName("time moves on: the same profile goes stale without any store activity")
        void timeMovesOn() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withMaxSampleAge(Duration.ofSeconds(10)));
            learnAt(sizing, "job", 800, 99_000L, null);
            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(1_000);

            now.addAndGet(Duration.ofDays(1).toMillis());

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
        }

        @Test
        @DisplayName("measured under another version: as if never seen; the same version: learned")
        void staleByVersionIsNeverSeen() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withVersion("build-2"));
            learnAt(sizing, "job", 800, 99_000L, "build-1");
            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
            assertThat(sizing.effectiveSize("job", 1_000)).isEqualTo(1_000);

            learnAt(sizing, "job", 400, 99_500L, "build-2");

            assertThat(sizing.effectiveSize("job", -1)).as("only the build-2 sample counts").isEqualTo(500);
        }

        @Test
        @DisplayName("an unversioned sample never satisfies a configured version; an unversioned config accepts every sample")
        void unversionedSamplesAndConfigs() {
            final AdaptiveSizing versioned = sizing(config(1.25, Estimator.ALLOCATION).withVersion("build-2"));
            learnAt(versioned, "job", 800, 99_000L, null);
            assertThat(versioned.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);

            final AdaptiveSizing unversioned = sizing(config(1.25, Estimator.ALLOCATION));
            learnAt(unversioned, "job", 800, 99_000L, "build-1");
            learnAt(unversioned, "job", 80, 99_000L, "build-7");
            assertThat(unversioned.effectiveSize("job", -1)).isEqualTo(1_000);
        }

        @Test
        @DisplayName("a partly stale ring: the maximum is over the fresh samples only")
        void partlyStaleRingUsesFreshMaxOnly() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withMaxSampleAge(Duration.ofSeconds(10)));
            learnAt(sizing, "job", 900, 50_000L, null);                      // the old maximum
            learnAt(sizing, "job", 100, 99_000L, null);

            assertThat(sizing.effectiveSize("job", -1)).isEqualTo(125);
        }

        @Test
        @DisplayName("freshProfile() is exactly what effectiveSize reads; profile() is the raw ring")
        void freshProfileIsWhatIsRead() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION).withVersion("b").withMaxSampleAge(Duration.ofSeconds(10)));
            learnAt(sizing, "job", 900, 50_000L, "b");
            learnAt(sizing, "job", 800, 99_000L, "a");
            learnAt(sizing, "job", 100, 99_000L, "b");

            assertThat(sizing.profile("job").samples()).hasSize(3);
            assertThat(sizing.freshProfile("job").samples()).extracting(Sample::allocatedBytes).containsExactly(100L);
            assertThat(sizing.freshProfile("job").lastMeasuredAt()).contains(Instant.ofEpochMilli(99_000L));
            assertThat(sizing.freshProfile("never-seen")).isNull();
            assertThat(sizing.freshProfile(null)).isNull();
        }

        @Test
        @DisplayName("with no version and no age limit, the fresh profile is the raw profile itself")
        void noFiltersMeansTheSameProfile() {
            final AdaptiveSizing sizing = sizing(config(1.25, Estimator.ALLOCATION));
            learnAt(sizing, "job", 900, 1L, "whatever");

            assertThat(sizing.freshProfile("job")).isSameAs(sizing.profile("job"));
        }
    }
}
