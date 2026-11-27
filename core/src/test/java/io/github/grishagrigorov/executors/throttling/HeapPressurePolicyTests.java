package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The reactive policy: it looks at how full the heap actually is after collection, rather than at
 * what the batch declared it would need.
 */
@DisplayName("HeapPressurePolicy")
class HeapPressurePolicyTests {

    @Nested
    @DisplayName("the threshold")
    class Threshold {

        @Test
        @DisplayName("admits below the high mark and refuses above it")
        void refusesAboveHigh() {

            final FakeHeap heap = new FakeHeap(0.50);
            final HeapPressurePolicy policy = policy(heap, 0.85, 0.75);

            assertThat(policy.admit(task(), snapshotOf(1, 0))).isTrue();

            heap.pressure = 0.90;
            assertThat(policy.admit(task(), snapshotOf(1, 0))).isFalse();
        }

        @Test
        @DisplayName("hysteresis: once refusing, it keeps refusing until the heap drops below the LOW mark")
        void hysteresis() {

            final FakeHeap heap = new FakeHeap(0.90);
            final HeapPressurePolicy policy = policy(heap, 0.85, 0.75);

            assertThat(policy.admit(task(), snapshotOf(1, 0))).isFalse();

            // Back under `high` but not under `low`: still refusing. Without this the policy
            // would chatter — admit, push the heap back over, refuse, repeat.
            heap.pressure = 0.80;
            assertThat(policy.admit(task(), snapshotOf(1, 0))).isFalse();

            heap.pressure = 0.74;
            assertThat(policy.admit(task(), snapshotOf(1, 0))).isTrue();
        }

        @Test
        @DisplayName("reports the reading it last acted on")
        void reportsLastReading() {

            final FakeHeap heap = new FakeHeap(0.90);
            final HeapPressurePolicy policy = policy(heap, 0.85, 0.75);

            policy.admit(task(), snapshotOf(1, 0));

            assertThat(policy.lastPressure()).isEqualTo(0.90);
            assertThat(policy.isUnderPressure()).isTrue();
        }
    }

    @Nested
    @DisplayName("sampling")
    class Sampling {

        @Test
        @DisplayName("the heap is read at most once per sample period, however often admit is called")
        void memoisesWithinThePeriod() {

            final FakeHeap heap = new FakeHeap(0.10);
            final AtomicLong now = new AtomicLong(1_000L);
            final HeapPressurePolicy policy = new HeapPressurePolicy(
                    0.85, 0.75, Duration.ofMillis(50), heap, () -> Instant.ofEpochMilli(now.get()));

            for (int i = 0; i < 20; i++) {
                policy.admit(task(), snapshotOf(1, 0));
            }
            assertThat(heap.reads).hasValue(1);

            now.addAndGet(49);
            policy.admit(task(), snapshotOf(1, 0));
            assertThat(heap.reads).as("still inside the period").hasValue(1);

            now.addAndGet(1);
            policy.admit(task(), snapshotOf(1, 0));
            assertThat(heap.reads).as("the period elapsed").hasValue(2);
        }

        @Test
        @DisplayName("a clock that steps backwards re-reads rather than freezing the memo")
        void backwardsClockStepDoesNotFreezeTheMemo() {

            /*
             * The memo is "reuse the reading until samplePeriod has elapsed", measured on an
             * InstantSource — wall-clock time by default, which steps backwards on an NTP
             * correction, a manual change or a VM resume. After a step back, now - lastRead is
             * negative and stays under the period until the clock catches up: the reading, and
             * with it a latched "under pressure", was frozen for the whole size of the step,
             * even though the heap had recovered at once.
             *
             * A step backwards cannot be told apart from "the period has not elapsed" except by
             * its sign, so a backwards step is treated as elapsed. The memo can then only ever
             * under-cache on a bad clock, never over-cache.
             */
            final FakeHeap heap = new FakeHeap(0.10);
            final AtomicLong now = new AtomicLong(10_000L);
            final HeapPressurePolicy policy = new HeapPressurePolicy(
                    0.85, 0.75, Duration.ofMillis(50), heap, () -> Instant.ofEpochMilli(now.get()));

            policy.admit(task(), snapshotOf(1, 0));
            assertThat(heap.reads).hasValue(1);

            now.set(5_000L);                    // the clock went back five seconds
            policy.admit(task(), snapshotOf(1, 0));

            assertThat(heap.reads)
                    .as("a backwards step is not a reason to keep a stale reading")
                    .hasValue(2);
        }

        @Test
        @DisplayName("a zero period reads every time — for callers who would rather pay than lag")
        void zeroPeriodAlwaysReads() {

            final FakeHeap heap = new FakeHeap(0.10);
            final HeapPressurePolicy policy = new HeapPressurePolicy(
                    0.85, 0.75, Duration.ZERO, heap, InstantSource.system());

            policy.admit(task(), snapshotOf(1, 0));
            policy.admit(task(), snapshotOf(1, 0));
            policy.admit(task(), snapshotOf(1, 0));

            assertThat(heap.reads).hasValue(3);
        }
    }

    @Nested
    @DisplayName("an unreadable heap")
    class Unreadable {

        @Test
        @DisplayName("a NaN reading means unknown, and unknown means admit — not 'whatever it was last time'")
        void nanReadingIsUnknown() {

            /*
             * A negative reading was already read as "the reader could not tell" and cleared a
             * latched refusal. NaN was not: every comparison against NaN is false, so it fell
             * through both threshold checks and left underPressure exactly as it was. A custom
             * HeapReader that computes 0.0/0.0 (an empty pool, a zero max) while the policy was
             * refusing then refused for the life of the JVM — the latch the documented reading
             * says cannot happen.
             */
            final FakeHeap heap = new FakeHeap(0.95);
            final HeapPressurePolicy policy =
                    new HeapPressurePolicy(0.85, 0.75, Duration.ZERO, heap, InstantSource.system());

            assertThat(policy.admit(task(), snapshotOf(1, 0))).as("latched under pressure").isFalse();

            heap.pressure = Double.NaN;

            assertThat(policy.admit(task(), snapshotOf(1, 0)))
                    .as("the reader cannot tell; that is not a reason to keep refusing")
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("liveness")
    class Liveness {

        @Test
        @DisplayName("it never claims an in-flight count, so the harness floor still admits one task under pressure")
        void doesNotHijackTheFloor() {

            final HeapPressurePolicy policy = policy(new FakeHeap(0.99), 0.85, 0.75);

            assertThat(policy.inFlightTaskCount())
                    .as("-1 leaves the floor reading the harness's own count")
                    .isEqualTo(-1);

            // The floor is what turns sustained pressure into serial execution rather than a
            // deadlock: with nothing running, the next candidate goes in regardless.
            final ThrottleHarness harness = new ThrottleHarness();
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(task(), task()));

            assertThat(harness.admitFirst(queue, policy)).as("admitted by the floor").isNotNull();
            assertThat(harness.admitFirst(queue, policy)).as("but only one at a time").isNull();
        }
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("of(high) derives a low mark ten points below")
        void ofDerivesLow() {

            final HeapPressurePolicy policy = HeapPressurePolicy.of(0.80);

            assertThat(policy.high()).isEqualTo(0.80);
            assertThat(policy.low()).isCloseTo(0.70, org.assertj.core.data.Offset.offset(1e-9));
        }

        @Test
        @DisplayName("defaults() is 0.85 / 0.75")
        void defaults() {

            assertThat(HeapPressurePolicy.defaults().high()).isEqualTo(0.85);
            assertThat(HeapPressurePolicy.defaults().low()).isCloseTo(0.75, org.assertj.core.data.Offset.offset(1e-9));
        }

        @Test
        @DisplayName("a threshold below the hysteresis band still leaves room to recover, rather than latching off")
        void smallThresholdsDoNotLatch() {

            // low would be <= 0 with a flat ten-point band, and occupancy never falls below
            // zero — so the first refusal would be permanent.
            final HeapPressurePolicy policy = HeapPressurePolicy.of(0.05);

            assertThat(policy.low()).isPositive().isLessThan(policy.high());
        }

        @Test
        @DisplayName("a threshold just ABOVE the band width does not latch either")
        void thresholdsJustAboveTheBandDoNotLatch() {

            /*
             * The latch guard caught high <= 0.10 and nothing else. of(0.11) therefore derived
             * low = 0.01: post-collection occupancy on a live JVM essentially never drops below
             * one percent of max heap, so the first refusal was the last admission for the life
             * of the JVM — precisely the failure the guard's own javadoc describes. The band has
             * to be proportional until a flat ten points leaves a low mark that occupancy can
             * actually fall under.
             */
            final HeapPressurePolicy policy = HeapPressurePolicy.of(0.11);

            assertThat(policy.low())
                    .as("half of high, not one point above zero")
                    .isCloseTo(0.055, org.assertj.core.data.Offset.offset(1e-9));

            assertThat(HeapPressurePolicy.of(0.20).low())
                    .as("the two rules meet where a flat band first leaves real room")
                    .isCloseTo(0.10, org.assertj.core.data.Offset.offset(1e-9));
        }

        @Test
        @DisplayName("nonsensical thresholds are rejected at construction, not silently clamped")
        void rejectsBadThresholds() {

            assertThatThrownBy(() -> HeapPressurePolicy.of(0.0)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> HeapPressurePolicy.of(1.5)).isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> new HeapPressurePolicy(0.5, 0.6, Duration.ZERO,
                    new FakeHeap(0.1), InstantSource.system()))
                    .as("low above high would never release")
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("against the real JVM")
    class RealJvm {

        @Test
        @DisplayName("the platform reader returns a usable fraction on this collector")
        void platformReaderWorks() {

            // Collector-agnostic by construction: every heap pool on every collector this
            // project runs (generational Shenandoah, G1, ZGC) reports collection usage.
            System.gc();
            final double pressure = HeapPressurePolicy.defaults().readPressure();

            assertThat(pressure).isBetween(0.0, 1.0);
        }
    }

    private static HeapPressurePolicy policy(HeapReader reader, double high, double low) {
        // Zero period: these tests are about the decision, not the memoisation.
        return new HeapPressurePolicy(high, low, Duration.ZERO, reader, InstantSource.system());
    }

    private static ThrottledTask<Runnable> task() {
        return ThrottledTask.of(() -> { }, 1_000L, "T");
    }

    private static PoolSnapshot snapshotOf(int runningTasks, long trackedSizeBytes) {
        return new PoolSnapshot() {
            @Override
            public int runningTasks() {
                return runningTasks;
            }

            @Override
            public long trackedSizeBytes() {
                return trackedSizeBytes;
            }
        };
    }

    /** A heap whose occupancy the test sets directly, counting how often it is asked. */
    private static final class FakeHeap implements HeapReader {

        final AtomicInteger reads = new AtomicInteger();
        volatile double pressure;

        FakeHeap(double pressure) {
            this.pressure = pressure;
        }

        @Override
        public double occupancyFraction() {
            reads.incrementAndGet();
            return pressure;
        }
    }
}
