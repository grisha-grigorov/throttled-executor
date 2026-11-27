package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.GlobalMemoryBudgetPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The measuring half, driven through a real {@link ThrottledExecutor}: for every
 * <em>named</em> task that completes normally the listener records what the worker thread
 * allocated and — when nothing else ran meanwhile ("solo") — how far heap-in-use rose, and
 * hands the {@link Sample} to the store.
 *
 * <p>Two measurements, two seams: the allocation meter is {@code -1} on virtual-thread
 * workers (the JDK guards {@code getCurrentThreadAllocatedBytes} on {@code isVirtual()}),
 * and the heap reading is only attributable when the task ran alone. Tests compile at 25,
 * so they may ask the worker {@code Thread.isVirtual()}; {@code src/main} never does.</p>
 */
class AdaptiveSizingListenerTests {

    /** Keeps allocated blocks reachable long enough to be measured; cleared per test. */
    static volatile Object sink;

    @AfterEach
    void drain() {
        sink = null;
        assertThat(GlobalMemoryBudgetPolicy.processWideHeapBudget().inFlightBytes())
                .as("a test leaked accounting into the process-wide budget singleton")
                .isZero();
    }

    private static final LongSupplier HEAP_USED = () -> ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getUsed();

    private static AdaptiveConfig config(Duration samplePeriod) {
        return new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 30, samplePeriod);
    }

    /** A listener with the real meters and the given sample period. */
    private static AdaptiveSizingListener listener(ProfileStore store, Duration samplePeriod) {
        return new AdaptiveSizingListener(store, config(samplePeriod), AllocationMeter.platform(), HEAP_USED, InstantSource.system());
    }

    private static ThrottledTask<Runnable> named(String name, long size, Runnable body) {
        return new ThrottledTask<>(body, size, name);
    }

    private static Sample onlySample(ProfileStore store, String name) {
        final TaskProfile profile = store.find(name);
        assertThat(profile).as("a profile for %s", name).isNotNull();
        assertThat(profile.samples()).as("exactly one sample for %s", name).hasSize(1);
        return profile.samples().get(0);
    }

    /** The heap reading of the name's only sample, or -1 when nothing was recorded at all. */
    private static long heapOf(ProfileStore store, String name) {
        final TaskProfile profile = store.find(name);
        if (profile == null) {
            return -1;
        }
        assertThat(profile.samples()).hasSize(1);
        return profile.samples().get(0).soloHeapPeakBytes();
    }

    private static void retain(int bytes) {
        final byte[] block = new byte[bytes];
        block[block.length - 1] = 1;
        sink = block;
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static CompletableFuture<?> firstFuture(ThrottledExecutor root) {
        return root.getFutures().findFirst().orElseThrow().getCompletableFuture();
    }

    private static CompletableFuture<?> lastFuture(ThrottledExecutor root) {
        return root.getFutures().reduce((first, second) -> second).orElseThrow().getCompletableFuture();
    }

    private static void awaitLatch(CountDownLatch latch) {
        try {
            assertThat(latch.await(30, TimeUnit.SECONDS)).as("latch").isTrue();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new AssertionError(e);
        }
    }

    @Nested
    @DisplayName("what gets recorded")
    class Recording {

        @Test
        @DisplayName("a completed named task yields one sample: the declared size, and >= the bytes it allocated on a platform worker")
        void completedNamedTaskYieldsOneSample() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveRecord");
            root.addListener(listener);
            final AtomicBoolean virtualWorker = new AtomicBoolean();

            root.submitThrottled(List.of(named("EightMegabytes", 4_096, () -> {
                virtualWorker.set(Thread.currentThread().isVirtual());
                retain(8 << 20);
            })), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            final Sample sample = onlySample(store, "EightMegabytes");
            assertThat(sample.declaredSize()).isEqualTo(4_096);
            if (AllocationMeter.supported() && !virtualWorker.get()) {
                assertThat(sample.allocatedBytes()).as("platform worker, meter supported").isGreaterThanOrEqualTo(8L << 20);
            } else {
                assertThat(sample.allocatedBytes()).as("virtual worker or unsupported meter").isEqualTo(Sample.NOT_MEASURED);
            }
            assertThat(sample.soloHeapPeakBytes()).as("it ran alone").isGreaterThanOrEqualTo(0);
            assertThat(sample.epochMillis()).isGreaterThan(0);
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("a run whose memory cannot be measured still records how long it took")
        void unmeasurableMemoryStillRecordsDuration() {

            /*
             * Duration learning rode on the memory gate: a sample was recorded only when
             * allocation or a solo heap peak had been measured. On the virtual backend
             * allocation is never available and a heap peak needs a solo run — which a known
             * task never gets again — so duration was learned exactly once, on the quarantine
             * run, and SelectionKey.DURATION ordered on a single cold-start measurement for
             * ever. Wall-clock duration needs neither meter; it is recorded on its own.
             */
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = new AdaptiveSizingListener(
                    store, config(Duration.ZERO), () -> -1L, () -> -1L, InstantSource.system());   // no meters at all
            final ThrottledExecutor root =
                    ThrottledExecutor.withBackend("AdaptiveDurationOnly", ExecutorBackend.CALLER_RUNS);
            root.addListener(listener);

            root.submitThrottled(List.of(named("Timed", 4_096, () -> sleep(20))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            final Sample sample = onlySample(store, "Timed");
            assertThat(sample.allocatedBytes()).isEqualTo(Sample.NOT_MEASURED);
            assertThat(sample.soloHeapPeakBytes()).isEqualTo(Sample.NOT_MEASURED);
            assertThat(sample.durationNanos())
                    .as("the one thing every run can measure")
                    .isGreaterThanOrEqualTo(Duration.ofMillis(20).toNanos());
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("an unnamed plain submission records nothing")
        void unnamedRecordsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveUnnamed");
            root.addListener(listener);

            root.submit((Runnable) () -> retain(1 << 20));
            root.waitForCompletion();

            assertThat(store.snapshot()).isEmpty();
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("the profile is visible the moment the task's future completes (recorded before the future settles)")
        void profileVisibleWhenFutureCompletes() {
            final AdaptiveSizing sizing = AdaptiveSizing.of(new InMemoryProfileStore(16), config(Duration.ZERO));
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveVisible");
            root.addListener(sizing.listener());
            assertThat(sizing.listener()).as("one listener per facade").isSameAs(sizing.listener());

            for (int i = 0; i < 20; i++) {
                root.submitThrottled(List.of(named("Visible", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
                lastFuture(root).join();
                assertThat(sizing.profile("Visible").samples()).as("iteration %d", i).hasSize(Math.min(i + 1, 16));
            }
            root.waitForCompletion();
        }
    }

    @Nested
    @DisplayName("what does not get recorded")
    class NotRecorded {

        @Test
        @DisplayName("a FAILED task records nothing and the in-flight counter returns to zero")
        void failedRecordsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveFailed");
            root.addListener(listener);

            root.submitThrottled(List.of(named("Failing", -1, () -> {
                throw new IllegalStateException("the reactor is leaking");
            })), new MaxConcurrencyPolicy(4));
            try {
                root.waitForCompletion();
            } catch (RuntimeException expected) {
                // the aggregate
            }

            assertThat(store.find("Failing")).isNull();
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("a task cancelled before it started records nothing and never touches the counter")
        void cancelledBeforeStartRecordsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = ThrottledExecutor.withBackend("AdaptiveCancelQueued", ExecutorBackend.FORK_JOIN);
            root.addListener(listener);
            final int available = Runtime.getRuntime().availableProcessors();
            final int poolSize = (available - 2) <= 2 ? available : available - 2;   // mirrors ExecutorBackend
            final CountDownLatch allBlocked = new CountDownLatch(poolSize);
            final CountDownLatch release = new CountDownLatch(1);
            for (int i = 0; i < poolSize; i++) {
                root.submit((Runnable) () -> {
                    allBlocked.countDown();
                    awaitLatch(release);
                });
            }
            awaitLatch(allBlocked);

            final CompletableFuture<Void> queued = root.submit((Runnable) () -> { });
            assertThat(queued.cancel(true)).isTrue();
            release.countDown();
            try {
                root.waitForCompletion();
            } catch (RuntimeException expectedBecauseOfTheCancellation) {
                // unchanged semantics: a cancelled batch is not a clean batch
            }

            assertThat(store.snapshot()).isEmpty();
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("a task cancelled after it started records nothing and still balances the counter")
        void cancelledAfterStartRecordsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveCancelRunning");
            root.addListener(listener);
            final CountDownLatch started = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            root.submitThrottled(List.of(named("CancelledRunning", -1, () -> {
                started.countDown();
                awaitLatch(release);
            })), new MaxConcurrencyPolicy(4));
            awaitLatch(started);
            assertThat(listener.inFlight()).isEqualTo(1);
            firstFuture(root).cancel(true);
            release.countDown();
            try {
                root.waitForCompletion();
            } catch (RuntimeException expectedBecauseOfTheCancellation) {
                // as above
            }

            await().atMost(Duration.ofSeconds(10)).untilAsserted(() -> assertThat(listener.inFlight()).isZero());
            assertThat(store.find("CancelledRunning")).isNull();
        }

        @Test
        @DisplayName("a rejected submission records nothing")
        void rejectedRecordsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveRejected");
            root.addListener(listener);
            root.close();

            final CompletableFuture<Void> rejected = root.submit((Runnable) () -> { });

            assertThat(rejected.isCompletedExceptionally()).isTrue();
            assertThat(store.snapshot()).isEmpty();
            assertThat(listener.inFlight()).isZero();
        }
    }

    @Nested
    @DisplayName("solo detection")
    class Solo {

        @Test
        @DisplayName("a task that ran alone gets a heap reading")
        void loneTaskIsSolo() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveLone");
            root.addListener(listener(store, Duration.ZERO));

            root.submitThrottled(List.of(named("Lone", -1, () -> retain(1 << 20))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            assertThat(heapOf(store, "Lone")).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("an unnamed plain submission overlapping a named task breaks its solo-ness")
        void unnamedOverlapBreaksSolo() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveOverlapUnnamed");
            root.addListener(listener);
            final CountDownLatch namedStarted = new CountDownLatch(1);
            final CountDownLatch plainStarted = new CountDownLatch(1);
            final CountDownLatch release = new CountDownLatch(1);

            root.submitThrottled(List.of(named("Overlapped", -1, () -> {
                namedStarted.countDown();
                awaitLatch(release);
                retain(1 << 20);
            })), new MaxConcurrencyPolicy(4));
            awaitLatch(namedStarted);
            root.submit((Runnable) plainStarted::countDown);
            awaitLatch(plainStarted);
            release.countDown();
            root.waitForCompletion();

            assertThat(heapOf(store, "Overlapped")).as("not solo: no heap reading (or no sample at all on a virtual worker)").isEqualTo(-1);
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("two overlapping named tasks: neither is solo")
        void twoOverlappingNamedAreNotSolo() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveOverlapBoth");
            root.addListener(listener(store, Duration.ZERO));
            final CountDownLatch bothStarted = new CountDownLatch(2);

            root.submitThrottled(List.of(
                    named("First", -1, () -> {
                        bothStarted.countDown();
                        awaitLatch(bothStarted);
                    }),
                    named("Second", -1, () -> {
                        bothStarted.countDown();
                        awaitLatch(bothStarted);
                    })), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            assertThat(heapOf(store, "First")).isEqualTo(-1);
            assertThat(heapOf(store, "Second")).isEqualTo(-1);
        }

        @Test
        @DisplayName("solo-ness is per run: the same name is solo again once it runs alone")
        void soloIsPerRun() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveSoloAgain");
            root.addListener(listener(store, Duration.ZERO));
            final CountDownLatch bothStarted = new CountDownLatch(2);

            root.submitThrottled(List.of(
                    named("Again", -1, () -> {
                        bothStarted.countDown();
                        awaitLatch(bothStarted);
                    }),
                    named("Other", -1, () -> {
                        bothStarted.countDown();
                        awaitLatch(bothStarted);
                    })), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();
            root.submitThrottled(List.of(named("Again", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            final TaskProfile profile = store.find("Again");
            assertThat(profile).isNotNull();
            final List<Sample> samples = profile.samples();
            assertThat(samples.get(samples.size() - 1).soloHeapPeakBytes()).as("second run, alone").isGreaterThanOrEqualTo(0);
            if (samples.size() == 2) {
                assertThat(samples.get(0).soloHeapPeakBytes()).as("first run, overlapped").isEqualTo(-1);
            }
        }
    }

    @Nested
    @DisplayName("the heap sampler")
    class Sampler {

        @Test
        @DisplayName("with a 1 ms period, a 32 MB block held for 50 ms and dropped shows up as a peak of at least 16 MB")
        void samplerCatchesATransientPeak() throws Exception {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ofMillis(1));
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveSampler");
            root.addListener(listener);
            try {
                // Earlier tests leave garbage behind; a collection triggered by the 32 MB allocation that
                // reclaims more than 32 MB would hide the block entirely (every reading below the start,
                // nothing built afterwards — the one case HeapRise cannot see). Clear the decks first.
                System.gc();
                root.submitThrottled(List.of(named("Transient", -1, () -> {
                    retain(32 << 20);
                    sleep(50);
                    sink = null;
                })), new MaxConcurrencyPolicy(4));
                root.waitForCompletion();

                assertThat(onlySample(store, "Transient").soloHeapPeakBytes()).isGreaterThanOrEqualTo(16L << 20);
                assertThat(samplerThread()).as("the sampler thread exists after a solo run").isNotNull();
            } finally {
                listener.close();
            }
        }

        @Test
        @DisplayName("a ZERO period means no sampler thread; the inline start/end readings remain")
        void zeroPeriodMeansNoThread() throws Exception {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveNoSampler");
            root.addListener(listener);

            root.submitThrottled(List.of(named("Inline", -1, () -> retain(1 << 20))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            assertThat(onlySample(store, "Inline").soloHeapPeakBytes()).isGreaterThanOrEqualTo(0);
            assertThat(samplerThread()).isNull();
            listener.close();
        }

        @Test
        @DisplayName("close() stops the sampler thread; tasks afterwards are still measured inline")
        void closeStopsTheSampler() throws Exception {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = listener(store, Duration.ofMillis(1));
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveCloseSampler");
            root.addListener(listener);
            root.submitThrottled(List.of(named("BeforeClose", -1, () -> sleep(5))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();
            final Thread sampler = samplerThread();
            assertThat(sampler).isNotNull();

            listener.close();

            await().atMost(Duration.ofSeconds(10)).until(() -> !sampler.isAlive());
            root.submitThrottled(List.of(named("AfterClose", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();
            assertThat(onlySample(store, "AfterClose").soloHeapPeakBytes()).isGreaterThanOrEqualTo(0);
            assertThat(samplerThread()).as("not restarted").isNull();
        }

        private Thread samplerThread() {
            for (Thread t : Thread.getAllStackTraces().keySet()) {
                if (t.getName().equals(HeapSampler.THREAD_NAME) && t.isAlive()) {
                    return t;
                }
            }
            return null;
        }
    }

    @Nested
    @DisplayName("robustness")
    class Robustness {

        @Test
        @DisplayName("throwing meters never reach the task: it completes, nothing is recorded, the counter balances")
        void throwingMetersAreContained() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final LongSupplier broken = () -> {
                throw new IllegalStateException("no meter today");
            };
            final AdaptiveSizingListener listener = new AdaptiveSizingListener(store, config(Duration.ZERO), broken, broken, InstantSource.system());
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveBrokenMeters");
            root.addListener(listener);

            root.submitThrottled(List.of(named("Unmeasurable", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            assertThat(firstFuture(root).isCompletedExceptionally()).isFalse();
            // The meters threw, so no memory was measured — but the duration needs no meter.
            assertThat(store.find("Unmeasurable").samples())
                    .as("no memory measurement got through; only the timing was recorded")
                    .hasSize(1)
                    .noneMatch(Sample::measuresMemory)
                    .allMatch(sample -> sample.durationNanos() >= 0);
            assertThat(listener.inFlight()).isZero();
        }

        @Test
        @DisplayName("a meter that reports -1 leaves that measurement out; the other still lands")
        void minusOneMeterIsLeftOut() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = new AdaptiveSizingListener(store, config(Duration.ZERO), () -> -1L, HEAP_USED, InstantSource.system());
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveHalfMeter");
            root.addListener(listener);

            root.submitThrottled(List.of(named("HeapOnly", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            final Sample sample = onlySample(store, "HeapOnly");
            assertThat(sample.allocatedBytes()).isEqualTo(Sample.NOT_MEASURED);
            assertThat(sample.soloHeapPeakBytes()).isGreaterThanOrEqualTo(0);
        }

        @Test
        @DisplayName("a store that throws from record() is contained too")
        void throwingStoreIsContained() {
            final ProfileStore broken = new ProfileStore() {
                @Override
                public TaskProfile find(String name) {
                    return null;
                }

                @Override
                public void record(String name, Sample sample) {
                    throw new IllegalStateException("disk on fire");
                }

                @Override
                public java.util.Map<String, TaskProfile> snapshot() {
                    return java.util.Map.of();
                }

                @Override
                public void save() {
                }
            };
            final AdaptiveSizingListener listener = listener(broken, Duration.ZERO);
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveBrokenStore");
            root.addListener(listener);

            root.submitThrottled(List.of(named("Unstorable", -1, () -> retain(1 << 16))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            assertThat(firstFuture(root).isCompletedExceptionally()).isFalse();
            assertThat(listener.inFlight()).isZero();
        }
    }

    @Nested
    @DisplayName("the facade")
    class Facade {

        @Test
        @DisplayName("close() saves the store and stops the sampler")
        void closeSavesAndStops() throws Exception {
            final AtomicBoolean saved = new AtomicBoolean();
            final ProfileStore store = new ProfileStore() {
                private final InMemoryProfileStore inner = new InMemoryProfileStore(16);

                @Override
                public TaskProfile find(String name) {
                    return inner.find(name);
                }

                @Override
                public void record(String name, Sample sample) {
                    inner.record(name, sample);
                }

                @Override
                public java.util.Map<String, TaskProfile> snapshot() {
                    return inner.snapshot();
                }

                @Override
                public void save() {
                    saved.set(true);
                }
            };
            final AdaptiveSizing sizing = AdaptiveSizing.of(store, config(Duration.ofMillis(1)));
            final ThrottledExecutor root = new ThrottledExecutor("AdaptiveFacadeClose");
            root.addListener(sizing.listener());
            root.submitThrottled(List.of(named("Closing", -1, () -> sleep(5))), new MaxConcurrencyPolicy(4));
            root.waitForCompletion();

            sizing.close();

            assertThat(saved).isTrue();
            await().atMost(Duration.ofSeconds(10)).until(() -> {
                for (Thread t : Thread.getAllStackTraces().keySet()) {
                    if (t.getName().equals(HeapSampler.THREAD_NAME) && t.isAlive()) {
                        return false;
                    }
                }
                return true;
            });
        }

        @Test
        @DisplayName("close() on a facade whose listener was never created just saves")
        void closeWithoutListenerJustSaves() throws Exception {
            final AdaptiveSizing sizing = AdaptiveSizing.inMemory();

            sizing.close();

            assertThat(sizing.profile("anything")).isNull();
        }
    }

    /**
     * The listener driven directly — {@code onSubmit}/{@code onStart}/{@code onComplete} with
     * hand-built descriptors and outcomes — over <em>fake</em> meters that return exactly the
     * readings a test sets. This is where the ordering contracts live: what a run is allowed
     * to store when another run overlapped it, what it stores after {@code close()}, and
     * which readings it takes at all. None of that is deterministic through a real executor.
     */
    @Nested
    @DisplayName("ordering, driven directly over fake meters")
    class Ordering {

        /** Exact readings, and a count of how often heap was read. */
        final class FakeMeters {
            final AtomicLong alloc = new AtomicLong();
            final AtomicLong heap = new AtomicLong(100);
            final AtomicInteger heapReads = new AtomicInteger();

            LongSupplier allocSupplier() {
                return alloc::get;
            }

            LongSupplier heapSupplier() {
                return () -> {
                    heapReads.incrementAndGet();
                    return heap.get();
                };
            }
        }

        private final ThrottledExecutor root = new ThrottledExecutor("AdaptiveOrdering");
        private final AtomicLong sequence = new AtomicLong();

        private TaskDescriptor descriptor(String name, long size) {
            return new TaskDescriptor(root, sequence.incrementAndGet(), name, size, System.nanoTime(), System.currentTimeMillis());
        }

        private TaskOutcome completed() {
            final long now = System.nanoTime();
            return new TaskOutcome(TaskStatus.COMPLETED, null, now - 1, now);
        }

        private AdaptiveSizingListener listener(ProfileStore store, Duration samplePeriod, FakeMeters meters) {
            return new AdaptiveSizingListener(store, config(samplePeriod), meters.allocSupplier(), meters.heapSupplier(), InstantSource.system());
        }

        @Test
        @DisplayName("a sample is stamped with the configured version and the clock's time at completion")
        void sampleIsStampedWithVersionAndClock() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AtomicLong now = new AtomicLong(1_000_000L);
            final AdaptiveConfig config = config(Duration.ZERO).withVersion("build-42");
            final AdaptiveSizingListener listener = new AdaptiveSizingListener(
                    store, config, meters.allocSupplier(), meters.heapSupplier(), () -> Instant.ofEpochMilli(now.get()));

            final TaskDescriptor a = descriptor("A", -1);
            final AdaptiveSizingListener.Run run = listener.onSubmit(a);
            listener.onStart(a, run);
            meters.alloc.addAndGet(10);
            now.set(1_000_500L);                                  // the clock moved while the body ran
            listener.onComplete(a, run, completed());

            final Sample sample = onlySample(store, "A");
            assertThat(sample.version()).isEqualTo("build-42");
            assertThat(sample.epochMillis()).as("stamped at completion, from the injected clock").isEqualTo(1_000_500L);
            assertThat(store.find("A").lastMeasuredAt()).contains(Instant.ofEpochMilli(1_000_500L));
        }

        @Test
        @DisplayName("an unversioned configuration stamps no version")
        void unversionedConfigStampsNothing() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO, meters);

            final TaskDescriptor a = descriptor("A", -1);
            final AdaptiveSizingListener.Run run = listener.onSubmit(a);
            listener.onStart(a, run);
            meters.alloc.addAndGet(10);
            listener.onComplete(a, run, completed());

            assertThat(onlySample(store, "A").version()).isNull();
        }

        @Test
        @DisplayName("an unknown whose run measured no memory is warned about once — and its duration is still stored")
        void unmeasurableUnknownWarnsOnce() {

            /*
             * Virtual backend: no allocation counter, and something else was in flight, so the
             * quarantine run taught the sizing nothing and the policy will quarantine it again
             * next batch. The duration is recorded regardless — it is a real measurement — but
             * a run that leaves the task unknown must say so once per name, or the serialised
             * run repeats on every batch in silence. The gate that used to guard this warning
             * was always taken by the duration branch, so it never fired.
             */
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final AdaptiveSizingListener listener = new AdaptiveSizingListener(
                    store, config(Duration.ZERO), () -> -1L, () -> -1L, InstantSource.system());

            final TaskDescriptor a = descriptor("A", -1);
            final TaskDescriptor b = descriptor("B", 10);
            final AdaptiveSizingListener.Run runA = listener.onSubmit(a);
            final AdaptiveSizingListener.Run runB = listener.onSubmit(b);
            listener.onStart(a, runA);
            listener.onStart(b, runB);                              // overlaps A: nothing is solo
            listener.onComplete(a, runA, completed());
            listener.onComplete(b, runB, completed());

            assertThat(onlySample(store, "A").measuresMemory()).as("duration only").isFalse();
            assertThat(onlySample(store, "A").durationNanos()).isNotNegative();
            assertThat(listener.warnedUnlearned("A")).as("A is still unknown: said so").isTrue();
            assertThat(listener.warnedUnlearned("B")).as("B has a declared size: nothing to learn, nothing to say").isFalse();

            // A memory measurement makes A known; a later unmeasurable run is no longer worth a warning.
            final AdaptiveSizingListener fresh = new AdaptiveSizingListener(
                    store, config(Duration.ZERO), () -> -1L, () -> -1L, InstantSource.system());
            store.record("A", new Sample(-1, 800, Sample.NOT_MEASURED, 1L));
            final AdaptiveSizingListener.Run again = fresh.onSubmit(a);
            final AdaptiveSizingListener.Run other = fresh.onSubmit(b);
            fresh.onStart(a, again);
            fresh.onStart(b, other);
            fresh.onComplete(a, again, completed());
            fresh.onComplete(b, other, completed());

            assertThat(fresh.warnedUnlearned("A")).as("known now: the warning is about staying unknown").isFalse();
        }

        @Test
        @DisplayName("an overlapped run never stores a heap peak — not even 0 — whichever of the two ends first")
        void overlappedRunsStoreNoHeapPeak() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AdaptiveSizingListener listener = listener(store, Duration.ofMillis(1), meters);
            try {
                final TaskDescriptor a = descriptor("A", -1);
                final TaskDescriptor b = descriptor("B", -1);
                final AdaptiveSizingListener.Run runA = listener.onSubmit(a);
                final AdaptiveSizingListener.Run runB = listener.onSubmit(b);

                listener.onStart(a, runA);                       // solo: a sampler session opens
                meters.heap.set(120);
                listener.onStart(b, runB);                       // overlaps A: not solo
                meters.heap.set(164);
                meters.alloc.set(10);
                listener.onComplete(a, runA, completed());       // A ends while B runs
                listener.onComplete(b, runB, completed());

                assertThat(onlySample(store, "A").allocatedBytes()).isEqualTo(10);
                assertThat(heapOf(store, "A")).as("B started before A ended").isEqualTo(-1);
                assertThat(heapOf(store, "B")).as("A was in flight when B started").isEqualTo(-1);
                assertThat(listener.inFlight()).isZero();
            } finally {
                listener.close();
            }
        }

        @Test
        @DisplayName("two runs that start together are never both solo — the in-flight count and the start sequence agree")
        void simultaneousStartsAreNeverSolo() throws Exception {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            meters.alloc.set(-1);                                // only a solo heap peak could ever be stored
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO, meters);
            final int rounds = 20_000;
            final CyclicBarrier barrier = new CyclicBarrier(2);
            final ExecutorService pair = Executors.newFixedThreadPool(2);
            try {
                final Future<?> left = pair.submit(() -> race(listener, "Left", barrier, rounds));
                final Future<?> right = pair.submit(() -> race(listener, "Right", barrier, rounds));
                left.get(2, TimeUnit.MINUTES);
                right.get(2, TimeUnit.MINUTES);
            } finally {
                pair.shutdownNow();
                listener.close();
            }

            // Durations are recorded on every run now; what must NOT be here is a heap peak
            // taken while the other run was overlapping — that is what solo-ness protects.
            for (String name : List.of("Left", "Right")) {
                final TaskProfile profile = store.find(name);
                if (profile != null) {
                    assertThat(profile.samples())
                            .as("a memory sample here is a heap peak stored for an overlapped run")
                            .noneMatch(Sample::measuresMemory);
                }
            }
            assertThat(listener.inFlight()).isZero();
        }

        private void race(AdaptiveSizingListener listener, String name, CyclicBarrier barrier, int rounds) {
            try {
                for (int i = 0; i < rounds; i++) {
                    final TaskDescriptor task = descriptor(name, -1);
                    final AdaptiveSizingListener.Run run = listener.onSubmit(task);
                    barrier.await();
                    listener.onStart(task, run);
                    barrier.await();                             // both are in flight before either ends
                    listener.onComplete(task, run, completed());
                }
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }

        @Test
        @DisplayName("heap is read only for a solo run: an overlapped run's readings could never be used")
        void heapIsReadOnlyForSoloRuns() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO, meters);
            final TaskDescriptor a = descriptor("A", -1);
            final TaskDescriptor b = descriptor("B", -1);
            final AdaptiveSizingListener.Run runA = listener.onSubmit(a);
            final AdaptiveSizingListener.Run runB = listener.onSubmit(b);

            listener.onStart(a, runA);                           // solo: reads heap
            listener.onStart(b, runB);                           // overlapped: must not
            listener.onComplete(b, runB, completed());
            listener.onComplete(a, runA, completed());

            assertThat(meters.heapReads.get()).as("A's start and end only").isEqualTo(2);
        }

        @Test
        @DisplayName("a declared size below -1 is recorded as -1, not rejected on every completion")
        void declaredSizeBelowMinusOneIsClamped() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AdaptiveSizingListener listener = listener(store, Duration.ZERO, meters);
            final TaskDescriptor odd = descriptor("Odd", -2);
            final AdaptiveSizingListener.Run run = listener.onSubmit(odd);

            listener.onStart(odd, run);
            meters.alloc.set(10);
            listener.onComplete(odd, run, completed());

            final Sample sample = onlySample(store, "Odd");
            assertThat(sample.declaredSize()).isEqualTo(-1);
            assertThat(sample.allocatedBytes()).isEqualTo(10);
        }

        @Test
        @DisplayName("after close() a solo run is measured inline from its start and end readings")
        void afterCloseSoloRunsAreMeasuredInline() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            final FakeMeters meters = new FakeMeters();
            final AdaptiveSizingListener listener = listener(store, Duration.ofMillis(1), meters);
            listener.close();

            final TaskDescriptor a = descriptor("A", -1);
            final AdaptiveSizingListener.Run run = listener.onSubmit(a);
            listener.onStart(a, run);
            meters.heap.set(164);
            listener.onComplete(a, run, completed());

            assertThat(heapOf(store, "A")).as("inline: 164 - 100, not the closed sampler's answer").isEqualTo(64);
        }
    }
}
