package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.CompositePolicy;
import io.github.grishagrigorov.executors.throttling.GlobalMemoryBudgetPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The whole loop through real executors: listener and policy from one {@link AdaptiveSizing},
 * a first batch that learns, a second that is throttled on what was learned. The bodies
 * retain real memory so the measurements are real; the assertions are about the
 * <em>mechanism</em> (serial then parallel, ratio learned, survives a restart), with bounds
 * loose enough for both measurement paths — exact allocation on ForkJoin workers, solo heap
 * peaks on virtual ones.
 */
class AdaptiveEndToEndTests {

    private static final long MB = 1L << 20;

    /** Keeps allocated blocks reachable until the body ends; cleared per test. */
    static volatile Object sink;

    @AfterEach
    void drain() {
        sink = null;
        assertThat(GlobalMemoryBudgetPolicy.processWideHeapBudget().inFlightBytes())
                .as("a test leaked accounting into the process-wide budget singleton")
                .isZero();
    }

    /** Counts overlapping bodies. */
    private static final class Concurrency {
        final AtomicInteger current = new AtomicInteger();
        final AtomicInteger peak = new AtomicInteger();

        Runnable wrap(Runnable body) {
            return () -> {
                final int now = current.incrementAndGet();
                peak.accumulateAndGet(now, Math::max);
                try {
                    body.run();
                } finally {
                    current.decrementAndGet();
                }
            };
        }
    }

    private static AdaptiveConfig config() {
        return new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 30, Duration.ZERO);
    }

    private static void retainAndLinger(long bytes, long millis) {
        final byte[] block = new byte[(int) bytes];
        block[block.length - 1] = 1;
        sink = block;
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static boolean virtual(ThrottledExecutor root) {
        return root.backend() == ExecutorBackend.VIRTUAL_THREAD_PER_TASK;
    }

    /** Workers the ForkJoin backend actually has (mirrors ExecutorBackend); unbounded on virtual. */
    private static int workers(ThrottledExecutor root) {
        if (virtual(root)) {
            return Integer.MAX_VALUE;
        }
        final int available = Runtime.getRuntime().availableProcessors();
        return (available - 2) <= 2 ? available : available - 2;
    }

    @Test
    @DisplayName("learning loop: eight unknown tasks run one at a time; the same eight names run in parallel on the second batch")
    void unknownsRunSeriallyThenLearnedNamesRunInParallel() throws Exception {
        final AdaptiveSizing adaptive = AdaptiveSizing.of(new InMemoryProfileStore(16), config());
        final ThrottledExecutor root = new ThrottledExecutor("AdaptiveLearningLoop");
        root.addListener(adaptive.listener());
        final AdmissionPolicy policy = adaptive.policy(CompositePolicy.allOf(new GlobalMemoryBudgetPolicy(64 * MB, 1.0), new MaxConcurrencyPolicy(4)));
        final List<String> names = new ArrayList<>();
        for (int i = 0; i < 8; i++) {
            names.add("report-" + i);
        }

        final Concurrency first = new Concurrency();
        root.submitThrottled(batch(names, -1, first, () -> retainAndLinger(MB, 20)), policy);
        root.waitForCompletion();

        assertThat(first.peak).as("every task was unknown: quarantined, one at a time").hasValue(1);
        for (String name : names) {
            final long effective = adaptive.effectiveSize(name, -1);
            assertThat(effective).as("%s learned", name).isNotEqualTo(AdaptiveSizing.UNKNOWN);
            if (!virtual(root)) {
                assertThat(effective).as("%s: 1 MB allocated, measured exactly on a platform worker", name).isGreaterThanOrEqualTo(MB);
            }
            assertThat(effective).as("%s: nowhere near the 64 MB budget", name).isLessThan(32 * MB);
        }

        final Concurrency second = new Concurrency();
        root.submitThrottled(batch(names, -1, second, () -> retainAndLinger(MB, 20)), policy);
        root.waitForCompletion();

        assertThat(second.peak.get()).as("known now: admitted by the budget, several at once").isGreaterThan(1);
        assertThat(second.peak.get()).isLessThanOrEqualTo(4);
        adaptive.close();
    }

    @Test
    @DisplayName("ratio loop: declared 1 MB, bodies take 4 MB — the learned multiplier throttles the second batch harder than the declaration would")
    void ratioIsLearnedAndApplied() throws Exception {
        final AdaptiveSizing adaptive = AdaptiveSizing.of(new InMemoryProfileStore(16), config());
        final ThrottledExecutor root = new ThrottledExecutor("AdaptiveRatioLoop");
        root.addListener(adaptive.listener());
        // One task type run eight times — the realistic shape, and what makes the heap-peak path
        // robust: the profile is the MAX over the ring, so a GC landing inside one run (which reads
        // as a peak of 0) cannot hide the seven others.
        final List<String> names = List.of("read", "read", "read", "read", "read", "read", "read", "read");

        // Calibration batch: declared sizes, so no quarantine — serialise explicitly so every run is solo
        // (the heap-peak path needs that; the allocation path does not care).
        root.submitThrottled(batch(names, MB, new Concurrency(), () -> retainAndLinger(4 * MB, 10)), adaptive.policy(new MaxConcurrencyPolicy(1)));
        root.waitForCompletion();

        assertThat(adaptive.profile("read").samples()).hasSize(8);
        final long effective = adaptive.effectiveSize("read", MB);
        if (virtual(root)) {
            assertThat(effective).as("via solo heap peak").isBetween(2 * MB, 16 * MB);
        } else {
            assertThat(effective).as("4 MB / 1 MB * 1.25 = 5 MB, plus a little").isBetween(5 * MB, 7 * MB);
        }

        final long ceiling = 12 * MB;
        final Concurrency undecorated = new Concurrency();
        root.submitThrottled(batch(names, MB, undecorated, () -> retainAndLinger(4 * MB, 30)),
                CompositePolicy.allOf(new GlobalMemoryBudgetPolicy(ceiling, 1.0), new MaxConcurrencyPolicy(8)));
        root.waitForCompletion();
        final Concurrency decorated = new Concurrency();
        root.submitThrottled(batch(names, MB, decorated, () -> retainAndLinger(4 * MB, 30)),
                adaptive.policy(CompositePolicy.allOf(new GlobalMemoryBudgetPolicy(ceiling, 1.0), new MaxConcurrencyPolicy(8))));
        root.waitForCompletion();

        if (!virtual(root)) {
            assertThat(decorated.peak.get()).as("5 MB each under 12 MB: two fit, a third does not").isLessThanOrEqualTo(2);
        }
        assertThat(decorated.peak.get()).isGreaterThanOrEqualTo(1);
        if (workers(root) >= 3) {
            assertThat(undecorated.peak.get())
                    .as("declared 1 MB each under 12 MB: the declaration lets far more through than the learned 5 MB")
                    .isGreaterThan(decorated.peak.get());
        }
        adaptive.close();
    }

    @Test
    @DisplayName("restart survival: what one facade learned, a fresh facade on the same file knows without running anything")
    void learningSurvivesARestart(@TempDir Path dir) throws Exception {
        final Path file = dir.resolve("profiles.tsv");
        final AdaptiveSizing first = AdaptiveSizing.onFile(file);
        final ThrottledExecutor root = new ThrottledExecutor("AdaptiveRestart");
        root.addListener(first.listener());
        root.submitThrottled(batch(List.of("nightly"), -1, new Concurrency(), () -> retainAndLinger(2 * MB, 10)),
                first.policy(new MaxConcurrencyPolicy(4)));
        root.waitForCompletion();
        final long learned = first.effectiveSize("nightly", -1);
        assertThat(learned).isNotEqualTo(AdaptiveSizing.UNKNOWN);
        first.close();                                        // saves

        assertThat(Files.readAllLines(file, StandardCharsets.UTF_8).get(0)).isEqualTo(FileProfileStore.HEADER);
        final AdaptiveSizing second = AdaptiveSizing.onFile(file);

        assertThat(second.effectiveSize("nightly", -1)).isEqualTo(learned);
        assertThat(second.profile("nightly").samples()).hasSize(1);
        second.close();
    }

    @Test
    @DisplayName("a new version: what the old build learned is still on file, but the tasks are quarantined and measured again")
    void newVersionMeasuresAgain(@TempDir Path dir) throws Exception {
        final Path file = dir.resolve("profiles.tsv");
        // Distinct names: a name is quarantined only until its first sample lands, so four
        // copies of one name would run solo once and then three abreast (see the learning loop).
        final List<String> names = List.of("nightly-0", "nightly-1", "nightly-2", "nightly-3");

        final AdaptiveSizing build1 = AdaptiveSizing.onFile(file, config().withVersion("build-1"));
        final ThrottledExecutor root1 = new ThrottledExecutor("AdaptiveBuild1");
        root1.addListener(build1.listener());
        root1.submitThrottled(batch(names, -1, new Concurrency(), () -> retainAndLinger(2 * MB, 10)), build1.policy(new MaxConcurrencyPolicy(4)));
        root1.waitForCompletion();
        for (String name : names) {
            assertThat(build1.effectiveSize(name, -1)).as("%s learned under build-1", name).isNotEqualTo(AdaptiveSizing.UNKNOWN);
        }
        build1.close();

        final AdaptiveSizing build2 = AdaptiveSizing.onFile(file, config().withVersion("build-2"));
        for (String name : names) {
            assertThat(build2.profile(name).samples()).as("%s: the old sample is still on file", name).hasSize(1);
            assertThat(build2.profile(name).samples().get(0).version()).isEqualTo("build-1");
            assertThat(build2.freshProfile(name).samples()).as("%s: but it does not count", name).isEmpty();
            assertThat(build2.effectiveSize(name, -1)).as("%s: unknown again", name).isEqualTo(AdaptiveSizing.UNKNOWN);
        }

        final ThrottledExecutor root2 = new ThrottledExecutor("AdaptiveBuild2");
        root2.addListener(build2.listener());
        final Concurrency relearn = new Concurrency();
        root2.submitThrottled(batch(names, -1, relearn, () -> retainAndLinger(2 * MB, 10)), build2.policy(new MaxConcurrencyPolicy(4)));
        root2.waitForCompletion();

        assertThat(relearn.peak).as("quarantined again: one at a time").hasValue(1);
        for (String name : names) {
            assertThat(build2.effectiveSize(name, -1)).as("%s learned again under build-2", name).isNotEqualTo(AdaptiveSizing.UNKNOWN);
            assertThat(build2.profile(name).samples()).extracting(Sample::version).containsExactly("build-1", "build-2");
            assertThat(build2.freshProfile(name).samples()).extracting(Sample::version).containsExactly("build-2");
            assertThat(build2.freshProfile(name).lastMeasuredAt()).isPresent();
        }
        build2.close();

        final AdaptiveSizing build2Again = AdaptiveSizing.onFile(file, config().withVersion("build-2"));
        assertThat(build2Again.effectiveSize("nightly-0", -1)).as("the relearned size survives a restart").isNotEqualTo(AdaptiveSizing.UNKNOWN);
        assertThat(build2Again.forget("nightly-0")).isTrue();
        assertThat(build2Again.effectiveSize("nightly-0", -1)).as("forget(): unknown once more").isEqualTo(AdaptiveSizing.UNKNOWN);
        assertThat(build2Again.effectiveSize("nightly-1", -1)).as("the others are untouched").isNotEqualTo(AdaptiveSizing.UNKNOWN);
        build2Again.close();
    }

    private static List<ThrottledTask<Runnable>> batch(List<String> names, long declared, Concurrency concurrency, Runnable body) {
        final List<ThrottledTask<Runnable>> tasks = new ArrayList<>();
        for (String name : names) {
            tasks.add(new ThrottledTask<>(concurrency.wrap(body), declared, name));
        }
        return tasks;
    }
}
