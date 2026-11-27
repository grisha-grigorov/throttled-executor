package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import java.time.Duration;
import java.time.Instant;
import java.time.InstantSource;
import java.util.ArrayList;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.Test;


/**
 * JUnit 5 tests for the pluggable throttling policy API:
 * {@link AdmissionPolicy}, {@link ThrottleHarness} and the built-in policies
 * ({@link MemoryBudgetPolicy}, {@link MaxConcurrencyPolicy},
 * {@link CompositePolicy}, {@link RateLimitPolicy}), plus the
 * {@code ThrottledExecutor.submitThrottled} coordinator behaviour:
 * policy-defined ordering, the executor-enforced liveness floor, the
 * zero-config default, interrupt handling, and completion signalling.
 */
public class ThrottlePolicyTests {

    /** Fixed, hand-rolled snapshot for direct policy unit tests. */
    private record FixedSnapshot(int runningTasks, long trackedSizeBytes) implements PoolSnapshot {
    }

    private static ThrottledTask<Runnable> task(String name, long size) {
        return new ThrottledTask<>(() -> { }, size, name);
    }

    /** Mutable clock for deterministic RateLimitPolicy tests. */
    private static final class FakeClock implements InstantSource {
        private volatile Instant now = Instant.EPOCH;

        @Override
        public Instant instant() {
            return now;
        }

        void advance(Duration d) {
            now = now.plus(d);
        }
    }

    // ── MemoryBudgetPolicy ─────────────────────────────────────────────

    @Test
    public void memoryBudgetAdmitsWithinCeilingAndRefusesBeyond() {
        MemoryBudgetPolicy policy = new MemoryBudgetPolicy(10, 1.0);

        Assertions.assertThat(policy.admit(task("fits", 4), new FixedSnapshot(1, 5)))
                .as("5 + 4 = 9 < 10 must be admitted").isTrue();
        Assertions.assertThat(policy.admit(task("tooBig", 6), new FixedSnapshot(1, 5)))
                .as("5 + 6 = 11 >= 10 must be refused").isFalse();
        Assertions.assertThat(policy.admit(task("boundary", 5), new FixedSnapshot(1, 5)))
                .as("5 + 5 = 10, not strictly below the ceiling, must be refused").isFalse();
    }

    @Test
    public void memoryBudgetAppliesMultiplierToTheTotal() {
        MemoryBudgetPolicy policy = new MemoryBudgetPolicy(10, 1.5);

        // (4 + 2) * 1.5 = 9 < 10 → admitted
        Assertions.assertThat(policy.admit(task("fits", 2), new FixedSnapshot(1, 4))).isTrue();
        // (4 + 3) * 1.5 = 10.5 >= 10 → refused
        Assertions.assertThat(policy.admit(task("tooBig", 3), new FixedSnapshot(1, 4))).isFalse();
    }

    @Test
    public void memoryBudgetHandlesExtremeSizesWithoutOverflow() {
        MemoryBudgetPolicy policy = new MemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);

        // Long.MAX_VALUE + Long.MAX_VALUE overflows long arithmetic to a negative
        // number, which would wrongly admit an enormous task. Must be refused.
        Assertions.assertThat(policy.admit(task("huge", Long.MAX_VALUE),
                        new FixedSnapshot(1, Long.MAX_VALUE)))
                .as("extreme estimates must not overflow into admission").isFalse();
    }

    // ── ThrottleHarness ────────────────────────────────────────────────

    @Test
    public void harnessCountersSurviveConcurrentStartEndPairs() throws Exception {
        final int threads = 8;
        final int iterations = 5_000;

        ThrottleHarness harness = new ThrottleHarness();
        AdmissionPolicy noopPolicy = (candidate, snapshot) -> true;
        ThrottledTask<Runnable> memTask = task("RaceTask", 1_000_000L);

        CyclicBarrier barrier = new CyclicBarrier(threads);
        List<Thread> workers = new ArrayList<>(threads);
        for (int t = 0; t < threads; t++) {
            Thread worker = new Thread(() -> {
                try {
                    barrier.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                for (int i = 0; i < iterations; i++) {
                    harness.admitFirst(new ArrayDeque<>(List.of(memTask)), noopPolicy);
                    harness.onEnd(memTask, noopPolicy);
                }
            }, "harness-race-" + t);
            workers.add(worker);
            worker.start();
        }
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(60));
        }

        PoolSnapshot snapshot = harness.snapshot();
        Assertions.assertThat(snapshot.runningTasks()).isEqualTo(0);
        Assertions.assertThat(snapshot.trackedSizeBytes())
                .as("every onStart must be exactly cancelled by its onEnd").isEqualTo(0L);
        Assertions.assertThat(harness.stateVersion())
                .as("every completion must advance the state version")
                .isEqualTo((long) threads * iterations);
    }

    @Test
    public void harnessAwaitReturnsImmediatelyIfVersionAlreadyAdvanced() throws Exception {
        ThrottleHarness harness = new ThrottleHarness();
        AdmissionPolicy noopPolicy = (candidate, snapshot) -> true;
        ThrottledTask<Runnable> memTask = task("T", 1);

        long observed = harness.stateVersion();
        harness.admitFirst(new ArrayDeque<>(List.of(memTask)), noopPolicy);
        harness.onEnd(memTask, noopPolicy); // version advances past `observed`

        long before = System.nanoTime();
        harness.awaitStateChange(observed, 30_000); // must NOT wait 30s
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before);

        Assertions.assertThat(elapsedMillis)
                .as("await on an already-stale version must return immediately")
                .isLessThan(5_000L);
    }

    // ── MaxConcurrencyPolicy ───────────────────────────────────────────

    @Test
    public void maxConcurrencyCapsInFlightCount() {
        MaxConcurrencyPolicy policy = new MaxConcurrencyPolicy(2);

        Assertions.assertThat(policy.admit(task("t", 1), new FixedSnapshot(0, 0))).isTrue();
        Assertions.assertThat(policy.admit(task("t", 1), new FixedSnapshot(1, 0))).isTrue();
        Assertions.assertThat(policy.admit(task("t", 1), new FixedSnapshot(2, 0)))
                .as("2 running with max 2 must be refused").isFalse();
    }

    // ── CompositePolicy ────────────────────────────────────────────────

    @Test
    public void compositeRefusesIfAnyMemberRefusesAndShortCircuits() {
        AtomicInteger secondPolicyCalls = new AtomicInteger();

        AdmissionPolicy alwaysRefuse = (candidate, snapshot) -> false;
        AdmissionPolicy countingAdmit = (candidate, snapshot) -> {
            secondPolicyCalls.incrementAndGet();
            return true;
        };

        CompositePolicy refuseFirst = CompositePolicy.allOf(alwaysRefuse, countingAdmit);
        Assertions.assertThat(refuseFirst.admit(task("t", 1), new FixedSnapshot(1, 0))).isFalse();
        Assertions.assertThat(secondPolicyCalls.get())
                .as("evaluation must short-circuit on the first refusal").isEqualTo(0);

        CompositePolicy admitBoth = CompositePolicy.allOf(countingAdmit, countingAdmit);
        Assertions.assertThat(admitBoth.admit(task("t", 1), new FixedSnapshot(1, 0))).isTrue();
        Assertions.assertThat(secondPolicyCalls.get()).isEqualTo(2);
    }

    @Test
    public void compositeFansOutHooksAndDelegatesOrderingToFirstMember() {
        AtomicInteger startHooks = new AtomicInteger();
        Comparator<ThrottledTask<?>> smallestFirst = Comparator.comparingLong(t -> t.size);

        AdmissionPolicy first = new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public void onTaskStart(ThrottledTask<?> task) {
                startHooks.incrementAndGet();
            }

            @Override
            public Comparator<ThrottledTask<?>> selectionOrder() {
                return smallestFirst;
            }
        };
        AdmissionPolicy second = new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public void onTaskStart(ThrottledTask<?> task) {
                startHooks.incrementAndGet();
            }
        };

        CompositePolicy composite = CompositePolicy.allOf(first, second);
        composite.onTaskStart(task("t", 1));

        Assertions.assertThat(startHooks.get()).as("hooks must fan out to all members").isEqualTo(2);
        Assertions.assertThat(composite.selectionOrder())
                .as("ordering must delegate to the first member").isSameAs(smallestFirst);
    }

    // ── RateLimitPolicy ────────────────────────────────────────────────

    @Test
    public void rateLimitDeniesBeyondWindowAndRecoversWhenClockAdvances() {
        FakeClock clock = new FakeClock();
        RateLimitPolicy policy = new RateLimitPolicy(2, Duration.ofSeconds(1), clock);
        PoolSnapshot any = new FixedSnapshot(1, 0);

        Assertions.assertThat(policy.admit(task("t1", 1), any)).isTrue();
        policy.onTaskStart(task("t1", 1));
        Assertions.assertThat(policy.admit(task("t2", 1), any)).isTrue();
        policy.onTaskStart(task("t2", 1));

        Assertions.assertThat(policy.admit(task("t3", 1), any))
                .as("3rd start within the window must be denied").isFalse();

        clock.advance(Duration.ofSeconds(2));
        Assertions.assertThat(policy.admit(task("t3", 1), any))
                .as("after the window slides past, starts must be admitted again").isTrue();
    }

    // ── ThrottledExecutor.submitThrottled ───────────────────────────────

    @Test
    public void customSelectionOrderControlsExecutionOrder() {
        ThrottledExecutor pr = new ThrottledExecutor("SelectionOrder");
        StringBuilder builder = new StringBuilder();

        List<ThrottledTask<Runnable>> jobs = new ArrayList<>();
        for (String name : List.of("Big:9", "Small:1", "Medium:5")) {
            String taskName = name.split(":")[0];
            long size = Long.parseLong(name.split(":")[1]);
            jobs.add(new ThrottledTask<>(() -> {
                synchronized (builder) {
                    builder.append(taskName).append(",");
                }
            }, size, taskName));
        }

        // Smallest-first ordering, serialized by a concurrency cap of 1.
        AdmissionPolicy smallestFirstOneAtATime = new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return snapshot.runningTasks() < 1;
            }

            @Override
            public Comparator<ThrottledTask<?>> selectionOrder() {
                return Comparator.comparingLong(t -> t.size);
            }
        };

        pr.submitThrottled(jobs, smallestFirstOneAtATime);
        pr.waitForCompletion();

        Assertions.assertThat(builder.toString()).isEqualTo("Small,Medium,Big,");
    }

    @Test
    public void middleSizedCandidateRunsWhenEndsDoNotFit() throws Exception {
        ThrottledExecutor pr = new ThrottledExecutor("MiddleFit");
        MemoryBudgetPolicy budget = new MemoryBudgetPolicy(10, 1.0);

        // Occupy 4/10 with a latched task so the scan below sees runningTasks > 0.
        CountDownLatch release = new CountDownLatch(1);
        List<ThrottledTask<Runnable>> occupier = new ArrayList<>();
        occupier.add(new ThrottledTask<>(() -> {
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 4L, "Occupier"));
        pr.submitThrottled(occupier, budget);

        // Queue [8, 5, 2]: 8 does not fit (12), but the MIDDLE candidate 5 does (9 < 10).
        // The old front/back two-probe would have skipped 5 and picked 2.
        StringBuilder firstAdmitted = new StringBuilder();
        List<ThrottledTask<Runnable>> jobs = new ArrayList<>();
        for (long size : new long[]{8, 5, 2}) {
            String name = "T" + size;
            jobs.add(new ThrottledTask<>(() -> {
                synchronized (firstAdmitted) {
                    if (firstAdmitted.isEmpty()) {
                        firstAdmitted.append(name);
                    }
                }
            }, size, name));
        }

        Thread submitter = new Thread(() -> pr.submitThrottled(jobs, budget), "middle-fit-submitter");
        submitter.setDaemon(true);
        submitter.start();

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(firstAdmitted.toString())
                        .as("the middle-sized candidate must be admitted first")
                        .isEqualTo("T5"));

        release.countDown();
        submitter.join(TimeUnit.SECONDS.toMillis(30));
        pr.waitForCompletion();
    }

    @Test
    public void livenessFloorExecutesEverythingEvenIfPolicyAlwaysRefuses() {
        ThrottledExecutor pr = new ThrottledExecutor("LivenessFloor");
        StringBuilder builder = new StringBuilder();

        List<ThrottledTask<Runnable>> jobs = new ArrayList<>();
        for (String name : List.of("First", "Second", "Third")) {
            jobs.add(new ThrottledTask<>(() -> {
                synchronized (builder) {
                    builder.append(name).append(",");
                }
            }, 1L, name));
        }

        AdmissionPolicy alwaysRefuse = (candidate, snapshot) -> false;

        pr.submitThrottled(jobs, alwaysRefuse);
        pr.waitForCompletion();

        Assertions.assertThat(builder.toString())
                .as("the executor must never throttle to zero — every task runs (sequentially)")
                .contains("First").contains("Second").contains("Third");
    }

    @Test
    public void defaultPolicyFallbackRunsWithZeroConfigAndImmutableList() {
        ThrottledExecutor pr = new ThrottledExecutor("DefaultPolicy");
        AtomicLong executed = new AtomicLong();

        List<ThrottledTask<Runnable>> jobs = List.of(
                new ThrottledTask<>(executed::incrementAndGet, 1_000L, "A"),
                new ThrottledTask<>(executed::incrementAndGet, 2_000L, "B"));

        pr.submitThrottled(jobs); // no policy given — defaultPolicy(); immutable list must be accepted
        pr.waitForCompletion();

        Assertions.assertThat(executed.get()).isEqualTo(2L);
        Assertions.assertThat(jobs).as("the caller's list must not be mutated").hasSize(2);
    }

    @Test
    public void interruptDuringThrottledWaitTerminatesTheCoordinator() throws Exception {
        ThrottledExecutor pr = new ThrottledExecutor("InterruptThrottled");
        MemoryBudgetPolicy budget = new MemoryBudgetPolicy(10, 1.0);

        CountDownLatch release = new CountDownLatch(1);
        List<ThrottledTask<Runnable>> occupier = new ArrayList<>();
        occupier.add(new ThrottledTask<>(() -> {
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 9L, "Occupier"));
        pr.submitThrottled(occupier, budget);

        List<ThrottledTask<Runnable>> blocked = new ArrayList<>();
        blocked.add(new ThrottledTask<>(() -> { }, 9L, "Blocked"));

        Thread submitter = new Thread(() -> pr.submitThrottled(blocked, budget), "blocked-submitter");
        submitter.setDaemon(true);
        submitter.start();

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> submitter.getState() == Thread.State.WAITING
                        || submitter.getState() == Thread.State.TIMED_WAITING);
        submitter.interrupt();

        try {
            Awaitility.await().atMost(3, TimeUnit.SECONDS)
                    .untilAsserted(() -> Assertions.assertThat(submitter.isAlive())
                            .as("an interrupted coordinator must terminate, not spin")
                            .isFalse());
        } finally {
            release.countDown();
            pr.waitForCompletion();
            Thread.interrupted();
        }
    }

    @Test
    public void taskCompletionWakesAWaitingCoordinator() throws Exception {
        ThrottledExecutor pr = new ThrottledExecutor("CompletionWakes");
        MemoryBudgetPolicy budget = new MemoryBudgetPolicy(10, 1.0);

        // Occupy 6/10, then release shortly after the second coordinator blocks.
        CountDownLatch release = new CountDownLatch(1);
        List<ThrottledTask<Runnable>> occupier = new ArrayList<>();
        occupier.add(new ThrottledTask<>(() -> {
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }, 6L, "Occupier"));
        pr.submitThrottled(occupier, budget);

        // 5 does not fit next to 6; it fits as soon as the occupier completes.
        AtomicLong executed = new AtomicLong();
        List<ThrottledTask<Runnable>> waiting = new ArrayList<>();
        waiting.add(new ThrottledTask<>(executed::incrementAndGet, 5L, "Waiter"));

        Thread submitter = new Thread(() -> pr.submitThrottled(waiting, budget), "waiting-submitter");
        submitter.setDaemon(true);
        submitter.start();

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .until(() -> submitter.getState() == Thread.State.WAITING
                        || submitter.getState() == Thread.State.TIMED_WAITING);

        release.countDown(); // occupier completes → framework signals unconditionally

        Awaitility.await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertThat(executed.get())
                        .as("the waiting coordinator must be woken and submit its task")
                        .isEqualTo(1L));

        submitter.join(TimeUnit.SECONDS.toMillis(30));
        pr.waitForCompletion();
    }

    // ── Policy hooks must never break the framework ─────────────────────

    /**
     * A policy whose hook throws: the framework must treat it as a policy bug, not as a
     * failure of the admission or the task. Shared helper.
     */
    private static AdmissionPolicy throwingPolicy(boolean throwOnStart, boolean throwOnEnd) {
        return new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public void onTaskStart(ThrottledTask<?> task) {
                if (throwOnStart) {
                    throw new IllegalStateException("onTaskStart boom");
                }
            }

            @Override
            public void onTaskEnd(ThrottledTask<?> task) {
                if (throwOnEnd) {
                    throw new IllegalStateException("onTaskEnd boom");
                }
            }
        };
    }

    /**
     * The dangerous case. claimLocked increments the harness counters and THEN calls
     * onTaskStart; if the hook throws, the exception escapes admitFirst with the counters
     * already bumped and no matching onEnd ever coming — a permanent budget leak. The
     * claim must stand and the exception must be contained.
     */
    @Test
    public void harnessShouldNotLeakCountersWhenOnTaskStartThrows() {
        ThrottleHarness harness = new ThrottleHarness();
        ThrottledTask<Runnable> t = task("claimed", 100);

        // If the hook's exception escapes, this line throws and the test fails right here.
        List<ThrottledTask<Runnable>> candidates = new ArrayList<>(List.of(t));
        ThrottledTask<Runnable> admitted = harness.admitFirst(candidates, throwingPolicy(true, false));

        Assertions.assertThat(admitted)
                .as("a throwing onTaskStart must not abort the admission")
                .isSameAs(t);
        Assertions.assertThat(candidates)
                .as("admitFirst removes the admitted candidate from the collection it was given")
                .isEmpty();
        Assertions.assertThat(harness.snapshot().runningTasks()).isEqualTo(1);

        harness.onEnd(t, throwingPolicy(false, false));

        Assertions.assertThat(harness.snapshot().runningTasks())
                .as("counters balance once the task ends")
                .isZero();
        Assertions.assertThat(harness.snapshot().trackedSizeBytes()).isZero();
    }

    /**
     * onEnd already guarantees the counters are released and waiters are signalled even
     * if the hook throws — but it re-throws afterwards, so the exception escapes into a
     * whenComplete stage and silently vanishes. It should be contained and logged.
     */
    @Test
    public void harnessOnEndShouldNotPropagateAPolicyException() {
        ThrottleHarness harness = new ThrottleHarness();
        ThrottledTask<Runnable> t = task("ending", 100);

        harness.admitFirst(new ArrayList<>(List.of(t)), throwingPolicy(false, false));
        long versionBefore = harness.stateVersion();

        Assertions.assertThatCode(() -> harness.onEnd(t, throwingPolicy(false, true)))
                .as("a throwing onTaskEnd is a policy bug, not the framework's failure")
                .doesNotThrowAnyException();

        Assertions.assertThat(harness.snapshot().runningTasks()).isZero();
        Assertions.assertThat(harness.snapshot().trackedSizeBytes()).isZero();
        Assertions.assertThat(harness.stateVersion())
                .as("waiters are still signalled")
                .isEqualTo(versionBefore + 1);
    }

    /** One throwing member must not stop the fan-out reaching the members after it. */
    @Test
    public void compositeShouldFanOutHooksToEveryMemberEvenIfOneThrows() {
        AtomicInteger starts = new AtomicInteger();
        AtomicInteger ends = new AtomicInteger();

        AdmissionPolicy counting = new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public void onTaskStart(ThrottledTask<?> task) {
                starts.incrementAndGet();
            }

            @Override
            public void onTaskEnd(ThrottledTask<?> task) {
                ends.incrementAndGet();
            }
        };

        // Throwing member FIRST, so a naive loop would never reach the counting one.
        AdmissionPolicy composite = CompositePolicy.allOf(throwingPolicy(true, true), counting);
        ThrottledTask<Runnable> t = task("fanout", 1);

        Assertions.assertThatCode(() -> {
            composite.onTaskStart(t);
            composite.onTaskEnd(t);
        }).doesNotThrowAnyException();

        Assertions.assertThat(starts.get()).as("onTaskStart reached the second member").isEqualTo(1);
        Assertions.assertThat(ends.get()).as("onTaskEnd reached the second member").isEqualTo(1);
    }
}
