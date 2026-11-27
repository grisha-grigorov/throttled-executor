package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link GlobalMemoryBudgetPolicy} — a memory budget shared across
 * <em>independently-rooted</em> executors.
 *
 * <p>The subtree case is already covered structurally: children delegate to the root's
 * {@code ThrottleHarness}, so one tree admits against one set of counters. What that does
 * not cover is two unrelated roots — different subsystems, each with its own harness —
 * both throttling against the same JVM heap. {@link MemoryBudgetPolicy} is useless there
 * even when the instance is shared, because it reads the per-tree snapshot it is handed.
 * This policy owns its own counters, driven by the {@code onTaskStart}/{@code onTaskEnd}
 * hooks, and deliberately ignores the snapshot — the {@link RateLimitPolicy} template.</p>
 *
 * <p>Test hygiene: every test constructs its own policy instance. The process-wide
 * singleton behind {@code AdmissionPolicy.defaultPolicy(backend)} is touched only by the
 * test that exists to verify it, inside try/finally, and the {@code @AfterEach} guard
 * asserts it was left drained — shared mutable JVM state must never leak between tests.</p>
 */
class GlobalMemoryBudgetPolicyTests {

    @AfterEach
    void processWideBudgetMustBeLeftDrained() {
        assertThat(GlobalMemoryBudgetPolicy.processWideHeapBudget().inFlightBytes())
                .as("a test leaked accounting into the process-wide budget singleton")
                .isZero();
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

    private static ThrottledTask<Runnable> taskOf(long size, String name) {
        return new ThrottledTask<>(() -> { }, size, name);
    }

    // ─── Unit behaviour ──────────────────────────────────────────────────────────

    /**
     * The defining property: admission is judged against the policy's own counters, not
     * the per-tree snapshot. A snapshot claiming the pool is idle must not win an
     * admission the global ledger refuses.
     */
    @Test
    @DisplayName("admit ignores the passed snapshot and trusts its own ledger")
    void admitShouldIgnoreThePassedSnapshot() {

        final GlobalMemoryBudgetPolicy policy = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
        final ThrottledTask<Runnable> occupant = taskOf(600L, "occupant");

        policy.onTaskStart(occupant);
        try {
            assertThat(policy.admit(taskOf(600L, "hopeful"), snapshotOf(0, 0L)))
                    .as("600 in flight + 600 candidate exceeds 1000, no matter what the "
                            + "(per-tree, therefore irrelevant) snapshot claims")
                    .isFalse();
        } finally {
            policy.onTaskEnd(occupant);
        }
    }

    @Test
    @DisplayName("onTaskEnd releases exactly what onTaskStart claimed")
    void onTaskEndShouldReleaseGlobalBudget() {

        final GlobalMemoryBudgetPolicy policy = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
        final ThrottledTask<Runnable> task = taskOf(600L, "transient");

        policy.onTaskStart(task);
        assertThat(policy.inFlightBytes()).isEqualTo(600L);
        assertThat(policy.inFlightTaskCount()).isEqualTo(1);

        policy.onTaskEnd(task);
        assertThat(policy.inFlightBytes()).isZero();
        assertThat(policy.inFlightTaskCount()).isZero();

        assertThat(policy.admit(taskOf(600L, "next"), snapshotOf(0, 0L)))
                .as("released budget is admissible again")
                .isTrue();
    }

    /** Mirrors the overflow guard pinned for {@link MemoryBudgetPolicy}. */
    @Test
    @DisplayName("absurd size estimates refuse cleanly instead of overflowing into admission")
    void budgetShouldNotOverflowOnHugeEstimates() {

        final GlobalMemoryBudgetPolicy policy = new GlobalMemoryBudgetPolicy(1_000L, 1.0);

        assertThat(policy.admit(taskOf(Long.MAX_VALUE, "absurd"), snapshotOf(0, 0L)))
                .as("long overflow must not wrap a huge estimate into a negative that fits")
                .isFalse();

        final ThrottledTask<Runnable> huge = taskOf(Long.MAX_VALUE, "huge-occupant");
        policy.onTaskStart(huge);
        try {
            assertThat(policy.admit(taskOf(1L, "tiny"), snapshotOf(0, 0L)))
                    .as("a huge occupant refuses even a tiny candidate")
                    .isFalse();
        } finally {
            policy.onTaskEnd(huge);
        }
    }

    @Test
    @DisplayName("a policy that tracks nothing reports -1; a composite reports its members' max")
    void compositePolicyShouldReportTheMaxInFlightCountOfItsMembers() {

        // MaxConcurrencyPolicy reads the snapshot only — it tracks no state of its own.
        assertThat(new MaxConcurrencyPolicy(4).inFlightTaskCount()).isEqualTo(-1);

        final GlobalMemoryBudgetPolicy tracking = new GlobalMemoryBudgetPolicy(10_000L, 1.0);
        final ThrottledTask<Runnable> first = taskOf(100L, "first");
        final ThrottledTask<Runnable> second = taskOf(100L, "second");

        final AdmissionPolicy composite = CompositePolicy.allOf(new MaxConcurrencyPolicy(4), tracking);

        composite.onTaskStart(first);
        composite.onTaskStart(second);
        try {
            assertThat(composite.inFlightTaskCount())
                    .as("the composite surfaces the most informed member")
                    .isEqualTo(2);
        } finally {
            composite.onTaskEnd(first);
            composite.onTaskEnd(second);
        }

        assertThat(CompositePolicy.allOf(new MaxConcurrencyPolicy(2), new MaxConcurrencyPolicy(3))
                .inFlightTaskCount())
                .as("a composite of untracking members tracks nothing")
                .isEqualTo(-1);
    }

    // ─── Cross-root behaviour ────────────────────────────────────────────────────

    /**
     * Two unrelated roots, one shared policy, a budget that fits one task: the second
     * root's task must wait for the first root's to finish.
     */
    @Test
    @DisplayName("a shared instance counts tasks from independent executor trees")
    void sharedInstanceShouldCountTasksFromMultipleIndependentExecutors() throws InterruptedException {

        final GlobalMemoryBudgetPolicy policy = new GlobalMemoryBudgetPolicy(1_000L, 1.0);

        final ThrottledExecutor rootA = new ThrottledExecutor("GlobalRootA");
        final ThrottledExecutor rootB = new ThrottledExecutor("GlobalRootB");

        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger peakInFlight = new AtomicInteger();
        final CountDownLatch firstStarted = new CountDownLatch(1);
        final CountDownLatch bothDone = new CountDownLatch(2);

        final Runnable measured = () -> {
            final int current = inFlight.incrementAndGet();
            peakInFlight.accumulateAndGet(current, Math::max);
            try {
                Thread.sleep(400);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            } finally {
                inFlight.decrementAndGet();
            }
        };

        final Thread submitterA = new Thread(() -> {
            rootA.submitThrottled(List.of(new ThrottledTask<>((Runnable) () -> {
                firstStarted.countDown();
                measured.run();
            }, 600L, "root-a-task")), policy);
            rootA.waitForCompletion();
            bothDone.countDown();
        }, "submitter-a");

        final Thread submitterB = new Thread(() -> {
            rootB.submitThrottled(List.of(new ThrottledTask<>(measured, 600L, "root-b-task")), policy);
            rootB.waitForCompletion();
            bothDone.countDown();
        }, "submitter-b");

        submitterA.start();
        // B is only released once A's task is genuinely occupying the shared budget.
        assertThat(firstStarted.await(10, TimeUnit.SECONDS)).isTrue();
        submitterB.start();

        assertThat(bothDone.await(30, TimeUnit.SECONDS)).as("both roots finished").isTrue();
        submitterA.join(TimeUnit.SECONDS.toMillis(5));
        submitterB.join(TimeUnit.SECONDS.toMillis(5));

        assertThat(peakInFlight.get())
                .as("two 600-byte tasks from UNRELATED roots share one 1000-byte budget, "
                        + "so they can never run together")
                .isEqualTo(1);
    }

    /**
     * The hole this workstream exists to close. The liveness floor reads the per-tree
     * harness, so an <em>idle</em> root sees {@code runningTasks() == 0} and admits its
     * next task unconditionally — sailing straight past a shared global budget that is
     * already fully occupied by another root. With several idle roots, the budget is
     * exceeded by a factor of their number, in exactly the multi-subsystem scenario a
     * global policy exists for.
     *
     * <p>The assertion pins exactly the guarantee the design makes: <b>no idle root's
     * task may start while the global ledger is occupied</b>. Deliberately NOT asserted:
     * that the overall peak stays at 1 after the seed finishes. Independent roots do not
     * share a harness lock, so at a moment of global zero two roots polling within the
     * same instant can each floor-admit one task — the transient documented on
     * {@link GlobalMemoryBudgetPolicy}. Without the floor fix this test fails
     * immediately: every idle root admits while the seed is still running.</p>
     */
    @Test
    @DisplayName("an idle root's liveness floor does not bypass an occupied global budget")
    void livenessFloorShouldNotBypassAGlobalBudget() throws InterruptedException {

        final GlobalMemoryBudgetPolicy policy = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
        final int idleRootCount = 3;

        final java.util.concurrent.atomic.AtomicBoolean seedInFlight =
                new java.util.concurrent.atomic.AtomicBoolean(false);
        final AtomicInteger startedWhileSeedRan = new AtomicInteger();
        final CountDownLatch seedStarted = new CountDownLatch(1);
        final CountDownLatch allDone = new CountDownLatch(idleRootCount + 1);

        final ThrottledExecutor seedRoot = new ThrottledExecutor("SeedRoot");

        final Thread seeder = new Thread(() -> {
            seedRoot.submitThrottled(List.of(new ThrottledTask<>((Runnable) () -> {
                seedInFlight.set(true);
                seedStarted.countDown();
                try {
                    Thread.sleep(600);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } finally {
                    /*
                     * Cleared in the task body, so it happens-before the ledger release
                     * (which runs in whenComplete, after the body): any idle-root task
                     * admitted on the ledger reaching zero reliably observes false.
                     */
                    seedInFlight.set(false);
                }
            }, 600L, "seed")), policy);
            seedRoot.waitForCompletion();
            allDone.countDown();
        }, "seeder");

        seeder.start();
        assertThat(seedStarted.await(10, TimeUnit.SECONDS)).isTrue();

        // The budget is now fully occupied by the seed. Every one of these roots is idle,
        // so each one's OWN harness reports zero running tasks.
        final List<Thread> idleRootSubmitters = new java.util.ArrayList<>();
        for (int i = 0; i < idleRootCount; i++) {
            final ThrottledExecutor idleRoot = new ThrottledExecutor("IdleRoot-" + i);
            final Thread submitter = new Thread(() -> {
                idleRoot.submitThrottled(List.of(new ThrottledTask<>((Runnable) () -> {
                    if (seedInFlight.get()) {
                        startedWhileSeedRan.incrementAndGet();
                    }
                }, 600L, "idle-root-task")), policy);
                idleRoot.waitForCompletion();
                allDone.countDown();
            });
            idleRootSubmitters.add(submitter);
            submitter.start();
        }

        assertThat(allDone.await(30, TimeUnit.SECONDS))
                .as("everything finished — the floor still provides liveness once the seed ends")
                .isTrue();
        seeder.join(TimeUnit.SECONDS.toMillis(5));
        for (Thread submitter : idleRootSubmitters) {
            submitter.join(TimeUnit.SECONDS.toMillis(5));
        }

        assertThat(startedWhileSeedRan.get())
                .as("no idle root's task may start while the seed occupies the whole budget — "
                        + "any such start means its liveness floor bypassed the global ledger")
                .isZero();
    }

    // ─── The default policy's process-wide budget ────────────────────────────────

    /**
     * The heap is process-wide, so the fail-safe default's memory member must be too:
     * two separately-obtained default policies share one ledger.
     */
    @Test
    @DisplayName("separately-obtained default policies share one heap budget")
    void defaultPolicyShouldShareOneHeapBudgetAcrossCalls() {

        final AdmissionPolicy first = AdmissionPolicy.defaultPolicy(ExecutorBackend.FORK_JOIN);
        final AdmissionPolicy second = AdmissionPolicy.defaultPolicy(ExecutorBackend.FORK_JOIN);

        // The whole heap, so admission is refused regardless of the 85% ceiling's exact value.
        final ThrottledTask<Runnable> everything = taskOf(Runtime.getRuntime().maxMemory(), "everything");

        first.onTaskStart(everything);
        try {
            assertThat(second.admit(taskOf(1_000L, "small"), snapshotOf(0, 0L)))
                    .as("a policy obtained by a DIFFERENT defaultPolicy() call must see the "
                            + "heap budget the first one occupied")
                    .isFalse();
        } finally {
            first.onTaskEnd(everything);
        }
    }
}
