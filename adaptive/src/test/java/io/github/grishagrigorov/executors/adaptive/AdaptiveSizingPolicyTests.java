package io.github.grishagrigorov.executors.adaptive;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.CompositePolicy;
import io.github.grishagrigorov.executors.throttling.GlobalMemoryBudgetPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.MemoryBudgetPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

/**
 * The deciding half, driven directly — no executor. {@link AdaptiveSizingPolicy} is a
 * decorator: the delegate is shown a <em>resized</em> copy of every task (the learned
 * estimate instead of the declared size), the original→resized pairing lives in the
 * decorator's own ledger from {@code onTaskStart} to {@code onTaskEnd}, and tasks nothing
 * is known about are quarantined so they run alone, once.
 *
 * <p>Facts from core this leans on: the harness's liveness floor short-circuits
 * {@code admit} and calls {@code onTaskStart} directly; {@code admit} must be pure; the
 * same {@code ThrottledTask} instance reaches start and end; {@code PoolSnapshot}'s tracked
 * bytes are <em>declared</em> sizes.</p>
 */
class AdaptiveSizingPolicyTests {

    private static final long NM = Sample.NOT_MEASURED;

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

    private static ThrottledTask<Runnable> task(String name, long size) {
        return new ThrottledTask<>(() -> { }, size, name);
    }

    private static AdaptiveSizing sizing(long reserve) {
        return AdaptiveSizing.of(new InMemoryProfileStore(16), new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, reserve, Duration.ZERO));
    }

    private static AdaptiveSizing sizing() {
        return sizing(1L << 30);
    }

    private static void learn(AdaptiveSizing sizing, String name, long declared, long alloc) {
        sizing.store().record(name, new Sample(declared, alloc, NM, 1L));
    }

    /** A delegate that records everything it is shown and answers as told. */
    private static final class RecordingPolicy implements AdmissionPolicy {
        final List<ThrottledTask<?>> admitted = new ArrayList<>();
        final List<PoolSnapshot> snapshots = new ArrayList<>();
        final List<ThrottledTask<?>> started = new ArrayList<>();
        final List<ThrottledTask<?>> ended = new ArrayList<>();
        boolean answer = true;
        int inFlight = -1;

        @Override
        public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
            admitted.add(candidate);
            snapshots.add(snapshot);
            return answer;
        }

        @Override
        public void onTaskStart(ThrottledTask<?> task) {
            started.add(task);
        }

        @Override
        public void onTaskEnd(ThrottledTask<?> task) {
            ended.add(task);
        }

        @Override
        public int inFlightTaskCount() {
            return inFlight;
        }
    }

    @Nested
    @DisplayName("resizing")
    class Resizing {

        @Test
        @DisplayName("the resized copy carries the original's tags — a resource policy behind the decorator still meters")
        void resizedCopyKeepsTags() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);                 // absolute 800 * 1.25 = 1000 -> a copy is made
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(delegate);
            final ThrottledTask<Runnable> tagged =
                    new ThrottledTask<>(() -> { }, -1, "job", java.util.Set.of("hsqldb", "slow"));

            assertThat(policy.admit(tagged, snapshotOf(1, 0))).isTrue();

            final ThrottledTask<?> seen = delegate.admitted.get(0);
            assertThat(seen).as("a resize really happened").isNotSameAs(tagged);
            assertThat(seen.tags).containsExactlyInAnyOrder("hsqldb", "slow");
        }

        @Test
        @DisplayName("the delegate is shown a copy carrying the learned size, same job and name; the original is untouched")
        void delegateSeesResizedCopy() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);                 // absolute 800 * 1.25 = 1000
            learn(sizing, "read", 100, 200);               // ratio 2.0
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(delegate);
            final ThrottledTask<Runnable> unsized = task("job", -1);
            final ThrottledTask<Runnable> sized = task("read", 1_000);

            assertThat(policy.admit(unsized, snapshotOf(1, 0))).isTrue();
            assertThat(policy.admit(sized, snapshotOf(1, 0))).isTrue();

            assertThat(delegate.admitted).hasSize(2);
            final ThrottledTask<?> seenUnsized = delegate.admitted.get(0);
            assertThat(seenUnsized).isNotSameAs(unsized);
            assertThat(seenUnsized.size).isEqualTo(1_000);
            assertThat(seenUnsized.name).isEqualTo("job");
            assertThat(seenUnsized.job).isSameAs(unsized.job);
            assertThat(delegate.admitted.get(1).size).isEqualTo(2_500);
            assertThat(unsized.size).isEqualTo(-1);
            assertThat(sized.size).isEqualTo(1_000);
        }

        @Test
        @DisplayName("a declared task nothing was learned about is passed through as the same instance")
        void unlearnedDeclaredTaskPassesThroughUnchanged() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);
            final ThrottledTask<Runnable> t = task("never-seen", 4_096);

            policy.admit(t, snapshotOf(1, 0));
            policy.onTaskStart(t);
            policy.onTaskEnd(t);

            assertThat(delegate.admitted).containsExactly(t);
            assertThat(delegate.started).containsExactly(t);
            assertThat(delegate.ended).containsExactly(t);
        }

        @Test
        @DisplayName("a learned zero is shown as zero")
        void learnedZeroIsShownAsZero() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "cheap", -1, 0);
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1_000, 1.0);
            final AdaptiveSizingPolicy policy = sizing.policy(budget);
            final ThrottledTask<Runnable> t = task("cheap", -1);

            assertThat(policy.admit(t, snapshotOf(1, 0))).isTrue();
            policy.onTaskStart(t);
            assertThat(budget.inFlightBytes()).isZero();
            assertThat(budget.inFlightTaskCount()).isEqualTo(1);
            policy.onTaskEnd(t);
        }

        @Test
        @DisplayName("a delegate refusal is a refusal")
        void delegateRefusalIsARefusal() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);
            final RecordingPolicy delegate = new RecordingPolicy();
            delegate.answer = false;

            assertThat(sizing.policy(delegate).admit(task("job", -1), snapshotOf(1, 0))).isFalse();
            assertThat(delegate.admitted).hasSize(1);
        }
    }

    @Nested
    @DisplayName("the ledger")
    class Ledger {

        @Test
        @DisplayName("start and end forward the SAME resized instance, so a real budget ledger returns to zero even when a sample lands in between")
        void sameResizedInstanceStartToEnd() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1L << 30, 1.0);
            final RecordingPolicy recorder = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(CompositePolicy.allOf(budget, recorder));
            final ThrottledTask<Runnable> t = task("job", -1);

            policy.onTaskStart(t);                        // floor path: no admit before it
            assertThat(budget.inFlightBytes()).isEqualTo(1_000);
            learn(sizing, "job", -1, 8_000);              // the estimate is now 10 000
            policy.onTaskEnd(t);

            assertThat(budget.inFlightBytes()).isZero();
            assertThat(budget.inFlightTaskCount()).isZero();
            assertThat(recorder.started).hasSize(1);
            assertThat(recorder.ended).hasSize(1);
            assertThat(recorder.ended.get(0)).isSameAs(recorder.started.get(0));
            assertThat(recorder.started.get(0).size).isEqualTo(1_000);
        }

        @Test
        @DisplayName("the same instance in flight twice is two ledger entries: both starts and both ends reach the delegate")
        void sameInstanceInFlightTwiceIsChargedTwice() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);                // estimate 1 000
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(delegate);
            final ThrottledTask<Runnable> t = task("job", -1);

            policy.onTaskStart(t);
            policy.onTaskStart(t);                        // core charges per claim; so must the decorator

            assertThat(delegate.started).hasSize(2);
            assertThat(policy.inFlightTaskCount()).isEqualTo(2);
            policy.admit(task("probe", 10), snapshotOf(2, 0));
            assertThat(delegate.snapshots.get(0).trackedSizeBytes()).as("both copies in the effective ledger").isEqualTo(2_000);

            policy.onTaskEnd(t);
            assertThat(delegate.ended).hasSize(1);
            assertThat(policy.inFlightTaskCount()).isEqualTo(1);
            policy.onTaskEnd(t);
            assertThat(delegate.ended).hasSize(2);
            assertThat(policy.inFlightTaskCount()).as("ledger empty, delegate tracks nothing").isEqualTo(-1);
            policy.admit(task("probe", 10), snapshotOf(0, 0));
            assertThat(delegate.snapshots.get(1).trackedSizeBytes()).isZero();
        }

        @Test
        @DisplayName("the same unknown listed twice runs twice — one after the other, never together")
        void theSameUnknownListedTwiceRunsSerially() {
            // Under the private census this was "in flight twice, still quarantined until the
            // second end". The harness now refuses to admit anything while an exclusive task is
            // running, including a second copy of the same instance, so the batch is serialised
            // rather than double-claimed and the ledger never carries two unknown entries.
            final AdaptiveSizingPolicy policy = sizing().policy(new RecordingPolicy());
            final ThrottleHarness harness = new ThrottleHarness();
            final ThrottledTask<Runnable> unknown = task("never-seen", -1);

            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(unknown, unknown));

            assertThat(harness.admitFirst(queue, policy)).isSameAs(unknown);
            assertThat(harness.admitFirst(queue, policy)).as("the second copy waits").isNull();

            harness.onEnd(unknown, policy);

            assertThat(harness.admitFirst(queue, policy)).as("and then runs").isSameAs(unknown);
            harness.onEnd(unknown, policy);
        }

        @Test
        @DisplayName("onTaskEnd for a task that never started is dropped, not forwarded (it would unbalance the delegate)")
        void endWithoutStartIsNotForwarded() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);

            policy.onTaskEnd(task("ghost", 10));

            assertThat(delegate.ended).isEmpty();
        }

        @Test
        @DisplayName("inFlightTaskCount: the larger of the own ledger and the delegate's while the ledger has entries; the delegate's alone when it is empty")
        void inFlightTaskCountIsTheMax() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);

            delegate.inFlight = 3;
            assertThat(policy.inFlightTaskCount()).as("own 0, delegate 3").isEqualTo(3);

            delegate.inFlight = -1;
            assertThat(policy.inFlightTaskCount())
                    .as("own 0, delegate -1: nothing to add — keep the harness flooring on the tree's own count")
                    .isEqualTo(-1);

            final ThrottledTask<Runnable> a = task("a", 1);
            final ThrottledTask<Runnable> b = task("b", 1);
            policy.onTaskStart(a);
            policy.onTaskStart(b);
            assertThat(policy.inFlightTaskCount()).as("own 2, delegate -1").isEqualTo(2);
            delegate.inFlight = 5;
            assertThat(policy.inFlightTaskCount()).as("own 2, delegate 5").isEqualTo(5);
            policy.onTaskEnd(a);
            policy.onTaskEnd(b);
            delegate.inFlight = -1;
            assertThat(policy.inFlightTaskCount()).as("ledger drained: the delegate's answer again").isEqualTo(-1);
        }

        @Test
        @DisplayName("2 threads x 10 000 start/end pairs leave an empty ledger and a drained delegate")
        void concurrentStartEndLeavesNothingBehind() throws Exception {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1L << 40, 1.0);
            final AdaptiveSizingPolicy policy = sizing.policy(budget);
            final CountDownLatch go = new CountDownLatch(1);
            final AtomicInteger failures = new AtomicInteger();
            final List<Thread> threads = new ArrayList<>();
            for (int i = 0; i < 2; i++) {
                final Thread thread = new Thread(() -> {
                    try {
                        go.await();
                        for (int n = 0; n < 10_000; n++) {
                            final ThrottledTask<Runnable> t = task(n % 2 == 0 ? "job" : "other", n % 3 == 0 ? -1 : n);
                            policy.onTaskStart(t);
                            policy.onTaskEnd(t);
                        }
                    } catch (Throwable e) {
                        failures.incrementAndGet();
                    }
                });
                thread.start();
                threads.add(thread);
            }
            go.countDown();
            for (Thread t : threads) {
                t.join(30_000);
            }

            assertThat(failures).hasValue(0);
            assertThat(policy.inFlightTaskCount()).isZero();
            assertThat(budget.inFlightBytes()).isZero();
            assertThat(budget.inFlightTaskCount()).isZero();
        }
    }

    @Nested
    @DisplayName("unknown tasks and the quarantine")
    class Unknowns {

        @Test
        @DisplayName("an unknown task is never admitted by admit() — only the liveness floor may start it")
        void unknownIsRefusedByAdmit() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);

            assertThat(policy.admit(task("never-seen", -1), snapshotOf(0, 0))).isFalse();
            assertThat(delegate.admitted).as("the delegate is not even consulted").isEmpty();
        }

        @Test
        @DisplayName("requiresExclusive is free for a task that cannot be unknown — no store lookup, no delegate call it cannot need")
        void requiresExclusiveIsCheapForKnowableTasks() {

            /*
             * The harness asks requiresExclusive for EVERY remaining candidate on every scan
             * once the policy says it may isolate, which this one always does. A List batch of
             * N declared-size tasks is then N² questions per batch — and each one did a store
             * lookup, a clock read and a delegate call. Only a task declared -1 can ever be
             * unknown, so for everything else the answer is the delegate's alone, and not
             * even that when the delegate's class says it never isolates.
             */
            final java.util.concurrent.atomic.AtomicInteger finds = new java.util.concurrent.atomic.AtomicInteger();
            final ProfileStore counting = new ProfileStore() {
                private final InMemoryProfileStore inner = new InMemoryProfileStore(16);

                @Override
                public TaskProfile find(String name) {
                    finds.incrementAndGet();
                    return inner.find(name);
                }

                @Override
                public void record(String name, Sample sample) {
                    inner.record(name, sample);
                }

                @Override
                public void save() {
                }

                @Override
                public java.util.Map<String, TaskProfile> snapshot() {
                    return inner.snapshot();
                }
            };
            final AdaptiveSizing sizing = AdaptiveSizing.of(counting,
                    new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 20, Duration.ZERO));
            final java.util.concurrent.atomic.AtomicInteger delegateAsked = new java.util.concurrent.atomic.AtomicInteger();
            final AdmissionPolicy neverIsolates = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }
            };
            final AdmissionPolicy isolatesSometimes = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    delegateAsked.incrementAndGet();
                    return false;
                }
            };

            final AdaptiveSizingPolicy cheap = sizing.policy(neverIsolates);
            for (int i = 0; i < 1_000; i++) {
                assertThat(cheap.requiresExclusive(task("declared-" + i, 10))).isFalse();
                assertThat(cheap.requiresExclusive(task(null, -1))).isFalse();
            }
            assertThat(finds).as("a declared size can never be unknown: nothing to look up").hasValue(0);

            final AdaptiveSizingPolicy forwarding = sizing.policy(isolatesSometimes);
            for (int i = 0; i < 1_000; i++) {
                assertThat(forwarding.requiresExclusive(task("declared-" + i, 10))).isFalse();
            }
            assertThat(finds).hasValue(0);
            assertThat(delegateAsked).as("a delegate that may isolate is still asked").hasValue(1_000);

            assertThat(cheap.requiresExclusive(task("never-seen", -1))).as("an unknown still isolates").isTrue();
            assertThat(finds).as("and that is the one question worth a lookup").hasValue(1);
        }

        @Test
        @DisplayName("a delegate's explicit per-tree ledger scope survives wrapping")
        void delegateLedgerScopeIsForwarded() {

            /*
             * ledgerSpansExecutors decides whose parked claims the re-entrancy exemption may
             * count. The inherited default derives it from inFlightTaskCount() >= 0 — and this
             * decorator forwards the delegate's count, so a per-tree delegate that reports a
             * count and explicitly answers false would be flipped back to process-wide the
             * moment it is wrapped. The decorator's own ledger spans whatever trees share the
             * instance, so: own entries -> spanning; otherwise the delegate's word stands.
             */
            final AdmissionPolicy perTree = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public int inFlightTaskCount() {
                    return 2;                                       // a ledger, but a per-tree one
                }

                @Override
                public boolean ledgerSpansExecutors() {
                    return false;
                }
            };
            final AdaptiveSizingPolicy policy = sizing(0).policy(perTree);

            assertThat(policy.ledgerSpansExecutors())
                    .as("an empty own ledger: the delegate's explicit choice stands")
                    .isFalse();

            final ThrottledTask<Runnable> running = task("declared", 10);
            policy.onTaskStart(running);
            assertThat(policy.ledgerSpansExecutors())
                    .as("own entries span whatever trees share this instance")
                    .isTrue();
            policy.onTaskEnd(running);

            assertThat(policy.ledgerSpansExecutors()).isFalse();
        }

        @Test
        @DisplayName("nested in a composite behind a member that already isolates, the delegate refresh still happens")
        void compositeNestingDoesNotFreezeTheDelegateRefresh() {

            /*
             * The refresh rides on this instance's own mayRequireExclusive call. A composite
             * that short-circuits on its first may-isolate member would never make that call
             * for a later adaptive member, freezing its delegate answer at the construction
             * seed — the exact freeze the per-scan refresh exists to prevent, one supported
             * composition away. The refresh therefore rides on onScanStart, the lifecycle
             * announcement composites fan out to every member unconditionally.
             */
            final java.util.concurrent.atomic.AtomicBoolean isolating = new java.util.concurrent.atomic.AtomicBoolean(false);
            final AdmissionPolicy flagged = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return isolating.get() && "Solo".equals(task.name);
                }

                @Override
                public boolean mayRequireExclusive() {
                    return isolating.get();
                }
            };
            final AdmissionPolicy alwaysMay = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return false;                                   // may isolate, never does here
                }
            };
            final AdmissionPolicy composite = io.github.grishagrigorov.executors.throttling.CompositePolicy.allOf(
                    alwaysMay, sizing(0).policy(flagged));
            final ThrottledTask<Runnable> solo = task("Solo", 10);

            assertThat(composite.requiresExclusive(solo)).isFalse();

            isolating.set(true);
            composite.onScanStart();                                // the harness announces every scan
            assertThat(composite.requiresExclusive(solo))
                    .as("the adaptive member behind an already-isolating one was refreshed too")
                    .isTrue();
        }

        @Test
        @DisplayName("a delegate that throws from mayRequireExclusive is read as \"may\" — the side that costs a scan, not an isolation")
        void throwingMayRequireExclusiveIsReadAsMay() {

            /*
             * The framework's convention everywhere this hook is read (the harness's guard,
             * CompositePolicy): a thrown answer is "may", so the delegate's requiresExclusive
             * is still asked. Unguarded, the throw escaped through requiresExclusive into the
             * harness's guard for THAT hook, which reads a throw as "not exclusive" — the
             * fail-dangerous direction, silently losing the delegate's isolation.
             */
            final AdmissionPolicy failing = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }

                @Override
                public boolean mayRequireExclusive() {
                    throw new IllegalStateException("flag store unavailable");
                }
            };
            final AdaptiveSizingPolicy policy = sizing(0).policy(failing);

            assertThat(policy.requiresExclusive(task("Solo", 10)))
                    .as("the delegate's isolation survives its broken mayRequireExclusive")
                    .isTrue();
            assertThat(policy.requiresExclusive(task("Other", 10))).isFalse();

            // The convention covers Errors short of a VirtualMachineError too (a lazily linked
            // flag store throws NoClassDefFoundError; a test-double, AssertionError) — and the
            // constructor now asks the hook for the first time, where an escape would abort
            // executor wiring instead of degrading to full scans.
            final AdmissionPolicy linkageBroken = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }

                @Override
                public boolean mayRequireExclusive() {
                    throw new NoClassDefFoundError("flags/FlagStore");
                }
            };
            final AdaptiveSizingPolicy stillWired = sizing(0).policy(linkageBroken);
            assertThat(stillWired.requiresExclusive(task("Solo", 10)))
                    .as("an Error from the hook is \"may\" as well, at wiring time and after")
                    .isTrue();
        }

        @Test
        @DisplayName("a delegate whose mayRequireExclusive answer changes is honoured from the next scan, not frozen at construction")
        void delegateMayIsolateIsReadPerScan() {

            /*
             * The SPI fixes mayRequireExclusive for the length of one scan and expressly
             * invites dynamic answers (a mutable composite, a feature-flagged isolator). A
             * decorator that caches the answer at construction skips the delegate's
             * requiresExclusive for ever after — the silent-isolation-loss failure mode this
             * class's own javadoc warns wrappers about. Refreshing on onScanStart — the
             * harness's per-scan announcement — keeps the per-candidate path to one volatile
             * read and mayRequireExclusive a pure query.
             */
            final java.util.concurrent.atomic.AtomicBoolean isolating = new java.util.concurrent.atomic.AtomicBoolean(false);
            final AdmissionPolicy flagged = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return isolating.get() && "Solo".equals(task.name);
                }

                @Override
                public boolean mayRequireExclusive() {
                    return isolating.get();
                }
            };
            final AdaptiveSizingPolicy policy = sizing(0).policy(flagged);
            final ThrottledTask<Runnable> solo = task("Solo", 10);

            assertThat(policy.requiresExclusive(solo)).isFalse();

            isolating.set(true);
            policy.onScanStart();                                   // the harness announces every scan
            assertThat(policy.requiresExclusive(solo))
                    .as("the flag flipped after construction; from the next scan the delegate is asked")
                    .isTrue();

            isolating.set(false);
            policy.onScanStart();
            assertThat(policy.requiresExclusive(solo)).as("and back off at the following scan").isFalse();
        }

        @Test
        @DisplayName("while an unknown runs, the harness admits nothing beside it; after it ends, admission resumes")
        void quarantine() {
            // The isolation moved from this decorator's private census onto the framework's
            // requiresExclusive, so the guarantee is pinned where it now lives — through the
            // harness, rather than by asserting that admit() itself says no.
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "known", -1, 800);
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(delegate);
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> unknown = task("never-seen", -1);
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(unknown)), policy)).isSameAs(unknown);

            final ArrayDeque<ThrottledTask<Runnable>> others =
                    new ArrayDeque<>(List.of(task("known", -1), task("declared", 10)));
            assertThat(harness.admitFirst(others, policy)).isNull();
            assertThat(others).as("nothing was claimed").hasSize(2);
            assertThat(delegate.admitted).as("the delegate was never consulted").isEmpty();

            harness.onEnd(unknown, policy);
            assertThat(harness.admitFirst(others, policy)).isNotNull();
        }

        @Test
        @DisplayName("an unknown in flight reserves unknownReserveBytes in the delegate's ledger")
        void unknownReservesTheConfiguredBytes() {
            final AdaptiveSizing sizing = sizing(12_345);
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1L << 30, 1.0);
            final RecordingPolicy recorder = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(CompositePolicy.allOf(budget, recorder));
            final ThrottledTask<Runnable> unknown = task("never-seen", -1);

            policy.onTaskStart(unknown);
            assertThat(budget.inFlightBytes()).isEqualTo(12_345);
            assertThat(recorder.started.get(0).size).isEqualTo(12_345);
            assertThat(recorder.started.get(0).name).isEqualTo("never-seen");
            assertThat(policy.inFlightTaskCount()).isEqualTo(1);

            policy.onTaskEnd(unknown);
            assertThat(budget.inFlightBytes()).isZero();
        }

        @Test
        @DisplayName("an unnamed task is invisible to the decorator: nothing can be learned about it, so the delegate sees it untouched")
        void unnamedTaskIsForwardedUntouched() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);
            final ThrottledTask<Runnable> unnamed = task(null, -1);

            assertThat(policy.admit(unnamed, snapshotOf(0, 0))).isTrue();
            assertThat(delegate.admitted).containsExactly(unnamed);

            policy.onTaskStart(unnamed);
            assertThat(delegate.started).containsExactly(unnamed);
            assertThat(policy.admit(task("known", 10), snapshotOf(1, 0))).as("no quarantine for it").isTrue();
            policy.onTaskEnd(unnamed);
            assertThat(delegate.ended).containsExactly(unnamed);
        }

        @Test
        @DisplayName("each policy() call has its own ledger: what one has in flight does not reach another")
        void eachPolicyHasItsOwnLedger() {
            final AdaptiveSizing sizing = sizing();
            final RecordingPolicy d1 = new RecordingPolicy();
            final RecordingPolicy d2 = new RecordingPolicy();
            final AdaptiveSizingPolicy p1 = sizing.policy(d1);
            final AdaptiveSizingPolicy p2 = sizing.policy(d2);
            assertThat(p1).isNotSameAs(p2);

            p1.onTaskStart(task("never-seen", -1));

            assertThat(p1.inFlightTaskCount()).isEqualTo(1);
            assertThat(p2.inFlightTaskCount()).as("idle ledger: the delegate's -1, so the harness floors on its tree").isEqualTo(-1);
            assertThat(p2.admit(task("declared", 10), snapshotOf(1, 0)))
                    .as("the other ledger is empty, so nothing about p1's work reaches it")
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("the wrapped snapshot")
    class WrappedSnapshot {

        @Test
        @DisplayName("one wrapper per scan: every candidate offered against the same raw snapshot shares it")
        void oneWrapperPerScan() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 800);
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing.policy(delegate);
            final PoolSnapshot scan = snapshotOf(1, 0);

            policy.admit(task("job", -1), scan);
            policy.admit(task("job", -1), scan);
            policy.admit(task("job", -1), snapshotOf(1, 0));     // the next scan

            assertThat(delegate.snapshots).hasSize(3);
            assertThat(delegate.snapshots.get(1)).isSameAs(delegate.snapshots.get(0));
            assertThat(delegate.snapshots.get(2)).isNotSameAs(delegate.snapshots.get(0));

            policy.onTaskStart(task("job", -1));                  // the ledger moved: a fresh wrapper even for the old scan
            policy.admit(task("job", -1), scan);
            assertThat(delegate.snapshots.get(3)).isNotSameAs(delegate.snapshots.get(0));
            assertThat(delegate.snapshots.get(3).trackedSizeBytes()).isEqualTo(1_000);
        }

        @Test
        @DisplayName("trackedSizeBytes is the decorator's sum of EFFECTIVE sizes in flight, not the harness's declared sum")
        void trackedBytesAreEffectiveSizes() {
            final AdaptiveSizing sizing = sizing();
            final AdaptiveSizingPolicy policy = sizing.policy(new MemoryBudgetPolicy(10, 1.0));   // admits while tracked + size < 10
            final ThrottledTask<Runnable> running = task("a", 6);
            policy.onTaskStart(running);

            assertThat(policy.admit(task("b", 5), snapshotOf(1, 0)))
                    .as("raw snapshot says 0 tracked (undecorated would admit 0 + 5 < 10); own sum is 6, 6 + 5 >= 10")
                    .isFalse();
            assertThat(policy.admit(task("c", 3), snapshotOf(1, 0))).as("6 + 3 < 10").isTrue();

            policy.onTaskEnd(running);
            assertThat(policy.admit(task("b", 5), snapshotOf(0, 0))).isTrue();
        }

        @Test
        @DisplayName("a learned size counts at its learned value in the wrapped sum")
        void learnedSizesCountAtTheirLearnedValue() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "job", -1, 8);                  // 8 * 1.25 = 10
            final AdaptiveSizingPolicy policy = sizing.policy(new MemoryBudgetPolicy(100, 1.0));
            final ThrottledTask<Runnable> running = task("job", -1);
            policy.onTaskStart(running);

            assertThat(policy.admit(task("x", 89), snapshotOf(1, 0))).as("10 + 89 < 100").isTrue();
            assertThat(policy.admit(task("x", 90), snapshotOf(1, 0))).as("10 + 90 = 100, not < 100").isFalse();
            policy.onTaskEnd(running);
        }

        @Test
        @DisplayName("runningTasks passes through from the harness snapshot")
        void runningTasksPassesThrough() {
            final RecordingPolicy delegate = new RecordingPolicy();
            final AdaptiveSizingPolicy policy = sizing().policy(delegate);

            policy.admit(task("x", 1), snapshotOf(7, 123));

            assertThat(delegate.snapshots.get(0).runningTasks()).isEqualTo(7);
            assertThat(delegate.snapshots.get(0).trackedSizeBytes()).as("nothing in flight in the decorator").isZero();
            assertThat(sizing().policy(new MaxConcurrencyPolicy(2)).admit(task("x", 1), snapshotOf(2, 0))).isFalse();
            assertThat(sizing().policy(new MaxConcurrencyPolicy(2)).admit(task("x", 1), snapshotOf(1, 0))).isTrue();
        }
    }

    @Nested
    @DisplayName("selection order")
    class SelectionOrder {

        @Test
        @DisplayName("unknowns come first, then effective size descending")
        void unknownsFirstThenEffectiveDescending() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "big", -1, 8_000);              // 10 000
            learn(sizing, "read", 100, 200);              // ratio 2.0
            final AdaptiveSizingPolicy policy = sizing.policy(new RecordingPolicy());
            final List<ThrottledTask<Runnable>> tasks = new ArrayList<>(List.of(
                    task("declared", 5_000), task("big", -1), task("never-seen", -1), task("read", 1_000), task("small", 10)));

            tasks.sort(policy.selectionOrder());

            // never-seen: UNKNOWN; big: 10 000; declared: 5 000 (unlearned, as declared); read: 1 000 * 2 * 1.25 = 2 500; small: 10
            assertThat(tasks).extracting(t -> t.name).containsExactly("never-seen", "big", "declared", "read", "small");
        }

        @Test
        @DisplayName("is a consistent comparator over a large mixed batch")
        void consistentOverALargeMixedBatch() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "a", -1, 100);
            learn(sizing, "b", 10, 30);
            final AdaptiveSizingPolicy policy = sizing.policy(new RecordingPolicy());
            final Random random = new Random(42);
            final String[] names = {"a", "b", "c", null};
            final List<ThrottledTask<Runnable>> tasks = new ArrayList<>();
            for (int i = 0; i < 1_000; i++) {
                tasks.add(task(names[random.nextInt(names.length)], random.nextInt(4) == 0 ? -1 : random.nextInt(10_000)));
            }
            final Comparator<ThrottledTask<?>> order = policy.selectionOrder();

            tasks.sort(order);                            // TimSort throws on an inconsistent comparator

            boolean seenKnown = false;
            long previous = Long.MAX_VALUE;
            for (ThrottledTask<Runnable> t : tasks) {
                final long effective = sizing.effectiveSize(t.name, t.size);
                if (t.name != null && effective == AdaptiveSizing.UNKNOWN) {      // an unnamed -1 is declared, not unknown
                    assertThat(seenKnown).as("an unknown after a known: %s", t).isFalse();
                } else {
                    seenKnown = true;
                    assertThat(effective).isLessThanOrEqualTo(previous);
                    previous = effective;
                }
            }
            for (int i = 0; i < 200; i++) {
                final ThrottledTask<Runnable> x = tasks.get(random.nextInt(tasks.size()));
                final ThrottledTask<Runnable> y = tasks.get(random.nextInt(tasks.size()));
                assertThat(Integer.signum(order.compare(x, y))).isEqualTo(-Integer.signum(order.compare(y, x)));
            }
        }

        @Test
        @DisplayName("each comparator fixes the sizes it has seen, so learning during a sort cannot break it; a fresh comparator sees the new sizes")
        void comparatorMemoizesPerInstance() {
            final AdaptiveSizing sizing = sizing();
            learn(sizing, "a", -1, 1_000);
            learn(sizing, "b", -1, 500);
            final AdaptiveSizingPolicy policy = sizing.policy(new RecordingPolicy());
            final ThrottledTask<Runnable> a = task("a", -1);
            final ThrottledTask<Runnable> b = task("b", -1);
            final Comparator<ThrottledTask<?>> order = policy.selectionOrder();
            assertThat(order.compare(a, b)).isNegative();

            learn(sizing, "b", -1, 5_000);

            assertThat(order.compare(a, b)).as("same comparator: unchanged").isNegative();
            assertThat(policy.selectionOrder().compare(a, b)).as("fresh comparator: b is bigger now").isPositive();
        }
    }

    @Nested
    @DisplayName("identity")
    class Identity {

        /** Drives the policy the way the harness does: consistent snapshot, start on admit, end later. */
        private List<Boolean> script(AdmissionPolicy policy, List<ThrottledTask<Runnable>> tasks) {
            final List<Boolean> answers = new ArrayList<>();
            final List<ThrottledTask<Runnable>> running = new ArrayList<>();
            final Random random = new Random(7);
            for (ThrottledTask<Runnable> t : tasks) {
                long tracked = 0;
                for (ThrottledTask<Runnable> r : running) {
                    tracked += r.size;
                }
                final boolean admitted = policy.admit(t, snapshotOf(running.size(), tracked));
                answers.add(admitted);
                if (admitted) {
                    policy.onTaskStart(t);
                    running.add(t);
                }
                if (!running.isEmpty() && random.nextInt(3) == 0) {
                    policy.onTaskEnd(running.remove(random.nextInt(running.size())));
                }
            }
            for (ThrottledTask<Runnable> r : running) {
                policy.onTaskEnd(r);
            }
            return answers;
        }

        @Test
        @DisplayName("with an empty store and declared sizes, the decorator answers exactly like the undecorated composite")
        void emptyStoreIsIdentity() {
            final Random random = new Random(11);
            final List<ThrottledTask<Runnable>> tasks = new ArrayList<>();
            for (int i = 0; i < 500; i++) {
                tasks.add(task("t" + i, random.nextInt(400)));
            }
            final AdmissionPolicy undecorated = CompositePolicy.allOf(new MemoryBudgetPolicy(1_000, 1.0), new MaxConcurrencyPolicy(3));
            final AdmissionPolicy decorated = sizing().policy(CompositePolicy.allOf(new MemoryBudgetPolicy(1_000, 1.0), new MaxConcurrencyPolicy(3)));

            final List<Boolean> expected = script(undecorated, tasks);
            final List<Boolean> actual = script(decorated, tasks);

            assertThat(actual).isEqualTo(expected);
            assertThat(expected).contains(true, false);
        }
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("policy(null) is rejected")
        void rejectsNullDelegate() {
            assertThatThrownBy(() -> sizing().policy(null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("the delegate is exposed")
        void exposesDelegate() {
            final RecordingPolicy delegate = new RecordingPolicy();

            assertThat(sizing().policy(delegate).delegate()).isSameAs(delegate);
        }
    }
}
