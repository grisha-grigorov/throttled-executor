package io.github.grishagrigorov.executors.adaptive;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import java.time.Duration;
import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.ForwardingAdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The quarantine, expressed through the framework's {@code requiresExclusive} rather than
 * through a private census.
 *
 * <p>The old arrangement refused every candidate while an unknown was in flight and overrode
 * {@code inFlightTaskCount()} to redirect the liveness floor. It worked, but the floor is a
 * safety net rather than an isolation mechanism, and it came with a documented starvation
 * caveat: an unknown was only ever admitted by the floor, so a steady stream of known tasks
 * from another coordinator could keep it out indefinitely.</p>
 */
@DisplayName("quarantine through exclusive admission")
class QuarantineExclusivityTests {

    private static final long NM = Sample.NOT_MEASURED;

    @Nested
    @DisplayName("what the policy declares")
    class Declares {

        @Test
        @DisplayName("an unknown named task requires exclusivity; everything else does not")
        void unknownsAreExclusive() {

            final AdaptiveSizing sizing = sizing();
            sizing.store().record("known", new Sample(-1, 800, NM, 1L));
            final AdmissionPolicy policy = sizing.policy(admitAll());

            assertThat(policy.requiresExclusive(task("never-seen", -1))).isTrue();
            assertThat(policy.requiresExclusive(task("known", -1))).isFalse();
            assertThat(policy.requiresExclusive(task("declared", 1_000))).isFalse();
            assertThat(policy.requiresExclusive(task(null, -1)))
                    .as("an unnamed task can never be learned about, so isolating it buys nothing")
                    .isFalse();
        }

        @Test
        @DisplayName("the delegate's own isolation is honoured, not silently dropped")
        void delegateExclusivityIsForwarded() {

            /*
             * The decorator forwards admit, onTaskStart, onTaskEnd, selectionOrder and
             * inFlightTaskCount to its delegate — and answered requiresExclusive entirely on
             * its own account, so a delegate that isolates a task lost that isolation the
             * moment it was wrapped. policy(allOf(cap, myQuarantine)) admitted myQuarantine's
             * task beside up to `cap` others, with nothing logged: the interface default is
             * false, and false is a perfectly ordinary answer.
             */
            final AdaptiveSizing sizing = sizing();
            sizing.store().record("Solo", new Sample(-1, 800, NM, 1L));      // known: the decorator itself says no
            sizing.store().record("Other", new Sample(-1, 800, NM, 1L));

            final AdmissionPolicy isolatesSolo = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }
            };
            final AdmissionPolicy policy = sizing.policy(isolatesSolo);

            assertThat(policy.requiresExclusive(task("Solo", -1)))
                    .as("the delegate asked for isolation; wrapping it must not take that away")
                    .isTrue();
            assertThat(policy.requiresExclusive(task("Other", -1)))
                    .as("and a known task the delegate does not isolate stays ordinary")
                    .isFalse();
            assertThat(policy.mayRequireExclusive())
                    .as("the decorator always may — it quarantines unknowns on its own account")
                    .isTrue();
        }

        @Test
        @DisplayName("a decorator wrapping this policy keeps the quarantine only by forwarding requiresExclusive and mayRequireExclusive")
        void wrappingDecoratorMustForwardExclusivity() {

            /*
             * The harness asks requiresExclusive only when the OUTERMOST policy's class says
             * it may need it (ExclusivityOverride.declaredBy, or an explicit override). A
             * pass-through decorator that forwards admit/onTaskStart/onTaskEnd and nothing
             * else therefore hides the quarantine completely: admit() refuses the unknown,
             * the floor lets it in anyway, and known work is admitted beside it on the next
             * scan — the solo run never happens, nothing is logged. This pins the contract
             * the class javadoc states — and the way to satisfy it: extend core's
             * ForwardingAdmissionPolicy, which forwards the whole SPI by construction,
             * onScanStart included (the announcement this policy refreshes its delegate's
             * answer on — the forward a hand-rolled wrapper loses first).
             */
            final AdaptiveSizing sizing = sizing();
            final AtomicBoolean isolating = new AtomicBoolean(false);
            final AdmissionPolicy flagged = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return isolating.get() && "flagged-solo".equals(task.name);
                }

                @Override
                public boolean mayRequireExclusive() {
                    return isolating.get();
                }
            };
            final AdmissionPolicy inner = sizing.policy(flagged);

            final AdmissionPolicy forwards = new ForwardingAdmissionPolicy(inner) {
            };
            final AdmissionPolicy forwardsNothing = (candidate, snapshot) -> inner.admit(candidate, snapshot);

            assertThat(forwards.mayRequireExclusive()).isTrue();
            assertThat(forwardsNothing.mayRequireExclusive())
                    .as("the harness will never ask this one — the quarantine is silently gone")
                    .isFalse();

            final ThrottleHarness harness = new ThrottleHarness();
            final ThrottledTask<Runnable> unknown = task("never-seen", -1);
            final ArrayDeque<ThrottledTask<Runnable>> window =
                    new ArrayDeque<>(List.of(task("declared", 10), unknown));

            assertThat(harness.admitFirst(window, forwards))
                    .as("through a forwarding decorator the unknown is still isolated, and still offered first")
                    .isSameAs(unknown);
            assertThat(harness.admitFirst(window, forwards)).as("nothing runs beside it").isNull();
            harness.onEnd(unknown, forwards);

            // And because onScanStart is forwarded, a delegate that STARTS isolating after
            // construction is honoured through the wrapper too — the harness's next scan
            // announces, the adaptive member refreshes, the flagged task runs alone.
            isolating.set(true);
            sizing.store().record("flagged-solo", new Sample(-1, 800, NM, 1L));    // known: exclusivity is the delegate's, not the quarantine's
            final ThrottledTask<Runnable> flaggedSolo = task("flagged-solo", -1);
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("declared", 10), flaggedSolo)), forwards))
                    .as("admitted alone: the flipped delegate's isolation reached through the wrapper")
                    .isSameAs(flaggedSolo);
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("declared-b", 10))), forwards)).isNull();
        }

        @Test
        @DisplayName("the floor redirect is gone — the harness owns isolation now")
        void noLongerHijacksTheFloor() {

            final AdaptiveSizing sizing = sizing();
            final AdaptiveSizingPolicy policy = sizing.policy(admitAll());

            assertThat(policy.inFlightTaskCount())
                    .as("an empty ledger has nothing to add, and the delegate tracks nothing")
                    .isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("staying current")
    class StaysCurrent {

        @Test
        @DisplayName("a task that becomes known while it waits stops demanding exclusivity")
        void learningLandsBetweenScans() {

            /*
             * Driven through the harness on purpose. Calling requiresExclusive() twice in a row
             * asserted nothing about staleness — nothing in this class caches across such a
             * pair, so it passed with or without the thing it claimed to guard. What can go
             * stale is an answer held ACROSS SCANS: a candidate that was unknown when the first
             * scan asked, and that a sample has landed for by the second. Nothing caches it
             * today (see the note where the memo used to be), and this is what fails if
             * something starts to.
             */
            final AdaptiveSizing sizing = sizing();
            final AdmissionPolicy policy = sizing.policy(admitAll());
            final ThrottleHarness harness = new ThrottleHarness();

            // Something is already running, so an unknown cannot be admitted alone.
            final ThrottledTask<Runnable> busy = task("declared-busy", 10);
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), policy)).isSameAs(busy);

            final ThrottledTask<Runnable> learner = task("learner", -1);

            try (ThrottleHarness.Ticket mine = harness.newTicket()) {

                assertThat(harness.admitFirst(new ArrayDeque<>(List.of(learner)), policy, mine))
                        .as("unknown, and the pool is not empty — it has to wait to be measured")
                        .isNull();

                // Another run of the same task name finishes and its measurement lands.
                sizing.store().record("learner", new Sample(-1, 800, NM, 1L));

                assertThat(harness.admitFirst(new ArrayDeque<>(List.of(learner)), policy, mine))
                        .as("no longer an unknown: it goes in beside the running task, "
                                + "rather than holding the pool for a measurement it no longer needs")
                        .isSameAs(learner);
            }
        }
    }

    @Nested
    @DisplayName("in the harness")
    class InTheHarness {

        @Test
        @DisplayName("an unknown runs alone, and nothing else is admitted beside it")
        void unknownRunsAlone() {

            final AdaptiveSizing sizing = sizing();
            final AdmissionPolicy policy = sizing.policy(admitAll());
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> unknown = task("never-seen", -1);
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(unknown));

            assertThat(harness.admitFirst(queue, policy)).isSameAs(unknown);

            final ArrayDeque<ThrottledTask<Runnable>> others =
                    new ArrayDeque<>(List.of(task("declared-a", 10), task("declared-b", 10)));
            assertThat(harness.admitFirst(others, policy))
                    .as("the unknown owns the pool while it is measured")
                    .isNull();

            harness.onEnd(unknown, policy);
            assertThat(harness.admitFirst(others, policy)).as("released").isNotNull();
        }

        @Test
        @DisplayName("an unknown waiting behind running work is no longer starved by a busy neighbour")
        void unknownIsNotStarved() {

            final AdaptiveSizing sizing = sizing();
            final AdmissionPolicy policy = sizing.policy(admitAll());
            final ThrottleHarness harness = new ThrottleHarness();

            // Something is already running, so the unknown cannot be alone yet.
            final ThrottledTask<Runnable> busy = task("declared-busy", 10);
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), policy)).isSameAs(busy);

            try (ThrottleHarness.Ticket waiting = harness.newTicket()) {

                final ArrayDeque<ThrottledTask<Runnable>> quarantined =
                        new ArrayDeque<>(List.of(task("never-seen", -1)));
                assertThat(harness.admitFirst(quarantined, policy, waiting)).isNull();

                // The old caveat: a steady stream of admissible work from another coordinator
                // kept the unknown out forever, because only a moment of emptiness would let it
                // in and that moment never arrived. It does not get admitted now.
                for (int i = 0; i < 5; i++) {
                    assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("declared-" + i, 10))), policy))
                            .as("round %d: the pool must be allowed to drain", i)
                            .isNull();
                }

                harness.onEnd(busy, policy);

                assertThat(harness.admitFirst(quarantined, policy, waiting))
                        .as("the pool emptied, and the unknown was still first in line")
                        .isNotNull();
            }
        }

        @Test
        @DisplayName("an unknown on an idle root waits for known work on ANOTHER root sharing the policy")
        void unknownWaitsForASharedLedger() {

            /*
             * Two roots, one adaptive policy, so one ledger. Root A has known work in flight;
             * root B is idle and offers a never-seen task. Its solo run would be measured
             * against A's noise, and the reserve it charges lands in the ledger A is already
             * drawing on — the quarantine has to wait for the LEDGER to drain, not for B.
             */
            final AdaptiveSizing sizing = sizing();
            final AdmissionPolicy policy = sizing.policy(admitAll());
            final ThrottleHarness rootA = new ThrottleHarness();
            final ThrottleHarness rootB = new ThrottleHarness();

            final ThrottledTask<Runnable> busy = task("declared-busy", 10);
            assertThat(rootA.admitFirst(new ArrayDeque<>(List.of(busy)), policy)).isSameAs(busy);

            final ArrayDeque<ThrottledTask<Runnable>> quarantined =
                    new ArrayDeque<>(List.of(task("never-seen", -1)));
            assertThat(rootB.admitFirst(quarantined, policy))
                    .as("root B is idle, but the policy's ledger is not")
                    .isNull();

            rootA.onEnd(busy, policy);

            assertThat(rootB.admitFirst(quarantined, policy)).as("the ledger drained").isNotNull();
        }
    }

    @Nested
    @DisplayName("against a steady stream")
    class SteadyStream {

        @Test
        @DisplayName("an unknown gets its turn even when its own coordinator keeps offering known work behind it")
        void unknownIsNotStarvedByItsOwnBatch() {

            /*
             * The case the original starvation test missed. Its queue held the unknown ALONE, so
             * the coordinator was refused and earned priority. A real batch — and every window
             * the streaming overload produces — holds the unknown AND known work behind it. If
             * the coordinator keeps admitting from behind, it clears the very privilege that
             * makes the pool drain and competes with itself forever.
             */
            final AdaptiveSizing sizing = sizing();
            sizing.store().record("known", new Sample(-1, 800, NM, 1L));
            final AdmissionPolicy policy = sizing.policy(admitAll());
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> unknown = task("never-seen", -1);
            boolean unknownRan = false;
            ThrottledTask<Runnable> rival = null;

            try (ThrottleHarness.Ticket mine = harness.newTicket()) {

                for (int round = 0; round < 30 && !unknownRan; round++) {

                    // Another coordinator always has something in flight before this one scans.
                    if (rival == null) {
                        rival = harness.admitFirst(new ArrayDeque<>(List.of(task("known", -1))), policy);
                    }

                    final ArrayDeque<ThrottledTask<Runnable>> window =
                            new ArrayDeque<>(List.of(unknown, task("known", -1)));
                    final ThrottledTask<Runnable> admitted = harness.admitFirst(window, policy, mine);

                    if (admitted == unknown) {
                        unknownRan = true;
                    } else if (admitted != null) {
                        harness.onEnd(admitted, policy);
                    }

                    if (rival != null) {
                        harness.onEnd(rival, policy);
                        rival = null;
                    }
                }
            }

            assertThat(unknownRan)
                    .as("the quarantined task never got its solo slot — it was starved by known "
                            + "work its own coordinator kept admitting in front of it")
                    .isTrue();
        }
    }

    private static AdaptiveSizing sizing() {
        return AdaptiveSizing.of(new InMemoryProfileStore(16),
                new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 20, Duration.ZERO));
    }

    private static ThrottledTask<Runnable> task(String name, long size) {
        return ThrottledTask.of(() -> { }, size, name);
    }

    private static AdmissionPolicy admitAll() {
        return (candidate, snapshot) -> true;
    }
}
