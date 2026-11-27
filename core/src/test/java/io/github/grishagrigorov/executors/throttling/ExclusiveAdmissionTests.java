package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import java.util.ArrayDeque;
import java.util.List;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code requiresExclusive}: the harness's own notion of "this task must run alone", so a policy
 * that needs isolation does not have to build a private census and hijack the liveness floor to
 * get it.
 */
@DisplayName("exclusive admission")
class ExclusiveAdmissionTests {

    @Nested
    @DisplayName("the SPI")
    class Spi {

        @Test
        @DisplayName("nothing is exclusive unless a policy says so")
        void defaultIsFalse() {

            final AdmissionPolicy alwaysAdmit = (candidate, snapshot) -> true;

            assertThat(alwaysAdmit.requiresExclusive(task("A"))).isFalse();
        }

        @Test
        @DisplayName("a composite is exclusive if ANY member is — isolation is not something a member can veto")
        void compositeOrs() {

            final AdmissionPolicy plain = (candidate, snapshot) -> true;
            final AdmissionPolicy exclusive = exclusiveFor("Solo");

            final AdmissionPolicy composite = CompositePolicy.allOf(plain, exclusive);

            assertThat(composite.requiresExclusive(task("Solo"))).isTrue();
            assertThat(composite.requiresExclusive(task("Other"))).isFalse();
        }
    }

    @Nested
    @DisplayName("in the harness")
    class InTheHarness {

        @Test
        @DisplayName("an exclusive candidate waits for the pool to empty, then runs")
        void waitsForAnEmptyPool() {

            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");

            // Occupy the pool with an ordinary task.
            final ThrottledTask<Runnable> busy = task("Busy");
            final ArrayDeque<ThrottledTask<Runnable>> first = new ArrayDeque<>(List.of(busy));
            assertThat(harness.admitFirst(first, policy)).isSameAs(busy);

            final ThrottledTask<Runnable> solo = task("Solo");
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(solo));

            assertThat(harness.admitFirst(queue, policy))
                    .as("something else is running, so it cannot be alone")
                    .isNull();

            harness.onEnd(busy, policy);

            assertThat(harness.admitFirst(queue, policy)).as("now it can").isSameAs(solo);
            harness.onEnd(solo, policy);
        }

        @Test
        @DisplayName("while an exclusive task is in flight, nothing else is admitted — not even by the floor")
        void nothingElseRunsAlongside() {

            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");

            final ThrottledTask<Runnable> solo = task("Solo");
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(solo));
            assertThat(harness.admitFirst(queue, policy)).isSameAs(solo);

            final ArrayDeque<ThrottledTask<Runnable>> others = new ArrayDeque<>(List.of(task("A"), task("B")));
            assertThat(harness.admitFirst(others, policy)).isNull();
            assertThat(others).as("nothing was claimed").hasSize(2);

            harness.onEnd(solo, policy);

            assertThat(harness.admitFirst(others, policy)).as("released").isNotNull();
        }

        @Test
        @DisplayName("an exclusive candidate is admitted even by a policy that refuses it — exclusivity is the reason it is offered")
        void exclusiveBypassesAdmit() {

            final ThrottleHarness harness = new ThrottleHarness();

            // Refuses everything, but declares "Solo" exclusive. This is exactly the shape of
            // the adaptive quarantine: the unknown can never be admitted on its merits.
            final AdmissionPolicy policy = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return false;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }

                @Override
                public int inFlightTaskCount() {
                    return 0;       // a ledger of its own, and it is empty: the task may be alone
                }
            };

            final ThrottledTask<Runnable> solo = task("Solo");
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(solo));

            try (ThrottleHarness.Ticket ticket = harness.newTicket()) {
                assertThat(harness.admitFirst(queue, policy, ticket)).isSameAs(solo);
                assertThat(ticket.lastAdmissionByFloor())
                        .as("admitted on its exclusivity, not by the liveness floor overriding the refusal")
                        .isFalse();
            }
            harness.onEnd(solo, policy);
        }

        @Test
        @DisplayName("an ordinary candidate behind a blocked exclusive one is not admitted — that is what stops the exclusive one starving")
        void exclusiveDrainsThePool() {

            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");

            final ThrottledTask<Runnable> busy = task("Busy");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), policy)).isSameAs(busy);

            // A coordinator holds its ticket for the length of its batch — which is what gives
            // it a place in the queue at all. (The two-argument overload is for one-shot
            // callers and deliberately keeps no place.)
            try (ThrottleHarness.Ticket waiting = harness.newTicket()) {

                final ArrayDeque<ThrottledTask<Runnable>> blocked = new ArrayDeque<>(List.of(task("Solo")));
                assertThat(harness.admitFirst(blocked, policy, waiting)).isNull();

                // Another coordinator's ordinary work must now wait too, or the pool never empties.
                final ArrayDeque<ThrottledTask<Runnable>> ordinary = new ArrayDeque<>(List.of(task("A")));
                assertThat(harness.admitFirst(ordinary, policy)).isNull();
            }

            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("A"))), policy))
                    .as("the exclusive coordinator gave up, so ordinary work flows again")
                    .isNotNull();

            harness.onEnd(busy, policy);
        }
    }

    @Nested
    @DisplayName("against a steady stream of work")
    class NotStarved {

        @Test
        @DisplayName("a coordinator holding a blocked exclusive candidate stops admitting behind it, so the pool can drain")
        void exclusiveIsNotStarvedByItsOwnBatch() {

            // The failure this guards: a coordinator whose window holds an exclusive candidate
            // AND ordinary work keeps admitting the ordinary work, which calls ticket.satisfied()
            // and clears the privilege it needs. It then competes with itself forever — the pool
            // never empties, and the exclusive task never starts. A rival coordinator keeping one
            // task in flight at all times is what makes it permanent.
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");
            final ThrottledTask<Runnable> solo = task("Solo");

            boolean soloRan = false;
            ThrottledTask<Runnable> rival = null;

            try (ThrottleHarness.Ticket mine = harness.newTicket()) {

                for (int round = 0; round < 30 && !soloRan; round++) {

                    // The rival always has something in flight before this coordinator scans.
                    if (rival == null) {
                        rival = harness.admitFirst(new ArrayDeque<>(List.of(task("rival-" + round))), policy);
                    }

                    // This coordinator's window: the exclusive task, plus ordinary work behind it
                    // — refilled every round, the way the streaming overload refills.
                    final ArrayDeque<ThrottledTask<Runnable>> window =
                            new ArrayDeque<>(List.of(solo, task("mine-" + round)));
                    final ThrottledTask<Runnable> admitted = harness.admitFirst(window, policy, mine);

                    if (admitted == solo) {
                        soloRan = true;
                    } else if (admitted != null) {
                        harness.onEnd(admitted, policy);
                    }

                    if (rival != null) {
                        harness.onEnd(rival, policy);
                        rival = null;
                    }
                }
            }

            assertThat(soloRan)
                    .as("the exclusive task never got a turn: it was starved by work its own "
                            + "coordinator kept admitting in front of it")
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("the SPI — guarding and the cheap question")
    class SpiGuards {

        @Test
        @DisplayName("a composite asks EVERY member, even after one of them throws")
        void compositeGuardsEachMember() {

            /*
             * Every other hook in CompositePolicy is guarded per member, and the class says
             * why: one throwing member must not stop the hook reaching the members after it.
             * requiresExclusive was the exception. A member that NPEd on a task with no name
             * aborted the fan-out; the harness's own guard then read the whole composite as
             * "not exclusive", and the quarantine policy behind the buggy one was never asked.
             * A task that had to run alone ran beside everything, with one ERROR line naming
             * the wrong policy as the only trace.
             */
            final AdmissionPolicy buggy = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    throw new IllegalStateException("simulated member bug");
                }
            };

            final AdmissionPolicy composite = CompositePolicy.allOf(buggy, exclusiveFor("Solo"));

            assertThat(composite.requiresExclusive(task("Solo")))
                    .as("the member after the broken one still gets its isolation")
                    .isTrue();
            assertThat(composite.requiresExclusive(task("ordinary")))
                    .as("and nothing is invented for a task nobody isolates")
                    .isFalse();
        }

        @Test
        @DisplayName("mayRequireExclusive() is derived from whether requiresExclusive is overridden")
        void mayRequireExclusiveIsDerived() {

            /*
             * The question the harness asks ONCE per scan so it does not have to ask
             * requiresExclusive once per candidate. The default answer is derived, not hard-coded
             * false: a policy that overrides requiresExclusive and forgets this one must still
             * get the full scan, or its isolation silently disappears — the failure mode that
             * makes a plain `default false` unacceptable.
             */
            final AdmissionPolicy lambda = (candidate, snapshot) -> true;
            assertThat(lambda.mayRequireExclusive())
                    .as("a policy that never overrides requiresExclusive can never need it")
                    .isFalse();

            assertThat(exclusiveFor("Solo").mayRequireExclusive())
                    .as("overriding requiresExclusive alone is enough to opt in")
                    .isTrue();

            assertThat(CompositePolicy.allOf(lambda, lambda).mayRequireExclusive())
                    .as("a composite of policies that never isolate never isolates")
                    .isFalse();
            assertThat(CompositePolicy.allOf(lambda, exclusiveFor("Solo")).mayRequireExclusive())
                    .as("and one member that might is enough")
                    .isTrue();
        }
    }

    @Nested
    @DisplayName("what a scan costs")
    class ScanCost {

        @Test
        @DisplayName("a policy that can never isolate is not asked per candidate, and the scan stops at the first admission")
        void neverIsolatingPolicyGetsTheEarlyExit() {

            /*
             * The whole-window scan exists so a blocked exclusive candidate ANYWHERE in the
             * window can refuse the scan. It costs a requiresExclusive call per remaining
             * candidate under the harness lock — and for the List overload the window is the
             * entire remaining batch, which made a 20,000-task batch quadratic: ~200M calls,
             * giving back the exact throughput cliff this file's javadoc documents fixing.
             *
             * Most policies never isolate anything. They say so once, and pay nothing.
             */
            final java.util.concurrent.atomic.AtomicInteger asked = new java.util.concurrent.atomic.AtomicInteger();
            final AdmissionPolicy neverIsolates = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    asked.incrementAndGet();
                    return false;
                }

                @Override
                public boolean mayRequireExclusive() {
                    return false;
                }
            };

            final ArrayDeque<ThrottledTask<Runnable>> window = new ArrayDeque<>();
            for (int i = 0; i < 1_000; i++) {
                window.add(task("t" + i));
            }

            final ThrottleHarness harness = new ThrottleHarness();
            assertThat(harness.admitFirst(window, neverIsolates)).isNotNull();

            assertThat(asked).as("never asked per candidate").hasValue(0);
            assertThat(window).as("exactly one was claimed").hasSize(999);
        }

        @Test
        @DisplayName("a policy that MAY isolate is asked for the whole window — that cost is deliberate")
        void mayIsolatePolicyGetsTheFullScan() {

            final java.util.concurrent.atomic.AtomicInteger asked = new java.util.concurrent.atomic.AtomicInteger();
            final AdmissionPolicy mayIsolate = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    asked.incrementAndGet();
                    return false;
                }
            };

            final ArrayDeque<ThrottledTask<Runnable>> window = new ArrayDeque<>();
            for (int i = 0; i < 100; i++) {
                window.add(task("t" + i));
            }

            new ThrottleHarness().admitFirst(window, mayIsolate);

            assertThat(asked)
                    .as("every candidate is checked: an exclusive one behind the chosen one must still win")
                    .hasValue(100);
        }
    }

    @Nested
    @DisplayName("the scan-start hook")
    class ScanStart {

        /** Counts both lifecycle signals; never isolates on its own account. */
        final class Counting implements AdmissionPolicy {
            final java.util.concurrent.atomic.AtomicInteger scanStarts = new java.util.concurrent.atomic.AtomicInteger();
            final java.util.concurrent.atomic.AtomicInteger mayAsked = new java.util.concurrent.atomic.AtomicInteger();

            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public void onScanStart() {
                scanStarts.incrementAndGet();
            }

            @Override
            public boolean mayRequireExclusive() {
                mayAsked.incrementAndGet();
                return true;
            }
        }

        @Test
        @DisplayName("the default is a no-op, and the harness announces every non-exempt scan through it")
        void harnessAnnouncesEveryScan() {

            final AdmissionPolicy plain = (candidate, snapshot) -> true;
            plain.onScanStart();                                    // the default: nothing happens

            final Counting counting = new Counting();
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> a = task("A");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(a)), counting)).isSameAs(a);
            assertThat(counting.scanStarts).hasValue(1);

            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("B"))), counting)).isNotNull();
            assertThat(counting.scanStarts).as("once per scan").hasValue(2);
        }

        @Test
        @DisplayName("a composite announces the scan to EVERY member, while the pure may-isolate query still short-circuits")
        void compositeFansOutTheAnnouncement() {

            /*
             * The two are different in kind. onScanStart is a lifecycle hook — a member may
             * hang per-scan state refreshes on it (the adaptive decorator re-reads its
             * delegate's answer there), so it must reach every member, always.
             * mayRequireExclusive is a pure query again: once one member may isolate the
             * composite's answer is settled, and the rest need not be asked.
             */
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
            final Counting behind = new Counting();
            final AdmissionPolicy composite = CompositePolicy.allOf(alwaysMay, behind);

            composite.onScanStart();
            assertThat(behind.scanStarts).as("announced past the isolating member").hasValue(1);

            assertThat(composite.mayRequireExclusive()).isTrue();
            assertThat(behind.mayAsked).as("the settled query was not carried on").hasValue(0);
        }
    }

    @Nested
    @DisplayName("fairness against a foreign ledger")
    class ForeignLedger {

        @Test
        @DisplayName("a privileged waiter does not stall a locally idle harness because OTHER trees are busy")
        void privilegedWaiterDoesNotStallALocallyIdleHarness() {

            /*
             * The yield keyed on effectiveInFlight — the policy's own count when it reports one,
             * which for a shared GlobalMemoryBudgetPolicy spans every tree. Unrelated trees
             * keeping that count above zero made every peer on THIS harness yield to a
             * privileged ticket whose candidate the budget will never take, for as long as
             * those trees stayed busy: a locally empty harness admitting nothing. Fairness is an
             * ordering among coordinators sharing this harness, so what a peer refills is this
             * harness's own count — that is the one the yield has to read.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy budgetElsewhereBusy = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return !"never-fits".equals(candidate.name);
                }

                @Override
                public int inFlightTaskCount() {
                    return 1;       // another tree's work, in a ledger this policy shares
                }
            };

            try (ThrottleHarness.Ticket stuck = harness.newTicket()) {
                for (int i = 0; i <= ThrottleHarness.PRIORITY_AFTER_REFUSALS; i++) {
                    assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("never-fits"))), budgetElsewhereBusy, stuck))
                            .isNull();
                }

                // Nothing is running HERE. A peer with an admissible task must not be held
                // hostage to a waiter that nothing on this harness could ever satisfy.
                assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("peer"))), budgetElsewhereBusy))
                        .as("locally idle: the peer is admitted")
                        .isNotNull();
            }
        }

        @Test
        @DisplayName("an exclusive candidate waits for the ledger to empty, not just this tree — isolation spans what the policy counts")
        void exclusiveWaitsForAForeignLedger() {

            /*
             * "Runs alone" is a promise about the policy's accounting, not about one harness.
             * A GlobalMemoryBudgetPolicy shared by two roots has 800 MB of known work in flight
             * on root A; root B is idle and offers a task that must be alone. Admitting it on
             * B's own empty count starts it beside A's work against the same budget — the one
             * case admit() is never asked about. The liveness floor already keys on the
             * policy's count for exactly this reason; the exclusive gate must read the same.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final int[] elsewhere = {1};
            final AdmissionPolicy sharedLedger = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }

                @Override
                public int inFlightTaskCount() {
                    return elsewhere[0];
                }
            };

            final ThrottledTask<Runnable> solo = task("Solo");
            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(solo));

            assertThat(harness.admitFirst(queue, sharedLedger))
                    .as("another tree's work is in the ledger, so it cannot be alone")
                    .isNull();
            assertThat(queue).as("nothing was claimed").containsExactly(solo);

            elsewhere[0] = 0;

            assertThat(harness.admitFirst(queue, sharedLedger)).as("the ledger emptied").isSameAs(solo);
            harness.onEnd(solo, sharedLedger);
        }

        @Test
        @DisplayName("a policy's empty ledger does not blind the gate to work running on this tree under another policy")
        void exclusiveStillWaitsForLocalWorkThePolicyCannotSee() {

            /*
             * Two coordinators on one tree may run different policies. A task is running under
             * P1; a scan under P2, whose own ledger is empty, reports inFlightTaskCount() == 0.
             * "Alone" has to hold against BOTH counts — the ledger's and this harness's — or
             * the exclusive candidate starts beside work its policy simply cannot see, without
             * admit() ever being asked.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy p1 = (candidate, snapshot) -> true;
            final AdmissionPolicy p2 = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "Solo".equals(task.name);
                }

                @Override
                public int inFlightTaskCount() {
                    return 0;                                       // its own ledger; it never saw P1's task
                }
            };

            final ThrottledTask<Runnable> busy = task("Busy");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), p1)).isSameAs(busy);

            final ArrayDeque<ThrottledTask<Runnable>> queue = new ArrayDeque<>(List.of(task("Solo")));
            assertThat(harness.admitFirst(queue, p2))
                    .as("this tree is not empty, whatever the ledger says")
                    .isNull();

            harness.onEnd(busy, p1);

            assertThat(harness.admitFirst(queue, p2)).as("now it is").isNotNull();
        }

        @Test
        @DisplayName("an empty ledger does not blind the liveness floor either — the floor overrides a policy only on a genuinely idle system")
        void floorIsNotBlindedByAnEmptyLedger() {

            /*
             * The floor's contract: when the whole system is genuinely idle, both counts are
             * zero and the next candidate is admitted, so no policy bug can throttle to a
             * standstill. A ledger-reporting policy whose OWN ledger is empty on a tree busy
             * under another policy is not that: progress is being made, so the floor has no
             * standstill to break, and overriding the refusal admits refused work beside
             * running work.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy p1 = (candidate, snapshot) -> true;
            final AdmissionPolicy p2 = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return false;                                   // refuses everything
                }

                @Override
                public int inFlightTaskCount() {
                    return 0;                                       // its own ledger; it never saw P1's task
                }
            };

            final ThrottledTask<Runnable> busy = task("Busy");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), p1)).isSameAs(busy);

            final ArrayDeque<ThrottledTask<Runnable>> refused = new ArrayDeque<>(List.of(task("A")));
            assertThat(harness.admitFirst(refused, p2))
                    .as("the tree is busy: no standstill, no floor override")
                    .isNull();

            harness.onEnd(busy, p1);

            assertThat(harness.admitFirst(refused, p2))
                    .as("genuinely idle now: the floor keeps its liveness promise")
                    .isNotNull();
        }

        @Test
        @DisplayName("an exempt re-entrant scan never asks mayRequireExclusive — it takes the first candidate and reads nothing else")
        void exemptScanSkipsMayRequireExclusive() throws Exception {

            /*
             * The exempt path breaks on the first candidate before scanForExclusive is ever
             * read — and for the adaptive decorator that question is no longer a cheap class
             * lookup but a delegate walk plus a volatile store, under the harness lock, once
             * per nested submission. Work that is never consulted should not be done.
             */
            final java.util.concurrent.atomic.AtomicInteger asked = new java.util.concurrent.atomic.AtomicInteger();
            final AdmissionPolicy counting = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return false;
                }

                @Override
                public boolean mayRequireExclusive() {
                    asked.incrementAndGet();
                    return true;
                }

                @Override
                public void onScanStart() {
                    asked.incrementAndGet();
                }
            };
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> outer = task("Outer");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(outer)), counting)).isSameAs(outer);
            final int scans = asked.get();

            // Inside outer's body the thread holds its claim, so a nested scan asking for the
            // exemption is exempt: everything in flight is held by the scanning thread itself.
            ThrottleHarness.holdingAClaim(() -> {
                try (ThrottleHarness.Ticket nested = harness.newTicket()) {
                    final ArrayDeque<ThrottledTask<Runnable>> inner = new ArrayDeque<>(List.of(task("Inner")));
                    final ThrottledTask<Runnable> admitted = harness.admitFirst(inner, counting, nested, true);
                    assertThat(admitted).as("exempt: first candidate, no questions").isNotNull();
                    assertThat(asked).as("neither the question nor the announcement — the scan reads nothing").hasValue(scans);
                    harness.onEnd(admitted, counting);
                }
                return null;
            }).call();
            harness.onEnd(outer, counting);
        }
    }

    @Nested
    @DisplayName("holding the pool")
    class HoldingThePool {

        @Test
        @DisplayName("an exclusive task claimed twice keeps the pool until its LAST claim ends")
        void twoClaimsOfTheExclusiveInstance() throws Exception {

            /*
             * exclusiveInFlight is identity-compared, and the same instance can carry two
             * concurrent claims: the re-entrancy exemption can admit it from inside its own
             * body. The counter that was meant to cover that only incremented in the exclusive
             * branch — unreachable while an exclusive task is already in flight — while onEnd
             * decremented for the ordinary branch too, so the first end released the pool with
             * the other claim still running.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");
            final ThrottledTask<Runnable> solo = task("Solo");

            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(solo)), policy)).isSameAs(solo);

            // The same instance again, the way a nested submission from its own body would —
            // on a thread that actually holds the claim, which is what the harness checks.
            ThrottleHarness.holdingAClaim(() -> {
                try (ThrottleHarness.Ticket nested = harness.newTicket()) {
                    assertThat(harness.admitFirst(new ArrayDeque<>(List.of(solo)), policy, nested, true))
                            .as("its own nested work is exempt")
                            .isSameAs(solo);
                }
                return null;
            }).call();

            harness.onEnd(solo, policy);

            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("A"))), policy))
                    .as("one claim of the exclusive task is still running")
                    .isNull();

            harness.onEnd(solo, policy);

            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("A"))), policy))
                    .as("now it is really finished")
                    .isNotNull();
        }

        @Test
        @DisplayName("the admitted candidate is removed by identity, whatever equals() may one day say")
        void admittedCandidateIsRemovedByIdentity() {

            /*
             * A guard, not a repro: today ThrottledTask has no equals(), so by-value and
             * by-identity removal agree. The executor's position map is an IdentityHashMap,
             * and if the task ever became a record, a by-value remove would delete a
             * different-but-equal occurrence than the one claimed and hand a future to the
             * wrong batch position. This pins the harness to identity so that refactor fails
             * here rather than in production.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy admitAll = (candidate, snapshot) -> true;

            final ThrottledTask<Runnable> first = task("twin");
            final ThrottledTask<Runnable> second = task("twin");     // same name, size and shape
            final ArrayDeque<ThrottledTask<Runnable>> window = new ArrayDeque<>(List.of(first, second));

            assertThat(harness.admitFirst(window, admitAll)).isSameAs(first);
            assertThat(window).hasSize(1);
            assertThat(window.peekFirst()).as("the OTHER twin is the one left behind").isSameAs(second);
        }

        @Test
        @DisplayName("an exclusive candidate anywhere in the window stops the scan, not just one at the head")
        void exclusiveBehindOrdinaryWorkStillHoldsTheScan() {

            /*
             * The guard was a break on reaching a blocked exclusive candidate, which only
             * protects against admitting from BEHIND it. Anything ordered AHEAD of it is
             * admitted first and calls satisfied(), discarding the privilege that makes the pool
             * drain — and the streaming window re-sorts on every refill, so a size-ordered
             * comparator keeps pushing a small exclusive task backwards.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy policy = exclusiveFor("Solo");

            final ThrottledTask<Runnable> busy = task("Busy");
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), policy)).isSameAs(busy);

            try (ThrottleHarness.Ticket mine = harness.newTicket()) {

                // Ordinary work FIRST, the exclusive candidate behind it.
                final ArrayDeque<ThrottledTask<Runnable>> window =
                        new ArrayDeque<>(List.of(task("ordinary"), task("Solo")));

                assertThat(harness.admitFirst(window, policy, mine))
                        .as("nothing may be admitted while this window holds a blocked exclusive task")
                        .isNull();
                assertThat(window).as("nothing was claimed").hasSize(2);
            }

            harness.onEnd(busy, policy);
        }

        @Test
        @DisplayName("a privileged waiter does not outrank the liveness floor")
        void privilegedTicketYieldsToAnIdlePool() {

            /*
             * The privileged yield returned before the floor was even consulted. A coordinator
             * whose candidate can never fit stays privileged forever — it never calls
             * satisfied() — so it held the queue against peers whose candidates the floor would
             * have admitted. Nothing bounded how long that lasted.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;

            try (ThrottleHarness.Ticket stuck = harness.newTicket()) {

                // Earn priority: refused, with something running so the floor does not apply.
                final ThrottledTask<Runnable> busy = task("Busy");
                assertThat(harness.admitFirst(new ArrayDeque<>(List.of(busy)), refuseAll)).isSameAs(busy);
                for (int i = 0; i <= ThrottleHarness.PRIORITY_AFTER_REFUSALS; i++) {
                    harness.admitFirst(new ArrayDeque<>(List.of(task("never-fits"))), refuseAll, stuck);
                }
                harness.onEnd(busy, refuseAll);

                // Pool is empty now. The floor must still get a peer's task in.
                assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("peer"))), refuseAll))
                        .as("an idle pool admits the next candidate regardless of who is queued")
                        .isNotNull();
            }
        }
    }

    private static ThrottledTask<Runnable> task(String name) {
        return ThrottledTask.of(() -> { }, 10L, name);
    }

    /** Admits anything, but insists that tasks called {@code name} run alone. */
    private static AdmissionPolicy exclusiveFor(String name) {
        return new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public boolean requiresExclusive(ThrottledTask<?> task) {
                return name.equals(task.name);
            }
        };
    }
}
