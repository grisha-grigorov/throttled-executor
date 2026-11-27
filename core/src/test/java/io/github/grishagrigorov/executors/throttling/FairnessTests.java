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
 * The admission queue. {@code awaitStateChange} is a bare {@code signalAll}, so whichever
 * coordinator wakes first wins — every time. With several coordinators sharing a subtree's
 * harness that is not merely unfair but unbounded: a steady stream of work from one can keep
 * another's candidate out forever.
 */
@DisplayName("admission fairness")
class FairnessTests {

    @Nested
    @DisplayName("at rest")
    class AtRest {

        @Test
        @DisplayName("a coordinator that is never refused never queues, and never holds anyone up")
        void costsNothingWhenNobodyWaits() {

            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy admitAll = (candidate, snapshot) -> true;

            try (ThrottleHarness.Ticket a = harness.newTicket();
                 ThrottleHarness.Ticket b = harness.newTicket()) {

                for (int i = 0; i < 10; i++) {
                    assertThat(harness.admitFirst(queue("a" + i), admitAll, a)).isNotNull();
                    assertThat(harness.admitFirst(queue("b" + i), admitAll, b)).isNotNull();
                }
            }
        }
    }

    @Nested
    @DisplayName("under contention")
    class UnderContention {

        @Test
        @DisplayName("a coordinator refused often enough takes priority, and a newcomer then yields to it")
        void refusedCoordinatorGetsPriority() {

            final ThrottleHarness harness = new ThrottleHarness();
            final Gate gate = new Gate();

            try (ThrottleHarness.Ticket starved = harness.newTicket();
                 ThrottleHarness.Ticket newcomer = harness.newTicket()) {

                final ArrayDeque<ThrottledTask<Runnable>> waiting = queue("starved");

                // Refused while the gate is shut. Below the threshold nobody is yielded to.
                for (int i = 0; i < ThrottleHarness.PRIORITY_AFTER_REFUSALS - 1; i++) {
                    assertThat(harness.admitFirst(waiting, gate, starved)).isNull();
                }
                gate.open = true;
                assertThat(harness.admitFirst(queue("newcomer"), gate, newcomer))
                        .as("nobody has earned priority yet")
                        .isNotNull();
                gate.open = false;

                // One more refusal crosses the threshold.
                assertThat(harness.admitFirst(waiting, gate, starved)).isNull();
                gate.open = true;

                assertThat(harness.admitFirst(queue("newcomer-2"), gate, newcomer))
                        .as("someone has been waiting longer; yield")
                        .isNull();

                assertThat(harness.admitFirst(waiting, gate, starved))
                        .as("and the one who waited gets in")
                        .isNotNull();

                assertThat(harness.admitFirst(queue("newcomer-3"), gate, newcomer))
                        .as("queue drained, everyone proceeds")
                        .isNotNull();
            }
        }

        @Test
        @DisplayName("among coordinators with priority, the one that has waited longest goes first")
        void oldestPriorityWins() {

            final ThrottleHarness harness = new ThrottleHarness();
            final Gate gate = new Gate();

            // Something real is running on this harness. Fairness is an ordering among
            // coordinators competing for what THIS harness refills, so the yield reads this
            // harness's own count — the Gate's reported count alone only disables the floor.
            gate.open = true;
            final ThrottledTask<Runnable> busy = harness.admitFirst(queue("busy"), gate);
            gate.open = false;

            try (ThrottleHarness.Ticket first = harness.newTicket();
                 ThrottleHarness.Ticket second = harness.newTicket()) {

                final ArrayDeque<ThrottledTask<Runnable>> firstQueue = queue("first");
                final ArrayDeque<ThrottledTask<Runnable>> secondQueue = queue("second");

                for (int i = 0; i < ThrottleHarness.PRIORITY_AFTER_REFUSALS; i++) {
                    harness.admitFirst(firstQueue, gate, first);
                }
                for (int i = 0; i < ThrottleHarness.PRIORITY_AFTER_REFUSALS; i++) {
                    harness.admitFirst(secondQueue, gate, second);
                }

                gate.open = true;

                assertThat(harness.admitFirst(secondQueue, gate, second))
                        .as("second earned priority later, so it still yields")
                        .isNull();
                assertThat(harness.admitFirst(firstQueue, gate, first)).isNotNull();
                assertThat(harness.admitFirst(secondQueue, gate, second))
                        .as("now it is the oldest")
                        .isNotNull();
            }

            harness.onEnd(busy, gate);
        }

        @Test
        @DisplayName("a coordinator that walks away releases its place — an abandoned batch cannot wedge the harness")
        void abandonedTicketReleases() {

            final ThrottleHarness harness = new ThrottleHarness();
            final Gate gate = new Gate();

            // Something real is running on this harness. Fairness is an ordering among
            // coordinators competing for what THIS harness refills, so the yield reads this
            // harness's own count — the Gate's reported count alone only disables the floor.
            gate.open = true;
            final ThrottledTask<Runnable> busy = harness.admitFirst(queue("busy"), gate);
            gate.open = false;

            final ThrottleHarness.Ticket abandoned = harness.newTicket();
            final ArrayDeque<ThrottledTask<Runnable>> waiting = queue("abandoned");
            for (int i = 0; i < ThrottleHarness.PRIORITY_AFTER_REFUSALS; i++) {
                harness.admitFirst(waiting, gate, abandoned);
            }

            gate.open = true;

            try (ThrottleHarness.Ticket other = harness.newTicket()) {

                assertThat(harness.admitFirst(queue("other"), gate, other))
                        .as("still yielding to the one that waited")
                        .isNull();

                abandoned.close();

                assertThat(harness.admitFirst(queue("other-2"), gate, other))
                        .as("it gave up; nothing to yield to")
                        .isNotNull();
            }

            harness.onEnd(busy, gate);
        }
    }

    private static ArrayDeque<ThrottledTask<Runnable>> queue(String name) {
        return new ArrayDeque<>(List.of(ThrottledTask.of(() -> { }, 10L, name)));
    }

    /**
     * A policy that admits only while open, and reports work in flight so the liveness floor
     * does not admit past it.
     */
    private static final class Gate implements AdmissionPolicy {

        volatile boolean open;

        @Override
        public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
            return open;
        }

        @Override
        public int inFlightTaskCount() {
            return 1;
        }
    }
}
