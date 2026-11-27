package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import java.util.ArrayDeque;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * When the harness lets a nested coordinator claim past the policy.
 *
 * <p>The exemption exists for exactly one shape: a thread waiting for admission while
 * holding a claim that only it can release, with nothing else in flight that could release
 * anything either. Both halves are checked here, at the harness, on a thread the test
 * controls — the executor-level tests in {@code NestedSubmissionTests} cover the shapes that
 * reach it through {@code submitThrottled}.</p>
 */
@DisplayName("the re-entrancy exemption")
class ReentrancyExemptionTests {

    /** Refuses everything, and reports {@code inFlight} so the liveness floor does not apply. */
    private static AdmissionPolicy refusingWith(int inFlight) {
        return new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return false;
            }

            @Override
            public int inFlightTaskCount() {
                return inFlight;
            }
        };
    }

    private static ThrottledTask<Runnable> task(String name) {
        return ThrottledTask.of(() -> { }, 10L, name);
    }

    /** Parks {@code claims}-deep on {@code harness} until released; returns once it is parked. */
    private static Thread park(ThrottleHarness harness, int claims, CountDownLatch release) throws Exception {
        final CountDownLatch parked = new CountDownLatch(1);
        final Thread t = new Thread(() -> {
            java.util.concurrent.Callable<Void> body = () -> {
                parked.countDown();
                try {
                    // A long poll; the test releases it by signalling a completion.
                    harness.awaitStateChange(harness.stateVersion(), 10_000, true);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                return null;
            };
            for (int i = 0; i < claims; i++) {
                body = ThrottleHarness.holdingAClaim(body);
            }
            try {
                body.call();
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, "parked-holder");
        t.setDaemon(true);
        t.start();
        assertThat(parked.await(5, TimeUnit.SECONDS)).isTrue();
        Thread.sleep(50);       // past the version check and into the wait
        return t;
    }

    /**
     * Wakes the parked holder and waits for it to leave — the parked count it contributes to is
     * process-wide, so a holder still on its way out would leak into the next test.
     */
    private static void unpark(ThrottleHarness harness, Thread holder) throws Exception {
        harness.onEnd(task("wake"), refusingWith(1));
        holder.join(5_000);
        assertThat(holder.isAlive()).as("the parked holder left").isFalse();
    }

    @Nested
    @DisplayName("whose parked claims count")
    class ParkedScope {

        @Test
        @DisplayName("the scope is the policy's explicit choice, defaulting to the ledger's reach")
        void scopeDefaultsFollowTheLedger() {

            final AdmissionPolicy noLedger = (candidate, snapshot) -> true;
            assertThat(noLedger.ledgerSpansExecutors())
                    .as("no ledger: nothing reaches past this harness")
                    .isFalse();
            assertThat(refusingWith(0).ledgerSpansExecutors())
                    .as("a reported count is read as a cross-executor ledger unless the policy says otherwise")
                    .isTrue();
            assertThat(GlobalMemoryBudgetPolicy.processWideHeapBudget().ledgerSpansExecutors())
                    .as("the global budget says so explicitly")
                    .isTrue();
        }

        @Test
        @DisplayName("the scope is read only when an exemption is actually weighed — never on an ordinary scan")
        void scopeIsNotReadOnOrdinaryScans() {

            /*
             * ledgerSpansExecutors feeds one comparison: the exempt conjunct, reached only by
             * a nested coordinator that asked for the exemption while holding a claim. On the
             * default implementation the answer re-invokes inFlightTaskCount() — a second
             * ledger-lock acquisition per scan, under the harness admission lock, for a value
             * every ordinary scan discards.
             */
            final AtomicInteger scopeReads = new AtomicInteger();
            final AdmissionPolicy counting = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean ledgerSpansExecutors() {
                    scopeReads.incrementAndGet();
                    return false;
                }
            };
            final ThrottleHarness harness = new ThrottleHarness();

            final ThrottledTask<Runnable> a =
                    harness.admitFirst(new ArrayDeque<>(List.of(task("A"))), counting);
            assertThat(a).isNotNull();
            assertThat(harness.admitFirst(new ArrayDeque<>(List.of(task("B"))), counting)).isNotNull();
            assertThat(scopeReads).as("no exemption was requested, so the scope never mattered").hasValue(0);
        }

        @Test
        @DisplayName("a per-tree policy that reports a count keeps its parked accounting per harness")
        void perTreeLedgerKeepsLocalParkedScope() throws Exception {

            /*
             * The count and the scope are separate questions now. A policy tracking work on
             * this tree alone may report inFlightTaskCount() without silently opting into
             * process-wide parked-claims accounting: a holder parked on an UNRELATED harness
             * is not one of the tasks its ledger counts, so it must not tip this thread's
             * exemption. With the scope smuggled in the count's sign, exactly that happened.
             */
            final ThrottleHarness other = new ThrottleHarness();
            final CountDownLatch release = new CountDownLatch(1);
            final Thread holder = park(other, 1, release);          // parked ELSEWHERE

            abstract class PerTree implements AdmissionPolicy {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return false;
                }

                @Override
                public int inFlightTaskCount() {
                    return 2;                                       // this tree's own ledger
                }
            }
            final AdmissionPolicy perTree = new PerTree() {
                @Override
                public boolean ledgerSpansExecutors() {
                    return false;                                   // and it reaches no further
                }
            };
            final AdmissionPolicy spanning = new PerTree() {
            };

            final ThrottleHarness harness = new ThrottleHarness();
            final ThrottledTask<Runnable> running =
                    harness.admitFirst(new ArrayDeque<>(List.of(task("running"))), (c, s) -> true);
            assertThat(running).isNotNull();

            final AtomicReference<ThrottledTask<Runnable>> underPerTree = new AtomicReference<>();
            final AtomicReference<ThrottledTask<Runnable>> underSpanning = new AtomicReference<>();
            ThrottleHarness.holdingAClaim(() -> {
                // Two counted, one own claim: exempt only if one more counts as parked.
                underPerTree.set(harness.admitFirst(new ArrayDeque<>(List.of(task("nested"))), perTree, true));
                underSpanning.set(harness.admitFirst(new ArrayDeque<>(List.of(task("nested"))), spanning, true));
                return null;
            }).call();

            assertThat(underPerTree.get())
                    .as("the elsewhere-parked holder is outside this ledger's world: not exempt")
                    .isNull();
            assertThat(underSpanning.get())
                    .as("a spanning ledger counts that holder, and the exemption holds")
                    .isNotNull();

            harness.onEnd(underSpanning.get(), spanning);
            harness.onEnd(running, (c, s) -> true);
            unpark(other, holder);
        }
    }

    @Nested
    @DisplayName("who is exempt")
    class Who {

        @Test
        @DisplayName("a thread holding no claim is never exempt — even when everything in flight is parked")
        void noClaimNoExemption() throws Exception {

            /*
             * The half the bound alone cannot enforce. With one claim holder parked and the
             * policy counting exactly that one task, "everything in flight is waiting" is true
             * — but it is not THIS thread that is waiting on itself. A plain submit() body here
             * holds nothing; it was being handed the exemption on the strength of running inside
             * some task, and slipped one task past the policy ahead of the holder that actually
             * needed it.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final CountDownLatch release = new CountDownLatch(1);
            final Thread holder = park(harness, 1, release);

            assertThat(ThrottleHarness.claimsHeldByCurrentThread()).as("this thread holds nothing").isZero();

            final ThrottledTask<Runnable> admitted =
                    harness.admitFirst(new ArrayDeque<>(List.of(task("plain"))), refusingWith(1), true);

            assertThat(admitted)
                    .as("no claim, no exemption: the policy's refusal stands")
                    .isNull();

            unpark(harness, holder);
        }

        @Test
        @DisplayName("a claim holder is exempt exactly when everything in flight is parked or its own")
        void claimHolderIsExemptOnlyWhenNothingElseCouldRelease() throws Exception {

            final ThrottleHarness harness = new ThrottleHarness();
            final CountDownLatch release = new CountDownLatch(1);
            final Thread holder = park(harness, 1, release);        // one parked claim

            final AtomicReference<ThrottledTask<Runnable>> unrelatedRunning = new AtomicReference<>();
            final AtomicReference<ThrottledTask<Runnable>> deadlockShape = new AtomicReference<>();

            ThrottleHarness.holdingAClaim(() -> {
                // Counted: the parked claim + this thread's own = 2. Two in flight -> exempt.
                deadlockShape.set(harness.admitFirst(new ArrayDeque<>(List.of(task("nested"))), refusingWith(2), true));
                // Three in flight: something unrelated is running and will finish by itself.
                unrelatedRunning.set(harness.admitFirst(new ArrayDeque<>(List.of(task("nested"))), refusingWith(3), true));
                return null;
            }).call();

            assertThat(deadlockShape.get())
                    .as("nothing out there will ever release: the holder may place one task")
                    .isNotNull();
            assertThat(unrelatedRunning.get())
                    .as("unrelated work is in flight and will end on its own: wait for it like anyone else")
                    .isNull();

            unpark(harness, holder);
        }
    }
}
