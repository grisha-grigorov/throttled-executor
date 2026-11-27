package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The pass-through base for decorator authors. {@code AdmissionPolicy}'s defaults are wrong
 * inside a wrapper — {@code inFlightTaskCount} answers {@code -1} for a delegate with a
 * ledger, {@code mayRequireExclusive} reflects on the wrapper's own class, {@code onScanStart}
 * swallows the announcement a member hangs refreshes on — and the forwarding checklist has
 * grown twice since the SPI shipped, biting this library's own decorator both times. The base
 * forwards everything; the reflective sweep at the bottom refuses a future SPI method until
 * the base declares it, and these behavioural tests are what pin that the declarations
 * actually forward (or, for the one derived answer, derive).
 */
@DisplayName("the forwarding base")
class ForwardingAdmissionPolicyTests {

    /** Answers no default would give, so a dropped forward is unmistakable. */
    private final class Distinctive implements AdmissionPolicy {
        final List<String> calls = new ArrayList<>();
        final Comparator<ThrottledTask<?>> order = Comparator.comparingLong(t -> t.size);

        @Override
        public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
            calls.add("admit");
            return false;
        }

        @Override
        public void onTaskStart(ThrottledTask<?> task) {
            calls.add("onTaskStart");
        }

        @Override
        public void onTaskEnd(ThrottledTask<?> task) {
            calls.add("onTaskEnd");
        }

        @Override
        public void onScanStart() {
            calls.add("onScanStart");
        }

        @Override
        public Comparator<ThrottledTask<?>> selectionOrder() {
            return order;
        }

        @Override
        public int inFlightTaskCount() {
            return 7;
        }

        @Override
        public boolean requiresExclusive(ThrottledTask<?> task) {
            calls.add("requiresExclusive");
            return true;
        }

        @Override
        public boolean mayRequireExclusive() {
            calls.add("mayRequireExclusive");
            return true;
        }

        @Override
        public boolean ledgerSpansExecutors() {
            return true;
        }
    }

    @Test
    @DisplayName("every SPI method reaches the delegate, answers included")
    void everyMethodForwards() {

        final Distinctive delegate = new Distinctive();
        final AdmissionPolicy wrapper = new ForwardingAdmissionPolicy(delegate) {
        };
        final ThrottledTask<Runnable> task = ThrottledTask.of(() -> { }, 10L, "A");

        assertThat(wrapper.admit(task, null)).isFalse();
        wrapper.onTaskStart(task);
        wrapper.onTaskEnd(task);
        wrapper.onScanStart();
        assertThat(wrapper.selectionOrder()).isSameAs(delegate.order);
        assertThat(wrapper.inFlightTaskCount()).isEqualTo(7);
        assertThat(wrapper.requiresExclusive(task)).isTrue();
        assertThat(wrapper.mayRequireExclusive())
                .as("forwarded, not derived from the wrapper's own class")
                .isTrue();
        assertThat(wrapper.ledgerSpansExecutors()).isTrue();

        assertThat(delegate.calls).containsExactly(
                "admit", "onTaskStart", "onTaskEnd", "onScanStart", "requiresExclusive", "mayRequireExclusive");
    }

    @Test
    @DisplayName("a bare extension is exactly the delegate — no default leaks through")
    void bareExtensionEqualsDelegate() {

        // The trap the base exists to close: a pass-through built on the interface inherits
        // defaults that answer for the WRAPPER (mayRequireExclusive reflects on its class,
        // inFlightTaskCount says "no ledger"), silently discarding what the delegate said.
        final Distinctive delegate = new Distinctive();
        final AdmissionPolicy plainPassThrough = (candidate, snapshot) -> delegate.admit(candidate, snapshot);
        final AdmissionPolicy based = new ForwardingAdmissionPolicy(delegate) {
        };

        assertThat(plainPassThrough.mayRequireExclusive()).as("the interface default: wrong").isFalse();
        assertThat(plainPassThrough.inFlightTaskCount()).as("the interface default: wrong").isEqualTo(-1);
        assertThat(based.mayRequireExclusive()).isTrue();
        assertThat(based.inFlightTaskCount()).isEqualTo(7);
    }

    @Test
    @DisplayName("a subclass adding its own isolation is scanned, even over a delegate that never isolates")
    void subclassIsolationSurvivesANeverIsolatingDelegate() {

        /*
         * The trap a pure forward digs: the interface's default DERIVES the answer from the
         * runtime class ("does it override requiresExclusive?"), so isolation can never be
         * lost by omission — but a forwarded answer is the DELEGATE's, and a subclass that
         * adds isolation over a never-isolating delegate would answer false, so the harness
         * never asks requiresExclusive and the isolation vanishes with nothing logged. The
         * pair is coupled: forward mayRequireExclusive only while requiresExclusive is also
         * still the forwarded one; a subclass declaring its own re-derives "may".
         */
        final AdmissionPolicy neverIsolates = (candidate, snapshot) -> true;
        assertThat(neverIsolates.mayRequireExclusive()).isFalse();

        final AdmissionPolicy isolating = new ForwardingAdmissionPolicy(neverIsolates) {
            @Override
            public boolean requiresExclusive(ThrottledTask<?> task) {
                return "Solo".equals(task.name);
            }
        };
        assertThat(isolating.mayRequireExclusive())
                .as("the subclass declared its own requiresExclusive: it may isolate, whatever the delegate says")
                .isTrue();

        final AdmissionPolicy passThrough = new ForwardingAdmissionPolicy(neverIsolates) {
        };
        assertThat(passThrough.mayRequireExclusive())
                .as("a pure pass-through keeps the delegate's fast path")
                .isFalse();

        // And through the harness: the subclass's isolation actually isolates.
        final ThrottleHarness harness = new ThrottleHarness();
        final ThrottledTask<Runnable> busy = ThrottledTask.of(() -> { }, 10L, "Busy");
        assertThat(harness.admitFirst(new java.util.ArrayDeque<>(List.of(busy)), isolating)).isSameAs(busy);
        final ThrottledTask<Runnable> solo = ThrottledTask.of(() -> { }, 10L, "Solo");
        assertThat(harness.admitFirst(new java.util.ArrayDeque<>(List.of(solo)), isolating))
                .as("cannot be alone: refused, not admitted beside the running task")
                .isNull();
        harness.onEnd(busy, isolating);
    }

    @Test
    @DisplayName("the ledger scope is the delegate's explicit word — forwarded, never re-derived from the count")
    void ledgerScopeIsForwardedNotDerived() {

        /*
         * ledgerSpansExecutors picks whose parked claims the re-entrancy exemption counts;
         * "spans" is the permissive side (an exemption granted against parkers on other
         * trees), and every other reader in the family degrades to per-tree. A per-tree
         * delegate that reports a count and explicitly answers false must keep that false
         * through the base — a pass-through, and equally a subclass that overrides
         * inFlightTaskCount only to observe it. Re-deriving from the count on any override
         * silently widened exactly that shape to process-wide. A subclass reporting a
         * ledger of its own overrides the pair together; the base does not guess.
         */
        final AdmissionPolicy perTree = new AdmissionPolicy() {
            @Override
            public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                return true;
            }

            @Override
            public int inFlightTaskCount() {
                return 2;                                             // a ledger, per tree
            }

            @Override
            public boolean ledgerSpansExecutors() {
                return false;
            }
        };
        assertThat(perTree.ledgerSpansExecutors()).isFalse();

        final AdmissionPolicy passThrough = new ForwardingAdmissionPolicy(perTree) {
        };
        assertThat(passThrough.ledgerSpansExecutors())
                .as("count >= 0, delegate says per-tree: the word is forwarded, not re-derived")
                .isFalse();

        final AdmissionPolicy observing = new ForwardingAdmissionPolicy(perTree) {
            @Override
            public int inFlightTaskCount() {
                return super.inFlightTaskCount();                     // observe-and-forward
            }
        };
        assertThat(observing.ledgerSpansExecutors())
                .as("overriding the count to watch it must not widen the delegate's scope")
                .isFalse();

        final AdmissionPolicy ownPair = new ForwardingAdmissionPolicy(perTree) {
            @Override
            public int inFlightTaskCount() {
                return 3;
            }

            @Override
            public boolean ledgerSpansExecutors() {
                return true;                                          // the pair, overridden together
            }
        };
        assertThat(ownPair.ledgerSpansExecutors()).as("a subclass's own override always wins").isTrue();
    }

    @Test
    @DisplayName("core's delegators declare every non-static interface method — a future SPI addition fails here until each handles it (the handling itself is pinned by the behavioural tests above)")
    void coreDelegatorsCoverTheWholeInterface() throws NoSuchMethodException {

        // Each delegator swept once, in its own module: the base and the composite here,
        // the adaptive decorator in its satellite. An inherited default answers for the
        // wrapper, not the delegate — for a composite, for none of its members.
        for (Class<?> delegator : List.of(ForwardingAdmissionPolicy.class, CompositePolicy.class)) {
            for (Method spi : AdmissionPolicy.class.getMethods()) {
                if (Modifier.isStatic(spi.getModifiers())) {
                    continue;
                }
                final Method impl = delegator.getMethod(spi.getName(), spi.getParameterTypes());
                assertThat(impl.getDeclaringClass())
                        .as("%s must handle %s itself, not inherit the interface default",
                                delegator.getSimpleName(), spi.getName())
                        .isNotEqualTo(AdmissionPolicy.class);
            }
        }
    }
}
