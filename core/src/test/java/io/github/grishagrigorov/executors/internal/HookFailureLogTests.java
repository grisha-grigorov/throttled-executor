package io.github.grishagrigorov.executors.internal;


import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The rate limit in front of "a policy hook threw".
 *
 * <p>The flood this guards: {@code admit} is consulted on every candidate of every scan, and a
 * refused coordinator re-scans on every completion. A policy whose {@code admit} throws
 * therefore produced one ERROR line with a full stack trace per candidate per scan — thousands
 * per second under load, drowning the log that would have shown the cause.</p>
 */
@DisplayName("hook-failure logging")
class HookFailureLogTests {

    @Nested
    @DisplayName("one policy's flood does not silence another's first failure")
    class PerPolicy {

        @Test
        @DisplayName("each policy class gets its own log per hook")
        void keyedPerPolicyClass() {

            /*
             * The logs were one per hook per HARNESS, while two coordinators on one tree may
             * run different policies. Once one policy's admit() threw on a spin loop it held
             * the slot for a minute at a time; a second policy's first-ever failure inside that
             * minute was dropped — no message, no class name, no stack trace — and its count
             * merged into the other policy's "(n more like it went unlogged)" tail. The class
             * javadoc claimed the opposite guarantee.
             */
            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy a = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }
            };
            final AdmissionPolicy b = (candidate, snapshot) -> true;   // a different class

            final ThrottleHarness.HookFailureLog forA = harness.failureLog(ThrottleHarness.Hook.ADMIT, a);
            final ThrottleHarness.HookFailureLog forB = harness.failureLog(ThrottleHarness.Hook.ADMIT, b);

            assertThat(forA.claim()).as("A's first failure is logged").isZero();
            assertThat(forA.claim()).as("A's second, inside the quiet period, is not").isNegative();
            assertThat(forB.claim())
                    .as("B's FIRST failure is still logged — A's flood is A's problem")
                    .isZero();
        }

        @Test
        @DisplayName("the same policy class shares one log per hook; different hooks do not")
        void sameClassSharesAHook() {

            final ThrottleHarness harness = new ThrottleHarness();
            final AdmissionPolicy one = new MaxConcurrencyPolicy(1);
            final AdmissionPolicy two = new MaxConcurrencyPolicy(2);

            assertThat(harness.failureLog(ThrottleHarness.Hook.ADMIT, one))
                    .as("instances are often one per submit; the message names the class, so the class is the key")
                    .isSameAs(harness.failureLog(ThrottleHarness.Hook.ADMIT, two));

            assertThat(harness.failureLog(ThrottleHarness.Hook.ADMIT, one))
                    .as("a throwing admit must not silence a throwing onTaskEnd")
                    .isNotSameAs(harness.failureLog(ThrottleHarness.Hook.ON_TASK_END, one));
        }
    }

    @Nested
    @DisplayName("under a repeating failure")
    class Repeating {

        @Test
        @DisplayName("the first is logged in full and the rest are counted, not printed")
        void firstThroughThenQuiet() {

            final AtomicLong clock = new AtomicLong();
            final ThrottleHarness.HookFailureLog log =
                    new ThrottleHarness.HookFailureLog(TimeUnit.MINUTES.toNanos(1), clock::get);

            assertThat(log.claim()).as("the first failure is always logged").isZero();

            for (int i = 0; i < 5_000; i++) {
                clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(1));
                assertThat(log.claim()).as("still inside the quiet period").isNegative();
            }
        }

        @Test
        @DisplayName("the next line after the quiet period says how many were suppressed")
        void suppressedCountIsReported() {

            final AtomicLong clock = new AtomicLong();
            final ThrottleHarness.HookFailureLog log =
                    new ThrottleHarness.HookFailureLog(TimeUnit.MINUTES.toNanos(1), clock::get);

            assertThat(log.claim()).isZero();

            for (int i = 0; i < 17; i++) {
                assertThat(log.claim()).isNegative();
            }

            clock.addAndGet(TimeUnit.MINUTES.toNanos(2));

            assertThat(log.claim())
                    .as("the failures that went unlogged are still accounted for")
                    .isEqualTo(17);

            clock.addAndGet(TimeUnit.MINUTES.toNanos(2));

            assertThat(log.claim())
                    .as("and the count starts again from the last line, not from the beginning")
                    .isZero();
        }
    }
}
