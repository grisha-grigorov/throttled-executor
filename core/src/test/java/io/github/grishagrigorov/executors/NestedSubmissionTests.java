package io.github.grishagrigorov.executors;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.CompositePolicy;
import io.github.grishagrigorov.executors.throttling.GlobalMemoryBudgetPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A task body that submits more throttled work must always be able to finish.
 *
 * <p>Nested submission is a first-class pattern here — the executor tree, transitive
 * {@code waitForCompletion} and ambient discovery all exist to support it — and it is also the
 * one shape that can deadlock the throttle. The coordinator for the nested batch is running
 * <em>inside</em> a task whose claim the harness still holds, so if the policy refuses, the only
 * thread that could ever release that claim is the thread now waiting for admission.</p>
 *
 * <p>The guarantee is therefore stronger than the liveness floor's "when nothing is running":
 * a <b>re-entrant</b> coordinator is always allowed to make progress, one task at a time.</p>
 *
 * <p>Every test here runs the scenario on its own thread with a bounded join, so a regression
 * fails the build instead of hanging it.</p>
 */
@DisplayName("nested throttled submission")
class NestedSubmissionTests {

    /** Long enough that a slow machine is not mistaken for a deadlock, short enough to notice. */
    private static final long DEADLINE_MILLIS = 10_000;

    /**
     * The backend for tests that need real concurrency among nested tasks.
     *
     * <p>Not {@code FORK_JOIN}: its pool is sized from {@code availableProcessors()}, and the
     * outer task parks its worker in {@code awaitStateChange} — a plain {@code Condition.await},
     * not a {@code ManagedBlocker}, so no compensation thread is spawned. On a two-vCPU CI
     * container that leaves one worker for the whole nested batch, and on one vCPU it leaves
     * none, so a concurrency assertion either fails or hangs on the machine rather than on the
     * code. Virtual threads have no such ceiling.</p>
     */
    private static ExecutorBackend concurrentBackend() {
        org.junit.jupiter.api.Assumptions.assumeTrue(
                ExecutorBackend.VIRTUAL_THREAD_PER_TASK.isAvailable(),
                "needs a backend whose concurrency does not depend on the core count");
        return ExecutorBackend.VIRTUAL_THREAD_PER_TASK;
    }

    @Nested
    @DisplayName("on the caller-runs backend")
    class CallerRuns {

        @Test
        @DisplayName("a task that submits a nested batch completes — the default policy caps concurrency at 1 here")
        void nestedSubmitUnderDefaultPolicy() throws Exception {

            // defaultMaxConcurrency() is 1 for CALLER_RUNS, so defaultPolicy() refuses the
            // nested task while the outer one is still claimed — and the outer task's own
            // thread is the coordinator waiting for it.
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-caller-runs", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedCR", ExecutorBackend.CALLER_RUNS);
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    final ThrottledExecutor child = root.newChild("Inner");
                    child.submitThrottled(List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")));
                }, 10L, "outer")));
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).as("both the outer and the nested task ran").isEqualTo(2);
        }

        @Test
        @DisplayName("nesting two deep still completes")
        void nestedTwoDeep() throws Exception {

            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-two-deep", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedDeep", ExecutorBackend.CALLER_RUNS);
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    final ThrottledExecutor mid = root.newChild("Mid");
                    mid.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                        ran.incrementAndGet();
                        final ThrottledExecutor leaf = mid.newChild("Leaf");
                        leaf.submitThrottled(List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "leaf")));
                    }, 10L, "mid")));
                }, 10L, "outer")));
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(3);
        }

        @Test
        @DisplayName("a policy that refuses everything cannot wedge a nested batch either")
        void nestedSubmitUnderARefusingPolicy() throws Exception {

            final AtomicInteger ran = new AtomicInteger();
            /*
             * Refuses every candidate. The outer task still starts, because nothing is running
             * and the ordinary liveness floor covers that. The nested one is the interesting
             * case: the outer task is in flight, so the floor does not apply, and only the
             * re-entrancy guarantee can get it in.
             *
             * Note the policy must NOT also override inFlightTaskCount() to a constant: that
             * disables the floor for the outer task as well, and then nothing starts at all —
             * a broken policy rather than the scenario under test.
             */
            final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;

            assertCompletes("nested-refusing", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedRefuse", ExecutorBackend.CALLER_RUNS);
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    root.newChild("Inner").submitThrottled(
                            List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")), refuseAll);
                }, 10L, "outer")), refuseAll);
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("the exemption is bounded")
    class Bounded {

        @Test
        @DisplayName("a nested batch still obeys the concurrency cap — the exemption is one task, not a free pass")
        void nestedBatchStillObeysTheCap() throws Exception {

            /*
             * The regression this exists for. Every test above nests a SINGLE task, which a
             * blanket exemption satisfies just as well as a bounded one — so a flag that
             * short-circuited every scan of the batch went unnoticed. Measured against the
             * unbounded version, this reached a peak of 12 concurrent nested tasks under a cap
             * of 1: the memory budget and the concurrency cap both bypassed, which is the OOM
             * the library exists to prevent.
             */
            final int nested = 20;
            final AtomicInteger live = new AtomicInteger();
            final AtomicInteger peak = new AtomicInteger();
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-bounded", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedBounded", concurrentBackend());

                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    final List<ThrottledTask<Runnable>> batch = new java.util.ArrayList<>();
                    for (int i = 0; i < nested; i++) {
                        batch.add(ThrottledTask.of(() -> {
                            peak.accumulateAndGet(live.incrementAndGet(), Math::max);
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            live.decrementAndGet();
                            ran.incrementAndGet();
                        }, 10L, "inner"));
                    }
                    root.newChild("Inner").submitThrottled(batch, new MaxConcurrencyPolicy(1));
                }, 10L, "outer")), new MaxConcurrencyPolicy(1));

                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).as("the whole nested batch ran").isEqualTo(nested);
            assertThat(peak.get())
                    .as("the exemption lets a blocked coordinator place ONE task, not its whole batch")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("the streaming overload is bounded the same way")
        void streamedNestedBatchStillObeysTheCap() throws Exception {

            final int nested = 20;
            final AtomicInteger live = new AtomicInteger();
            final AtomicInteger peak = new AtomicInteger();
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-bounded-stream", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedBoundedStream", concurrentBackend());

                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    final List<ThrottledTask<Runnable>> batch = new java.util.ArrayList<>();
                    for (int i = 0; i < nested; i++) {
                        batch.add(ThrottledTask.of(() -> {
                            peak.accumulateAndGet(live.incrementAndGet(), Math::max);
                            try {
                                Thread.sleep(20);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            live.decrementAndGet();
                            ran.incrementAndGet();
                        }, 10L, "inner"));
                    }
                    root.newChild("Inner").submitThrottled(batch.iterator(), new MaxConcurrencyPolicy(1), 8);
                }, 10L, "outer")), new MaxConcurrencyPolicy(1));

                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(nested);
            assertThat(peak.get()).isEqualTo(1);
        }

        @Test
        @DisplayName("a permissive policy is not held back by the bound")
        void aPermissivePolicyStillRunsInParallel() throws Exception {

            // The exemption only engages where the policy refuses. With room to spare the
            // nested batch must still use it.
            final AtomicInteger live = new AtomicInteger();
            final AtomicInteger peak = new AtomicInteger();

            assertCompletes("nested-permissive", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedPermissive", concurrentBackend());

                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    final List<ThrottledTask<Runnable>> batch = new java.util.ArrayList<>();
                    for (int i = 0; i < 8; i++) {
                        batch.add(ThrottledTask.of(() -> {
                            peak.accumulateAndGet(live.incrementAndGet(), Math::max);
                            try {
                                Thread.sleep(60);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            live.decrementAndGet();
                        }, 10L, "inner"));
                    }
                    root.newChild("Inner").submitThrottled(batch, new MaxConcurrencyPolicy(16));
                }, 10L, "outer")), new MaxConcurrencyPolicy(16));

                root.waitForCompletion();
                root.close();
            });

            assertThat(peak.get())
                    .as("the policy allowed 16; the bound must not have serialised this")
                    .isGreaterThan(1);
        }
    }

    @Nested
    @DisplayName("across separate roots")
    class CrossRoot {

        @Test
        @DisplayName("nesting into a DIFFERENT root still completes when the policy's ledger spans both")
        void nestedSubmitIntoAnotherRoot() throws Exception {

            /*
             * The exemption used to require the nested submission to target the SAME harness, on
             * the reasoning that an unrelated tree holds nothing this one waits for. That is
             * false for the shipped default: the liveness floor defers to
             * inFlightTaskCount(), and defaultPolicy() composes the process-wide
             * GlobalMemoryBudgetPolicy, whose ledger spans every tree. Measured before the fix,
             * this shape did not finish in 8 seconds with the outer task's bytes still claimed.
             *
             * A private budget instance of the same shape, rather than the process-wide
             * singleton, so the test owns its own accounting.
             */
            final GlobalMemoryBudgetPolicy shared = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
            final AdmissionPolicy policy = CompositePolicy.allOf(shared, new MaxConcurrencyPolicy(8));
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-cross-root", () -> {
                final ThrottledExecutor a = new ThrottledExecutor("RootA");
                final ThrottledExecutor b = new ThrottledExecutor("RootB");

                a.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    // 700 of the 1000 is already claimed by this very task; another 700 cannot
                    // fit, and only this thread can release the first.
                    b.submitThrottled(List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 700L, "inner")), policy);
                    b.waitForCompletion();
                }, 700L, "outer")), policy);

                a.waitForCompletion();
                a.close();
                b.close();
            });

            assertThat(ran.get()).isEqualTo(2);

            /*
             * Awaited, not asserted outright. The claim is released from a whenComplete
             * dependent of the task's own future, and CompletableFuture unparks a waiter while
             * it is still running dependents — so waitForCompletion can return a hair before the
             * ledger is given back. The library never promised otherwise (the contract is
             * "recording precedes settling", which is about failures, not about policy state),
             * and asserting the stricter ordering made this test fail roughly one run in twenty
             * under the jar classpath. What matters here is that the bytes come back at all: a
             * leak leaves them charged forever, which no amount of waiting fixes.
             */
            Awaitility.await("the shared ledger drains")
                    .atMost(java.time.Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(shared.inFlightBytes()).isZero());
        }
    }

    @Nested
    @DisplayName("from a task that holds no throttle claim")
    class NoClaim {

        @Test
        @DisplayName("plain submit() bodies do NOT bypass the budget when they submit throttled work")
        void plainSubmitBodiesStillObeyThePolicy() throws Exception {

            /*
             * The exemption exists for one shape: a thread that is waiting for admission while
             * holding a claim that only it can release. It had been widened to "any thread inside
             * any library task" — which includes plain submit() bodies holding no claim at all.
             * Every such body then bypassed the policy outright: eight of them each submitting a
             * 700-byte task against a 1000-byte budget ran all eight at once. That is the OOM
             * this library exists to prevent, and there was no deadlock to justify it — nothing
             * these bodies hold is in the budget's ledger.
             */
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
            final AdmissionPolicy policy = CompositePolicy.allOf(budget, new MaxConcurrencyPolicy(8));
            final AtomicInteger inFlight = new AtomicInteger();
            final AtomicInteger peak = new AtomicInteger();
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-no-claim", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NoClaim", concurrentBackend());

                for (int i = 0; i < 8; i++) {
                    root.submit((Runnable) () -> root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                        peak.accumulateAndGet(inFlight.incrementAndGet(), Math::max);
                        try {
                            Thread.sleep(120);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        } finally {
                            inFlight.decrementAndGet();
                            ran.incrementAndGet();
                        }
                    }, 700L, "inner")), policy));
                }

                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(8);
            assertThat(peak.get())
                    .as("700 of 1000 fits exactly one at a time; nobody here holds a claim to exempt")
                    .isEqualTo(1);
            Awaitility.await().atMost(java.time.Duration.ofSeconds(5))
                    .untilAsserted(() -> assertThat(budget.inFlightBytes()).isZero());
        }

        @Test
        @DisplayName("a claim holder is exempt only while nothing else in the ledger could release it")
        void claimHolderWaitsForUnrelatedWorkFirst() throws Exception {

            /*
             * The refinement that makes the exemption safe: "I hold a claim" is necessary, but
             * the deadlock only exists when everything counted in flight is itself waiting to
             * nest. While unrelated work is running it will finish on its own, and the nested
             * coordinator should wait for it like anyone else rather than be admitted past the
             * budget on top of it.
             */
            final GlobalMemoryBudgetPolicy budget = new GlobalMemoryBudgetPolicy(1_000L, 1.0);
            final AdmissionPolicy policy = CompositePolicy.allOf(budget, new MaxConcurrencyPolicy(8));
            final AtomicInteger bytesInFlightPeak = new AtomicInteger();
            final java.util.concurrent.CountDownLatch unrelatedRunning = new java.util.concurrent.CountDownLatch(1);
            final java.util.concurrent.CountDownLatch releaseUnrelated = new java.util.concurrent.CountDownLatch(1);
            final AtomicBoolean innerRanWhileUnrelatedHeld = new AtomicBoolean();

            assertCompletes("nested-waits-for-unrelated", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("ClaimWaits", concurrentBackend());

                // 600 bytes of unrelated work, parked.
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    unrelatedRunning.countDown();
                    try {
                        releaseUnrelated.await(10, java.util.concurrent.TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }, 600L, "unrelated")), policy);
                Awaitility.await().atMost(java.time.Duration.ofSeconds(5)).until(() -> unrelatedRunning.getCount() == 0);

                // A 300-byte claim holder that nests a 300-byte task: 600 + 300 + 300 > 1000.
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                        innerRanWhileUnrelatedHeld.set(releaseUnrelated.getCount() > 0);
                    }, 300L, "inner")), policy);
                }, 300L, "outer")), policy);

                // Give the nested coordinator every chance to be wrongly exempted...
                try {
                    Thread.sleep(400);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                releaseUnrelated.countDown();

                root.waitForCompletion();
                root.close();
            });

            assertThat(innerRanWhileUnrelatedHeld.get())
                    .as("the inner task had to wait for the unrelated 600 bytes to leave the ledger")
                    .isFalse();
        }
    }

    @Nested
    @DisplayName("inside an exclusive task")
    class Exclusive {

        @Test
        @DisplayName("a task that must run alone can still submit nested work")
        void nestedSubmitInsideAnExclusiveTask() throws Exception {

            // An exclusive task owns the harness while it runs. If that ownership also shut out
            // the task's OWN nested submissions, a quarantined task that delegates any work
            // would wedge the harness permanently — nothing else can release the claim.
            final AtomicInteger ran = new AtomicInteger();
            final AdmissionPolicy exclusiveForOuter = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "outer".equals(task.name);
                }
            };

            assertCompletes("nested-exclusive", () -> {
                final ThrottledExecutor root = new ThrottledExecutor("NestedExclusive");
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    root.newChild("Inner").submitThrottled(
                            List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")),
                            exclusiveForOuter);
                }, 10L, "outer")), exclusiveForOuter);
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(2);
        }

        @Test
        @DisplayName("a nested task that is itself exclusive still runs — isolation yields to progress")
        void nestedExclusiveInsideExclusive() throws Exception {

            final AtomicInteger ran = new AtomicInteger();
            // Everything is exclusive: the nested task can never see an empty pool, because its
            // own parent is holding the only claim.
            final AdmissionPolicy everythingExclusive = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return true;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return true;
                }
            };

            assertCompletes("nested-exclusive-deep", () -> {
                final ThrottledExecutor root = new ThrottledExecutor("NestedExclusiveDeep");
                root.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                    ran.incrementAndGet();
                    root.newChild("Inner").submitThrottled(
                            List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")),
                            everythingExclusive);
                }, 10L, "outer")), everythingExclusive);
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(2);
        }
    }

    @Nested
    @DisplayName("against a privileged waiter")
    class VersusPriority {

        @Test
        @DisplayName("a coordinator that has earned priority must not block the nested work it is waiting on")
        void privilegedTicketDoesNotBlockNestedWork() throws Exception {

            /*
             * The second door into the same deadlock. The re-entrancy exemption was added to the
             * exclusive-holder check but not to the privileged-yield check that sits above it,
             * so a coordinator that became privileged after three refusals then blocked the
             * nested submission of the very task it was waiting on — with no exclusivity
             * involved at all. Measured before the fix: never finished.
             *
             * Shape: a cap of 1, two outer tasks. The first blocks long enough for the
             * coordinator to rack up refusals on the second and take priority, and only then
             * submits nested work.
             */
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-versus-priority", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedPriority", ExecutorBackend.FORK_JOIN);

                root.submitThrottled(List.of(
                        ThrottledTask.<Runnable>of(() -> {
                            try {
                                // Longer than PRIORITY_AFTER_REFUSALS × the 100ms poll.
                                Thread.sleep(600);
                            } catch (InterruptedException e) {
                                Thread.currentThread().interrupt();
                            }
                            root.newChild("Inner").submitThrottled(
                                    List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")),
                                    new MaxConcurrencyPolicy(1));
                            ran.incrementAndGet();
                        }, 10L, "outer-1"),
                        ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "outer-2")),
                        new MaxConcurrencyPolicy(1));

                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).as("all three ran").isEqualTo(3);
        }
    }

    @Nested
    @DisplayName("on a pooled backend")
    class Pooled {

        @Test
        @DisplayName("every worker submitting nested work at a concurrency cap still drains")
        void allWorkersNestUnderACap() throws Exception {

            // Without the re-entrancy guarantee this is the general form of the same deadlock:
            // all N slots are held by tasks that are each waiting for a nested admission.
            final int outer = 4;
            final AtomicInteger ran = new AtomicInteger();

            assertCompletes("nested-pooled", () -> {
                final ThrottledExecutor root =
                        ThrottledExecutor.withBackend("NestedPooled", concurrentBackend());
                final List<ThrottledTask<Runnable>> batch = new java.util.ArrayList<>();
                for (int i = 0; i < outer; i++) {
                    batch.add(ThrottledTask.of(() -> {
                        ran.incrementAndGet();
                        root.newChild("Inner").submitThrottled(
                                List.of(ThrottledTask.<Runnable>of(ran::incrementAndGet, 10L, "inner")),
                                new MaxConcurrencyPolicy(outer));
                    }, 10L, "outer"));
                }
                root.submitThrottled(batch, new MaxConcurrencyPolicy(outer));
                root.waitForCompletion();
                root.close();
            });

            assertThat(ran.get()).isEqualTo(outer * 2);
        }
    }

    /** Runs {@code body} on its own thread and fails — rather than hangs — if it does not finish. */
    private static void assertCompletes(String threadName, Runnable body) throws InterruptedException {

        final AtomicBoolean finished = new AtomicBoolean();
        final AtomicReference<Throwable> failure = new AtomicReference<>();

        final Thread thread = new Thread(() -> {
            try {
                body.run();
                finished.set(true);
            } catch (Throwable t) {
                failure.set(t);
            }
        }, threadName);
        thread.setDaemon(true);
        thread.start();
        thread.join(DEADLINE_MILLIS);

        if (failure.get() != null) {
            throw new AssertionError("the nested batch threw", failure.get());
        }
        assertThat(finished.get())
                .as("the nested batch deadlocked: it did not finish within %d ms", DEADLINE_MILLIS)
                .isTrue();
    }
}
