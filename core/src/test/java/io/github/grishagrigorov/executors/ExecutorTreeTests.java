package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.MemoryBudgetPolicy;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for the explicit parent/child executor tree.
 *
 * <p>Nesting already worked by accident: a child's {@code waitForCompletion()} throws on
 * the parent's task thread, which fails the parent's future, which the parent aggregates.
 * What was missing was any representation of the relationship, so nothing could share a
 * budget across a subtree, cancel a branch, report where in the tree a failure happened,
 * or wait for work a child spawned.</p>
 *
 * <p>Ownership is deliberately explicit. Construction site is not ownership — a
 * field-initialised or dependency-injected executor happens to be constructed while some
 * unrelated task is on the stack, and must not be adopted by it. So the plain constructor
 * always produces a root, and ambient discovery is opted into per instance.</p>
 */
class ExecutorTreeTests {

    // ─── Ownership ───────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a root has no parent")
    void rootShouldHaveNoParent() {

        final ThrottledExecutor root = new ThrottledExecutor("Root");

        assertThat(root.parent()).isNull();
        assertThat(root.isRoot()).isTrue();
    }

    @Test
    @DisplayName("newChild reports its parent")
    void newChildShouldReportItsParent() {

        final ThrottledExecutor root = new ThrottledExecutor("Root");
        final ThrottledExecutor child = root.newChild("Leg");

        assertThat(child.parent()).isSameAs(root);
        assertThat(child.isRoot()).isFalse();
    }

    /**
     * The ownership test. A plain constructor call must produce a root even when it
     * happens to run inside another executor's task — otherwise any field initialiser or
     * injected executor silently gets adopted by whatever is on the stack at the time.
     */
    @Test
    @DisplayName("the plain constructor produces a root even inside a running task")
    void plainConstructorShouldProduceARootEvenWhenCalledInsideARunningTask() {

        final ThrottledExecutor outer = new ThrottledExecutor("Outer");

        final CompletableFuture<Boolean> constructedInside =
                outer.submit((Supplier<Boolean>) () -> new ThrottledExecutor("Inner").isRoot());

        assertThat(constructedInside.join())
                .as("construction site is not ownership — the plain constructor never adopts")
                .isTrue();
    }

    @Test
    @DisplayName("withAmbientDiscovery adopts the executor whose task is running")
    void withAmbientDiscoveryInsideATaskShouldAdoptTheRunningExecutor() {

        final ThrottledExecutor outer = new ThrottledExecutor("AmbientOuter");

        final CompletableFuture<ThrottledExecutor> adopted = outer.submit(
                (Supplier<ThrottledExecutor>) () ->
                        ThrottledExecutor.withAmbientDiscovery("AmbientInner").parent());

        assertThat(adopted.join())
                .as("opting in must find the executor running the current task")
                .isSameAs(outer);
    }

    @Test
    @DisplayName("withAmbientDiscovery outside any task produces a root")
    void withAmbientDiscoveryOutsideAnyTaskShouldBeARoot() {

        assertThat(ThrottledExecutor.withAmbientDiscovery("NoContext").isRoot())
                .as("nothing to adopt means root, not an error")
                .isTrue();
    }

    /**
     * Binding and reading are separate concerns: every executor publishes itself as the
     * ambient parent while its tasks run, but only an opted-in construction reads that.
     */
    @Test
    @DisplayName("an executor that never opted in still serves as an ambient parent")
    void anExecutorThatDidNotOptInShouldStillServeAsAnAmbientParent() {

        final ThrottledExecutor plain = new ThrottledExecutor("PlainParent");

        final CompletableFuture<ThrottledExecutor> adopted = plain.submit(
                (Supplier<ThrottledExecutor>) () ->
                        ThrottledExecutor.withAmbientDiscovery("Child").parent());

        assertThat(adopted.join()).isSameAs(plain);
    }

    @Test
    @DisplayName("ambient context is restored, not cleared, after a nested task returns")
    void ambientShouldBeRestoredNotClearedAfterANestedTaskReturns() {

        final ThrottledExecutor outer = new ThrottledExecutor("RestoreOuter");
        final ThrottledExecutor inner = new ThrottledExecutor("RestoreInner");

        final CompletableFuture<ThrottledExecutor> observedAfterNesting =
                outer.submit((Supplier<ThrottledExecutor>) () -> {

                    // Run an inner task to completion ON THIS THREAD's ambient stack.
                    inner.submit((Runnable) () -> { }).join();

                    // Code still inside outer's task must see outer again, not nothing.
                    return ThrottledExecutor.currentAmbient();
                });

        assertThat(observedAfterNesting.join())
                .as("a finally that only removes would lose the enclosing binding")
                .isSameAs(outer);
    }

    @Test
    @DisplayName("ambient context does not leak onto a pooled worker after a task completes")
    void ambientShouldNotLeakOntoAPooledWorkerAfterTheTaskCompletes() {

        final ThrottledExecutor polluter = new ThrottledExecutor("AmbientPolluter");
        for (int i = 0; i < 32; i++) {
            polluter.submit((Runnable) () -> { });
        }
        polluter.waitForCompletion();

        final ThrottledExecutor probe = new ThrottledExecutor("AmbientProbe");
        final List<CompletableFuture<ThrottledExecutor>> observed = new ArrayList<>();

        for (int i = 0; i < 32; i++) {
            observed.add(probe.submit((Supplier<ThrottledExecutor>) () -> {
                // Ambient inside probe's own task is probe; what matters is that the
                // PREVIOUS executor is gone, i.e. nothing observes the polluter.
                return ThrottledExecutor.currentAmbient();
            }));
        }
        probe.waitForCompletion();

        for (CompletableFuture<ThrottledExecutor> future : observed) {
            assertThat(future.join()).isNotSameAs(polluter);
        }
    }

    @Test
    @DisplayName("a child inherits its parent's backend")
    void childShouldInheritTheParentsBackend() {

        final ThrottledExecutor root =
                ThrottledExecutor.withBackend("BackendRoot", ExecutorBackend.FORK_JOIN);

        assertThat(root.newChild("Inheritor").backend()).isEqualTo(ExecutorBackend.FORK_JOIN);
    }

    // ─── Shared budget across a subtree ──────────────────────────────────────────

    /**
     * The point of the tree. Each executor used to own its own {@code ThrottleHarness}, so
     * every executor in a nested tree independently believed it had the whole budget
     * available. That was masked only by the fixed pool capping total concurrency; remove
     * that cap and a deep tree overcommits by a factor of its own size.
     */
    @Test
    @DisplayName("a subtree shares one memory budget")
    void nestedSubtreeShouldShareOneMemoryBudget() throws InterruptedException {

        final ThrottledExecutor root = new ThrottledExecutor("BudgetRoot");

        final AtomicInteger inFlight = new AtomicInteger();
        final AtomicInteger peakInFlight = new AtomicInteger();
        final CountDownLatch done = new CountDownLatch(3);

        // Budget fits exactly one 600-byte task at a time.
        final MemoryBudgetPolicy policy = new MemoryBudgetPolicy(1_000L, 1.0);

        final List<ThrottledExecutor> children = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            children.add(root.newChild("BudgetChild-" + i));
        }

        final List<Thread> submitters = new ArrayList<>();
        for (ThrottledExecutor child : children) {
            final Thread submitter = new Thread(() -> {
                child.submitThrottled(List.of(new ThrottledTask<>((Runnable) () -> {
                    final int current = inFlight.incrementAndGet();
                    peakInFlight.accumulateAndGet(current, Math::max);
                    try {
                        Thread.sleep(250);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        inFlight.decrementAndGet();
                    }
                }, 600L, "budgeted")), policy);
                child.waitForCompletion();
                done.countDown();
            });
            submitters.add(submitter);
            submitter.start();
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).as("all children finished").isTrue();
        for (Thread submitter : submitters) {
            submitter.join(TimeUnit.SECONDS.toMillis(5));
        }

        assertThat(peakInFlight.get())
                .as("three independently-submitting children of one root share ONE budget, "
                        + "so only one 600-byte task fits at a time")
                .isEqualTo(1);
    }

    // ─── Transitive completion ───────────────────────────────────────────────────

    /**
     * A parent could return from {@code waitForCompletion()} while work spawned by its own
     * tasks was still running, because a fire-and-forget child was invisible to it.
     */
    @Test
    @DisplayName("waitForCompletion does not return while grandchildren are still running")
    void waitForCompletionShouldNotReturnWhileGrandchildrenAreStillRunning() {

        final ThrottledExecutor root = new ThrottledExecutor("TransitiveRoot");
        final AtomicBoolean grandchildFinished = new AtomicBoolean(false);

        root.submit((Runnable) () -> {
            final ThrottledExecutor child = ThrottledExecutor.withAmbientDiscovery("TransitiveChild");
            // Fire and forget: the child is never waited on here.
            child.submit((Runnable) () -> {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                grandchildFinished.set(true);
            });
        });

        root.waitForCompletion();

        assertThat(grandchildFinished)
                .as("the root's batch is not finished while work its tasks started is still running")
                .isTrue();
    }

    @Test
    @DisplayName("waitForCompletion aggregates failures from the whole subtree")
    void waitForCompletionShouldAggregateThrowablesFromTheWholeSubtree() {

        final ThrottledExecutor root = new ThrottledExecutor("SubtreeFailures");

        root.submit((Runnable) () -> {
            final ThrottledExecutor child = ThrottledExecutor.withAmbientDiscovery("FailingChild");
            child.submit((Runnable) () -> {
                throw new IllegalStateException("grandchild failure");
            });
        });

        final Throwable thrown = catchThrowable(root::waitForCompletion);

        assertThat(thrown)
                .as("a failure anywhere in the subtree must surface at the root")
                .isInstanceOf(ThreadContextAwareCompletionException.class);
    }

    @Test
    @DisplayName("the aggregate exception carries the full executor path")
    void exceptionShouldCarryTheFullExecutorPath() {

        final ThrottledExecutor root = new ThrottledExecutor("PathRoot");
        final ThrottledExecutor child = root.newChild("PathChild");
        final ThrottledExecutor grandchild = child.newChild("PathGrandchild");

        grandchild.submit((Runnable) () -> {
            throw new IllegalStateException("deep failure");
        });

        final Throwable thrown = catchThrowable(grandchild::waitForCompletion);

        assertThat(thrown).isInstanceOf(ThreadContextAwareCompletionException.class);

        final ThreadContextAwareCompletionException aggregate = (ThreadContextAwareCompletionException) thrown;

        assertThat(aggregate.executorPath())
                .as("one executorId does not locate a failure in a deep tree")
                .contains("PathRoot")
                .contains("PathChild")
                .contains("PathGrandchild");

        assertThat(aggregate.getMessage())
                .as("the path must be visible in ordinary logging, not only via a getter")
                .contains("PathGrandchild");
    }

    // ─── Cancellation ────────────────────────────────────────────────────────────

    @Test
    @DisplayName("cancelSubtree cancels grandchild tasks")
    void cancelSubtreeShouldCancelGrandchildTasks() {

        final ThrottledExecutor root = new ThrottledExecutor("CancelRoot");
        final ThrottledExecutor child = root.newChild("CancelChild");
        final ThrottledExecutor grandchild = child.newChild("CancelGrandchild");

        final CountDownLatch started = new CountDownLatch(1);

        // Occupy the grandchild so a second task stays queued and therefore cancellable.
        grandchild.submit((Runnable) () -> {
            started.countDown();
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        final CompletableFuture<Void> queued = grandchild.submit((Runnable) () -> { });

        root.cancelSubtree();

        assertThat(queued.isDone())
                .as("cancelling a subtree must reach a grandchild's pending work")
                .isTrue();
    }

    @Test
    @DisplayName("shutdownNow cancels the whole subtree")
    void shutdownNowShouldCancelTheWholeSubtree() {

        final ThrottledExecutor root = new ThrottledExecutor("ShutdownNowRoot");
        final ThrottledExecutor child = root.newChild("ShutdownNowChild");

        final CompletableFuture<Void> queued = child.submit((Runnable) () -> {
            try {
                Thread.sleep(2_000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        root.shutdownNow();

        assertThat(child.isClosed())
                .as("shutdownNow promises to stop executing tasks; leaving children running is a lie")
                .isTrue();
        assertThat(queued.isDone()).isTrue();
    }

    // ─── Registry hygiene ────────────────────────────────────────────────────────

    @Test
    @DisplayName("closing a child deregisters it from its parent")
    void closingAChildShouldDeregisterItFromItsParent() {

        final ThrottledExecutor root = new ThrottledExecutor("DeregisterRoot");
        final ThrottledExecutor child = root.newChild("DeregisterChild");

        assertThat(root.liveChildCount()).isEqualTo(1);

        child.close();

        assertThat(root.liveChildCount())
                .as("deterministic cleanup, no GC required")
                .isEqualTo(0);
    }

    /**
     * A long-lived root in a desktop application must not accumulate every child it has
     * ever created. Children are held weakly and the registry is compacted as it is used.
     */
    @Test
    @Tag("gc")
    @DisplayName("creating a child compacts unreachable siblings")
    void newChildShouldPruneUnreachableSiblings() {

        final ThrottledExecutor root = new ThrottledExecutor("PruneRoot");

        ThrottledExecutor doomed = root.newChild("Doomed");
        final WeakReference<ThrottledExecutor> sentinel = new WeakReference<>(doomed);

        assertThat(root.liveChildCount()).isEqualTo(1);

        doomed = null;

        // Give the collector a reason and a chance.
        for (int attempt = 0; attempt < 10 && sentinel.get() != null; attempt++) {
            System.gc();
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        org.junit.jupiter.api.Assumptions.assumeTrue(sentinel.get() == null,
                "the JVM declined to collect the dropped child");

        root.newChild("Trigger");

        assertThat(root.liveChildCount())
                .as("only the surviving child remains; the collected one was compacted away")
                .isEqualTo(1);
    }

    /**
     * The futures queue retains every completed future AND its job lambda. Repeated
     * batches on a long-lived executor must not grow it without bound.
     */
    @Test
    @DisplayName("repeated batches do not grow the futures queue without bound")
    void futuresQueueShouldNotGrowUnboundedAcrossRepeatedBatches() {

        final ThrottledExecutor executor = new ThrottledExecutor("LongLived");

        final int batches = 20;
        final int perBatch = 100;

        for (int batch = 0; batch < batches; batch++) {
            for (int task = 0; task < perBatch; task++) {
                executor.submit((Runnable) () -> { });
            }
            executor.waitForCompletion();
        }

        assertThat(executor.totalJobs())
                .as("%d submissions must not all be retained", batches * perBatch)
                .isLessThan(batches * perBatch);
    }

    @Test
    @DisplayName("inspecting futures immediately after a batch still works")
    void futuresShouldRemainInspectableAfterWaitingForABatch() {

        final ThrottledExecutor executor = new ThrottledExecutor("Inspectable");
        final AtomicReference<String> value = new AtomicReference<>();

        executor.submit((Supplier<String>) () -> "produced");
        executor.waitForCompletion();

        executor.getFutures().findFirst().ifPresent(pair ->
                value.set(String.valueOf(pair.getCompletableFuture().join())));

        assertThat(value.get())
                .as("pruning must not break inspecting a batch you have just waited on")
                .isEqualTo("produced");
    }
}
