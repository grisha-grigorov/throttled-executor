package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.lang.ref.WeakReference;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * Tests for the history tree: nodes mirror the executor tree, outlive their executors, and
 * are eventually folded into their parents once the executors are collected — history
 * compressed, never lost, and never pinning an executor.
 *
 * <p>Fold eligibility is <b>weak-ref-cleared only</b>. A closed executor still delivers
 * events (late bodies, REJECTED submissions), so {@code isClosed()} must not fold; a
 * collected one cannot deliver anything, because an in-flight event holds the descriptor,
 * which strongly holds the executor.</p>
 */
class RecorderTreeTests {

    private static void awaitCollected(WeakReference<?> sentinel) {
        for (int attempt = 0; attempt < 20 && sentinel.get() != null; attempt++) {
            System.gc();
            sleep(50);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @DisplayName("records land on the submitting executor's node, not the root the listener is attached to")
    void recordsLandOnTheSubmittingExecutorsNode() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("TreeRoot");
        root.addListener(recorder);
        final ThrottledExecutor grandchild = root.newChild("TreeChild").newChild("TreeGrandchild");

        grandchild.submit((Runnable) () -> { }).join();
        grandchild.waitForCompletion();

        final ExecutorNodeSnapshot grandchildNode =
                ExecutionRecorderTests.find(recorder.snapshot().roots(), grandchild.executorId);
        assertThat(grandchildNode).isNotNull();
        assertThat(grandchildNode.records()).hasSize(1);

        final ExecutorNodeSnapshot rootNode =
                ExecutionRecorderTests.find(recorder.snapshot().roots(), root.executorId);
        assertThat(rootNode.records()).as("the root itself ran nothing").isEmpty();
    }

    @Test
    @DisplayName("ancestor nodes are created by the parent walk and carry weak references")
    void ancestorNodesAreCreatedByTheParentWalkAndCarryWeakRefs() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("WalkRoot");
        root.addListener(recorder);
        final ThrottledExecutor child = root.newChild("WalkChild");
        final ThrottledExecutor grandchild = child.newChild("WalkGrandchild");

        // Only the grandchild ever sees a task; the walk must still have created the chain.
        grandchild.submit((Runnable) () -> { }).join();
        grandchild.waitForCompletion();

        for (ThrottledExecutor executor : List.of(root, child, grandchild)) {
            final ExecutorNode node = recorder.node(executor.executorId);
            assertThat(node).as("a node for %s", executor.executorId).isNotNull();
            assertThat(node.executorRef.get())
                    .as("the node references its executor weakly, and it is still alive here")
                    .isSameAs(executor);
        }
        assertThat(recorder.node(grandchild.executorId).parent.executorId).isEqualTo(child.executorId);
        assertThat(recorder.node(child.executorId).parent.executorId).isEqualTo(root.executorId);
        assertThat(recorder.node(root.executorId).parent).isNull();
    }

    @Test
    @DisplayName("multiple roots produce a forest")
    void multipleRootsProduceAForest() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor first = new ThrottledExecutor("ForestA");
        final ThrottledExecutor second = new ThrottledExecutor("ForestB");
        first.addListener(recorder);
        second.addListener(recorder);

        first.submit((Runnable) () -> { }).join();
        second.submit((Runnable) () -> { }).join();
        first.waitForCompletion();
        second.waitForCompletion();

        assertThat(recorder.snapshot().roots())
                .extracting(ExecutorNodeSnapshot::executorId)
                .containsExactly(first.executorId, second.executorId);
    }

    @Test
    @DisplayName("a closed executor still receives late and rejected events — CLOSED, never folded")
    void closedExecutorStillReceivesLateAndRejectedEvents() throws InterruptedException {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("CloseRoot");
        root.addListener(recorder);
        final ThrottledExecutor child = root.newChild("CloseChild");

        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);
        final AtomicBoolean lateBodyFinished = new AtomicBoolean();
        child.submit((Runnable) () -> {
            started.countDown();
            try {
                release.await(30, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
            lateBodyFinished.set(true);
        });
        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();

        child.close();   // closed while a body is still running

        final CompletableFuture<Void> rejected = child.submit((Runnable) () -> { });
        assertThat(rejected.isCompletedExceptionally()).isTrue();

        release.countDown();
        child.waitForCompletion();
        assertThat(lateBodyFinished).isTrue();

        final ExecutorNodeSnapshot node =
                ExecutionRecorderTests.find(recorder.snapshot().roots(), child.executorId);
        assertThat(node).as("a CLOSED node is never folded — its executor can still deliver events").isNotNull();
        assertThat(node.liveness()).isEqualTo("CLOSED");
        assertThat(node.statusCounts().get(TaskStatus.REJECTED)).isEqualTo(1L);
        assertThat(node.statusCounts().get(TaskStatus.COMPLETED)).isEqualTo(1L);
    }

    @Test
    @Tag("gc")
    @DisplayName("a collected child is marked COLLECTED and kept, up to the retention limit")
    void collectedChildIsMarkedTerminalAndKeptUpToTheLimit() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("GcRoot");
        root.addListener(recorder);

        ThrottledExecutor child = root.newChild("GcChild");
        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();
        final String childId = child.executorId;
        final WeakReference<ThrottledExecutor> sentinel = new WeakReference<>(child);
        child = null;

        awaitCollected(sentinel);
        Assumptions.assumeTrue(sentinel.get() == null, "the JVM declined to collect the dropped child");

        final ExecutorNodeSnapshot node = ExecutionRecorderTests.find(recorder.snapshot().roots(), childId);
        assertThat(node).as("within the retention limit the full node survives collection").isNotNull();
        assertThat(node.liveness()).isEqualTo("COLLECTED");
        assertThat(node.records()).hasSize(1);
    }

    @Test
    @Tag("gc")
    @DisplayName("terminal children beyond the limit fold into the parent, with an eviction count")
    void terminalChildrenBeyondTheLimitFoldIntoTheParentWithEvictedCount() {
        final ExecutionRecorder recorder = new ExecutionRecorder(new RecorderConfig(256, 2, 64));
        final ThrottledExecutor root = new ThrottledExecutor("FoldRoot");
        root.addListener(recorder);

        final List<WeakReference<ThrottledExecutor>> sentinels = new ArrayList<>();
        final List<String> childIds = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            ThrottledExecutor child = root.newChild("FoldChild-" + i);
            for (int task = 0; task < 3; task++) {
                child.submit((Runnable) () -> { });
            }
            child.waitForCompletion();
            childIds.add(child.executorId);
            sentinels.add(new WeakReference<>(child));
            child = null;
        }

        for (WeakReference<ThrottledExecutor> sentinel : sentinels) {
            awaitCollected(sentinel);
            Assumptions.assumeTrue(sentinel.get() == null, "the JVM declined to collect a dropped child");
        }

        final ExecutorNodeSnapshot rootNode = recorder.snapshot().roots().get(0);

        assertThat(rootNode.children())
                .as("the last M=2 terminal children are kept in full; the oldest 3 are folded")
                .extracting(ExecutorNodeSnapshot::executorId)
                .containsExactly(childIds.get(3), childIds.get(4));
        assertThat(rootNode.evictedChildren()).isEqualTo(3);
        assertThat(rootNode.statusCounts().get(TaskStatus.COMPLETED))
                .as("the folded children's 3 x 3 completions merged into the parent — compressed, not lost")
                .isEqualTo(9L);
        assertThat(rootNode.submitted()).isEqualTo(9L);
        assertThat(rootNode.records()).as("folded children's RECORDS are dropped; the parent ran nothing itself").isEmpty();
    }

    /**
     * The no-pinning proof, asserted rather than assumed: the recorder holds history for
     * the child, and the child must still be collectable. If the recorder pinned it, the
     * sentinel would never clear.
     */
    @Test
    @Tag("gc")
    @DisplayName("the recorder does not pin executors")
    void theRecorderDoesNotPinExecutors() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("PinRoot");
        root.addListener(recorder);

        ThrottledExecutor child = root.newChild("PinChild");
        for (int i = 0; i < 20; i++) {
            child.submit((Runnable) () -> { });
        }
        child.waitForCompletion();
        final WeakReference<ThrottledExecutor> sentinel = new WeakReference<>(child);
        child = null;

        awaitCollected(sentinel);

        assertThat(sentinel.get())
                .as("history for an executor must never keep the executor alive")
                .isNull();
    }

    @Test
    @DisplayName("concurrent recording and snapshotting: no exceptions, exact totals at rest")
    void concurrentRecordAndSnapshotSmokeTest() throws InterruptedException {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("SmokeRoot");
        root.addListener(recorder);

        final int submitters = 4;
        final int perSubmitter = 250;
        final CountDownLatch done = new CountDownLatch(submitters);
        final AtomicBoolean snapshotFailure = new AtomicBoolean();

        final Thread snapshotter = new Thread(() -> {
            try {
                while (done.getCount() > 0) {
                    recorder.snapshot().renderJson();
                }
            } catch (RuntimeException e) {
                snapshotFailure.set(true);
            }
        }, "snapshotter");
        snapshotter.start();

        for (int t = 0; t < submitters; t++) {
            new Thread(() -> {
                for (int i = 0; i < perSubmitter; i++) {
                    root.submit((Runnable) () -> { });
                }
                done.countDown();
            }, "submitter-" + t).start();
        }

        assertThat(done.await(30, TimeUnit.SECONDS)).isTrue();
        snapshotter.join(TimeUnit.SECONDS.toMillis(10));
        root.waitForCompletion();

        assertThat(snapshotFailure).as("snapshotting while recording must never throw").isFalse();
        final ExecutorNodeSnapshot node = recorder.snapshot().roots().get(0);
        assertThat(node.statusCounts().get(TaskStatus.COMPLETED)).isEqualTo((long) submitters * perSubmitter);
        assertThat(node.submitted()).isEqualTo((long) submitters * perSubmitter);
    }
}
