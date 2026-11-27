package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.slf4j.MDC;

/**
 * Contract tests for the task-execution listener SPI.
 *
 * <p>The invariants pinned here are the ones downstream consumers (context propagation, the
 * metadata tree, JFR events) will lean on:</p>
 * <ol>
 *   <li>every {@code onSubmit} is followed by exactly one {@code onComplete};</li>
 *   <li>{@code onStart} fires only for tasks whose body actually ran;</li>
 *   <li>{@code onSubmit}/{@code onStart} run root-first, {@code onComplete} in reverse;</li>
 *   <li>a listener can never fail the task, and one bad listener cannot suppress the others;</li>
 *   <li>the listener set is snapshotted at submission;</li>
 *   <li>a task's scope is closed (all {@code onComplete}s run) before any waiter or dependent
 *       observes its future as complete.</li>
 * </ol>
 */
class TaskExecutionListenerTests {

    /** What a listener saw, in order, with the thread it saw it on. */
    record Event(String hook, Thread thread, TaskDescriptor task, Object state, TaskOutcome outcome) { }

    /** Records every hook call. Its per-task state is a fresh object per submission. */
    static final class RecordingListener implements TaskExecutionListener<Object> {

        final List<Event> events = new CopyOnWriteArrayList<>();
        final String label;

        RecordingListener(String label) {
            this.label = label;
        }

        RecordingListener() {
            this("listener");
        }

        @Override
        public Object onSubmit(TaskDescriptor task) {
            final Object state = new Object();
            events.add(new Event("SUBMIT", Thread.currentThread(), task, state, null));
            return state;
        }

        @Override
        public void onStart(TaskDescriptor task, Object state) {
            events.add(new Event("START", Thread.currentThread(), task, state, null));
        }

        @Override
        public void onComplete(TaskDescriptor task, Object state, TaskOutcome outcome) {
            events.add(new Event("COMPLETE", Thread.currentThread(), task, state, outcome));
        }

        List<String> hooks() {
            final List<String> hooks = new ArrayList<>();
            for (Event e : events) {
                hooks.add(e.hook());
            }
            return hooks;
        }

        Event last() {
            return events.get(events.size() - 1);
        }

        Event completion() {
            for (Event e : events) {
                if (e.hook().equals("COMPLETE")) {
                    return e;
                }
            }
            throw new AssertionError("no COMPLETE event recorded; saw " + hooks());
        }
    }

    @BeforeEach
    @AfterEach
    void clearMdc() {
        MDC.clear();
    }

    // ─── Basic lifecycle ────────────────────────────────────────────────────────

    @Test
    @DisplayName("a successful task produces SUBMIT, START, COMPLETE exactly once, with monotonic timestamps")
    void listenerShouldSeeSubmitStartCompleteExactlyOnceForASuccessfulTask() {
        final ThrottledExecutor executor = new ThrottledExecutor("Lifecycle");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);

        executor.submit((Supplier<String>) () -> "ok").join();
        executor.waitForCompletion();

        assertThat(listener.hooks()).containsExactly("SUBMIT", "START", "COMPLETE");

        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.COMPLETED);
        assertThat(outcome.failure()).isNull();

        final TaskDescriptor task = listener.completion().task();
        assertThat(outcome.startedAtNanos()).isGreaterThanOrEqualTo(task.submittedAtNanos());
        assertThat(outcome.completedAtNanos()).isGreaterThanOrEqualTo(outcome.startedAtNanos());
    }

    @Test
    @DisplayName("onSubmit runs on the submitting thread")
    void onSubmitShouldRunOnTheSubmittingThread() {
        final ThrottledExecutor executor = new ThrottledExecutor("SubmitThread");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);

        executor.submit((Runnable) () -> { }).join();

        assertThat(listener.events.get(0).hook()).isEqualTo("SUBMIT");
        assertThat(listener.events.get(0).thread()).isSameAs(Thread.currentThread());
    }

    @Test
    @DisplayName("onStart and onComplete run on the worker thread that ran the body")
    void onStartAndOnCompleteShouldRunOnTheWorkerThread() {
        final ThrottledExecutor executor = new ThrottledExecutor("WorkerThread");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        final AtomicReference<Thread> bodyThread = new AtomicReference<>();

        executor.submit((Runnable) () -> bodyThread.set(Thread.currentThread())).join();
        executor.waitForCompletion();

        assertThat(listener.events.get(1).hook()).isEqualTo("START");
        assertThat(listener.events.get(1).thread()).isSameAs(bodyThread.get());
        assertThat(listener.events.get(2).hook()).isEqualTo("COMPLETE");
        assertThat(listener.events.get(2).thread()).isSameAs(bodyThread.get());
    }

    @Test
    @DisplayName("the per-task state returned by onSubmit is handed to onStart and onComplete")
    void perTaskStateFromOnSubmitShouldBePassedToOnStartAndOnComplete() {
        final ThrottledExecutor executor = new ThrottledExecutor("State");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);

        executor.submit((Runnable) () -> { }).join();
        executor.waitForCompletion();

        final Object submitted = listener.events.get(0).state();
        assertThat(submitted).isNotNull();
        assertThat(listener.events.get(1).state()).isSameAs(submitted);
        assertThat(listener.events.get(2).state()).isSameAs(submitted);
    }

    @Test
    @DisplayName("a failed task reports FAILED with the raw throwable, not a CompletionException")
    void failedTaskShouldReportFailedWithTheRawThrowable() {
        final ThrottledExecutor executor = new ThrottledExecutor("Failed");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        final IllegalStateException boom = new IllegalStateException("boom");

        final CompletableFuture<Void> future = executor.submit((Runnable) () -> {
            throw boom;
        });
        assertThat(future).failsWithin(java.time.Duration.ofSeconds(10));

        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.FAILED);
        assertThat(outcome.failure())
                .as("the listener gets the exception the body threw, unwrapped")
                .isSameAs(boom);
    }

    @Test
    @DisplayName("the descriptor carries a strictly increasing sequence, the executor, and submission time")
    void descriptorShouldCarrySequenceExecutorAndSubmissionTime() {
        final ThrottledExecutor executor = new ThrottledExecutor("Descriptor");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);

        executor.submit((Runnable) () -> { }).join();
        executor.submit((Runnable) () -> { }).join();

        final TaskDescriptor first = listener.events.get(0).task();
        assertThat(first.executor()).isSameAs(executor);
        assertThat(first.executorId()).isEqualTo(executor.executorId);
        assertThat(first.executorPath()).isEqualTo(executor.executorPath());
        assertThat(first.backend()).isEqualTo(executor.backend());
        assertThat(first.name()).as("plain submissions carry no name").isNull();
        assertThat(first.sizeEstimate()).as("plain submissions carry no size estimate").isEqualTo(-1L);
        assertThat(first.submittedAtEpochMillis()).isPositive();

        final List<Long> sequences = new ArrayList<>();
        for (Event e : listener.events) {
            if (e.hook().equals("SUBMIT")) {
                sequences.add(e.task().sequence());
            }
        }
        assertThat(sequences).hasSize(2);
        assertThat(sequences.get(1)).isGreaterThan(sequences.get(0));
    }

    // ─── Ordering and inheritance ───────────────────────────────────────────────

    @Test
    @DisplayName("listeners run root-first on submit/start and in reverse on complete")
    void listenersShouldRunRootFirstOnStartAndReverseOnComplete() {
        final ThrottledExecutor root = new ThrottledExecutor("OrderRoot");
        final ThrottledExecutor child = root.newChild("OrderChild");
        final List<String> order = new CopyOnWriteArrayList<>();

        root.addListener(new TaskExecutionListener<Void>() {
            @Override
            public Void onSubmit(TaskDescriptor t) {
                order.add("submit:A");
                return null;
            }

            @Override
            public void onStart(TaskDescriptor t, Void s) {
                order.add("start:A");
            }

            @Override
            public void onComplete(TaskDescriptor t, Void s, TaskOutcome o) {
                order.add("complete:A");
            }
        });
        child.addListener(new TaskExecutionListener<Void>() {
            @Override
            public Void onSubmit(TaskDescriptor t) {
                order.add("submit:B");
                return null;
            }

            @Override
            public void onStart(TaskDescriptor t, Void s) {
                order.add("start:B");
            }

            @Override
            public void onComplete(TaskDescriptor t, Void s, TaskOutcome o) {
                order.add("complete:B");
            }
        });

        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();

        assertThat(order).containsExactly(
                "submit:A", "submit:B",
                "start:A", "start:B",
                "complete:B", "complete:A");
    }

    @Test
    @DisplayName("a child sees listeners added to the root after the child was created (live inheritance)")
    void childShouldSeeListenersAddedToTheRootAfterItsCreation() {
        final ThrottledExecutor root = new ThrottledExecutor("LiveRoot");
        final ThrottledExecutor child = root.newChild("LiveChild");

        final RecordingListener lateListener = new RecordingListener();
        root.addListener(lateListener);

        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();

        assertThat(lateListener.hooks()).containsExactly("SUBMIT", "START", "COMPLETE");
    }

    @Test
    @DisplayName("a listener added after a task was submitted does not see that task")
    void listenerAddedAfterSubmissionShouldNotSeeThatTask() throws InterruptedException {
        final ThrottledExecutor executor = new ThrottledExecutor("Snapshot");
        final CountDownLatch release = new CountDownLatch(1);

        final CompletableFuture<Void> future = executor.submit((Runnable) () -> {
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        final RecordingListener late = new RecordingListener();
        executor.addListener(late);
        release.countDown();
        future.join();
        executor.waitForCompletion();

        assertThat(late.events)
                .as("the listener set is snapshotted at onSubmit; later additions never see the task")
                .isEmpty();
    }

    @Test
    @DisplayName("removeListener on a child cannot remove an inherited listener")
    void removeListenerOnAChildShouldNotRemoveAnInheritedListener() {
        final ThrottledExecutor root = new ThrottledExecutor("RemoveRoot");
        final ThrottledExecutor child = root.newChild("RemoveChild");
        final RecordingListener inherited = new RecordingListener();
        root.addListener(inherited);

        assertThat(child.removeListener(inherited))
                .as("an inherited listener represents a decision made higher up the tree")
                .isFalse();

        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();

        assertThat(inherited.hooks()).containsExactly("SUBMIT", "START", "COMPLETE");
        assertThat(root.removeListener(inherited)).as("the owner can remove it").isTrue();
    }

    // ─── ServiceLoader discovery (explicit, per-root opt-in) ────────────────────

    /**
     * Discovery is a thing a root ASKS for. A jar on the classpath must never change an
     * executor's behaviour by its mere presence — so a plain root sees no discovered
     * listener, and a root that opted in sees every listener registered under
     * {@code META-INF/services/…TaskExecutionListener}.
     */
    @Test
    @DisplayName("addDiscoveredListeners attaches ServiceLoader-registered listeners; a plain root gets none")
    void discoveryIsAnExplicitPerRootOptIn() {
        DiscoverableTestListener.COMPLETIONS.set(0);

        final ThrottledExecutor plain = new ThrottledExecutor("NoDiscovery");
        plain.submit((Runnable) () -> { }).join();
        plain.waitForCompletion();
        assertThat(DiscoverableTestListener.COMPLETIONS.get())
                .as("a root that never opted in must not see the discovered listener")
                .isZero();

        final ThrottledExecutor discovering = new ThrottledExecutor("Discovery").addDiscoveredListeners();
        discovering.submit((Runnable) () -> { }).join();
        discovering.waitForCompletion();
        assertThat(DiscoverableTestListener.COMPLETIONS.get())
                .as("the test-scoped META-INF/services entry names DiscoverableTestListener")
                .isEqualTo(1);

        // Inherited like any other listener.
        final ThrottledExecutor child = discovering.newChild("DiscoveryChild");
        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();
        assertThat(DiscoverableTestListener.COMPLETIONS.get()).isEqualTo(2);
    }

    // ─── Robustness ─────────────────────────────────────────────────────────────

    @Test
    @DisplayName("a throwing listener neither fails the task nor suppresses the other listeners")
    void aThrowingListenerShouldNotFailTheTaskNorSuppressTheOthers() {
        final ThrottledExecutor executor = new ThrottledExecutor("Throwing");

        executor.addListener(new TaskExecutionListener<Void>() {
            @Override
            public Void onSubmit(TaskDescriptor t) {
                throw new IllegalStateException("submit boom");
            }

            @Override
            public void onStart(TaskDescriptor t, Void s) {
                throw new IllegalStateException("start boom");
            }

            @Override
            public void onComplete(TaskDescriptor t, Void s, TaskOutcome o) {
                throw new IllegalStateException("complete boom");
            }
        });
        final RecordingListener healthy = new RecordingListener();
        executor.addListener(healthy);

        final CompletableFuture<String> future = executor.submit((Supplier<String>) () -> "survived");
        assertThat(future.join()).isEqualTo("survived");
        executor.waitForCompletion();

        assertThat(healthy.hooks())
                .as("the listener after the throwing one still sees every hook")
                .containsExactly("SUBMIT", "START", "COMPLETE");
        assertThat(healthy.completion().outcome().status()).isEqualTo(TaskStatus.COMPLETED);
    }

    @Test
    @DisplayName("a submission rejected by a closed executor reports REJECTED, with no START")
    void rejectedSubmissionOnAClosedExecutorShouldReportRejected() {
        final ThrottledExecutor executor = new ThrottledExecutor("Rejected");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        executor.close();

        final CompletableFuture<Void> future = executor.submit((Runnable) () -> { });

        assertThat(future.isCompletedExceptionally()).isTrue();
        assertThat(listener.hooks()).containsExactly("SUBMIT", "COMPLETE");

        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.REJECTED);
        assertThat(outcome.failure()).isInstanceOf(IllegalStateException.class);
        assertThat(outcome.startedAtNanos()).isEqualTo(-1L);
        assertThat(executor.getFutures()).as("a rejected task is not tracked").isEmpty();
    }

    @Test
    @DisplayName("cancelling a running task reports CANCELLED_AFTER_START once the body returns")
    void cancelWhileTheBodyIsRunningShouldReportCancelledAfterStartWhenTheBodyReturns() throws InterruptedException {
        final ThrottledExecutor executor = new ThrottledExecutor("CancelRunning");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        final CompletableFuture<Void> future = executor.submit((Runnable) () -> {
            started.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });

        assertThat(started.await(10, TimeUnit.SECONDS)).isTrue();
        future.cancel(true);
        assertThat(future.isCancelled()).isTrue();
        assertThat(listener.hooks()).as("the body is still running — no COMPLETE yet").containsExactly("SUBMIT", "START");

        release.countDown();
        Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> listener.hooks().contains("COMPLETE"));

        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.CANCELLED_AFTER_START);
        assertThat(outcome.startedAtNanos()).isGreaterThanOrEqualTo(0L);
        assertThat(listener.hooks()).containsExactly("SUBMIT", "START", "COMPLETE");
    }

    // ─── The ordering guarantee ─────────────────────────────────────────────────

    /**
     * The guarantee the metadata tree depends on: when {@code waitForCompletion()} returns,
     * every task's {@code onComplete} has already run. A listener that is slow in
     * {@code onComplete} must therefore delay the wait, not race it.
     */
    @RepeatedTest(20)
    @DisplayName("waitForCompletion returns only after every onComplete has run")
    void waitForCompletionShouldObserveEveryOnComplete() {
        final ThrottledExecutor executor = new ThrottledExecutor("ObserveComplete");
        final AtomicInteger completions = new AtomicInteger();
        final int taskCount = 32;

        executor.addListener(new TaskExecutionListener<Void>() {
            @Override
            public void onComplete(TaskDescriptor t, Void s, TaskOutcome o) {
                try {
                    Thread.sleep(2);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                completions.incrementAndGet();
            }
        });

        for (int i = 0; i < taskCount; i++) {
            executor.submit((Runnable) () -> { });
        }
        executor.waitForCompletion();

        assertThat(completions.get())
                .as("no waiting, no polling: the wait itself must imply the listeners ran")
                .isEqualTo(taskCount);
    }

    /**
     * A task's scope is closed BEFORE its future completes, so a dependent registered on the
     * future never sees the task's own context. Pins the record -> onComplete -> settle order.
     */
    @Test
    @DisplayName("a task's scope ends before its future completes")
    void theTasksScopeShouldEndBeforeItsFutureCompletes() throws InterruptedException {
        final ThrottledExecutor executor = new ThrottledExecutor("ScopeOrder");
        final CountDownLatch inBody = new CountDownLatch(1);
        final CountDownLatch release = new CountDownLatch(1);

        MDC.put("job", "task-context");
        final CompletableFuture<Void> future = executor.submit((Runnable) () -> {
            inBody.countDown();
            try {
                release.await(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        });
        MDC.clear();

        assertThat(inBody.await(10, TimeUnit.SECONDS)).isTrue();
        // Registered while the body runs; executes on the worker right after completion.
        final CompletableFuture<String> dependent = future.thenApply(v -> MDC.get("job"));
        release.countDown();

        assertThat(dependent.join())
                .as("the dependent runs after the task's MDC scope was restored, so it sees no task context")
                .isNull();
    }

    // ─── Metadata plumbing ──────────────────────────────────────────────────────

    @Test
    @DisplayName("a throttled submission exposes the ThrottledTask name and size in the descriptor")
    void throttledSubmissionShouldExposeNameAndSizeInTheDescriptor() {
        final ThrottledExecutor executor = new ThrottledExecutor("Throttled");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);

        executor.submitThrottled(
                List.of(new ThrottledTask<>((Runnable) () -> { }, 1234L, "named")),
                new MaxConcurrencyPolicy(4));
        executor.waitForCompletion();

        final TaskDescriptor task = listener.completion().task();
        assertThat(task.name()).isEqualTo("named");
        assertThat(task.sizeEstimate()).isEqualTo(1234L);
    }

    @Test
    @DisplayName("a Callable submission keeps the original callable as the tracked job")
    void callableSubmissionShouldKeepTheOriginalCallableAsTheJob() {
        final ThrottledExecutor executor = new ThrottledExecutor("CallableJob");
        final Callable<String> callable = () -> "value";

        executor.submit(callable).join();

        final FuturePair pair = executor.getFutures().findFirst().orElseThrow();
        assertThat(pair.getJob())
                .as("no synthetic wrapper — the user's own callable is what is tracked")
                .isSameAs(callable);
        assertThat(pair.getDescriptor()).isNotNull();
    }
}
