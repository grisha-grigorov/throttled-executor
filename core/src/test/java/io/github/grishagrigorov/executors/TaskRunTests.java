package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import io.github.grishagrigorov.executors.TaskExecutionListenerTests.RecordingListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.util.concurrent.CancellationException;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Deterministic tests for the per-task completion state machine, driven on the test thread
 * through the package-private {@code newRun(...)} factory — which prepares a task (descriptor,
 * {@code onSubmit}, future, registration) but does <em>not</em> schedule it. That lets the
 * cancel transitions be exercised at exact points, without racing a pool.
 *
 * <p>The machine is {@code SUBMITTED → STARTED → DONE}, with {@code CANCEL_REQUESTED} as a
 * side state entered only from {@code STARTED}. A single CAS arbitrates between the worker
 * and a canceller; there is deliberately no {@code isDone()}-then-flag sequence, because a
 * cancel landing between those two steps would fire {@code onComplete} before
 * {@code onStart} and leave listener-installed thread state un-restored on a pooled worker.</p>
 */
class TaskRunTests {

    @Test
    @DisplayName("cancel before execute: the body never runs and the outcome is CANCELLED_BEFORE_START")
    void cancelBeforeExecuteShouldSkipTheBodyAndReportCancelledBeforeStart() {
        final ThrottledExecutor executor = new ThrottledExecutor("RunCancelBefore");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        final AtomicBoolean bodyRan = new AtomicBoolean(false);

        final ThrottledExecutor.TaskRun<String> run = executor.newRun(() -> {
            bodyRan.set(true);
            return "never";
        }, "job", null, -1L);

        assertThat(run.future().cancel(true)).isTrue();
        run.execute();   // what the pool would do next — must be a no-op now

        assertThat(bodyRan).as("a cancelled-while-queued body must not run").isFalse();
        assertThat(listener.hooks()).containsExactly("SUBMIT", "COMPLETE");

        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.CANCELLED_BEFORE_START);
        assertThat(outcome.startedAtNanos()).isEqualTo(-1L);
        assertThat(outcome.failure()).isInstanceOf(CancellationException.class);
        assertThat(run.future().isCancelled()).isTrue();
    }

    @Test
    @DisplayName("cancel during the body: the worker must not overwrite the cancelled future")
    void cancelDuringTheBodyShouldNotLetTheWorkerOverwriteTheCancelledFuture() {
        final ThrottledExecutor executor = new ThrottledExecutor("RunCancelDuring");
        final RecordingListener listener = new RecordingListener();
        executor.addListener(listener);
        final AtomicReference<ThrottledExecutor.TaskRun<String>> self = new AtomicReference<>();

        final Callable<String> body = () -> {
            // The canceller strikes while the body is running.
            assertThat(self.get().future().cancel(true)).isTrue();
            return "value-that-must-not-win";
        };
        final ThrottledExecutor.TaskRun<String> run = executor.newRun(body, "job", null, -1L);
        self.set(run);

        run.execute();

        assertThat(run.future().isCancelled()).isTrue();
        assertThat(catchThrowable(() -> run.future().join())).isInstanceOf(CancellationException.class);

        assertThat(listener.hooks())
                .as("onStart fired (the body ran) and onComplete fired exactly once, on the worker")
                .containsExactly("SUBMIT", "START", "COMPLETE");
        final TaskOutcome outcome = listener.completion().outcome();
        assertThat(outcome.status()).isEqualTo(TaskStatus.CANCELLED_AFTER_START);
        assertThat(outcome.startedAtNanos()).isGreaterThanOrEqualTo(0L);
    }

    @Test
    @DisplayName("a cancellation is recorded at cancel time, so the next wait reports it")
    void theRecordedCancellationShouldBeDrainedByWaitForOwnCompletion() {
        final ThrottledExecutor executor = new ThrottledExecutor("RunRecordCancel");

        final ThrottledExecutor.TaskRun<String> run = executor.newRun(() -> "never", "job", null, -1L);
        run.future().cancel(true);

        final Throwable thrown = catchThrowable(executor::waitForOwnCompletion);

        assertThat(thrown)
                .as("a cancelled task is not a clean batch — today's semantics, preserved")
                .isInstanceOf(ThreadContextAwareCompletionException.class);
        final ThreadContextAwareCompletionException aggregate = (ThreadContextAwareCompletionException) thrown;
        assertThat(aggregate.aggregationOfThrowables).hasSize(1);
        assertThat(io.github.grishagrigorov.executors.exceptions.RootCauseExceptionUtil
                .getRootCause(aggregate.aggregationOfThrowables[0]))
                .isInstanceOf(CancellationException.class);
    }

    @Test
    @DisplayName("a successful run settles the future only after the listeners' onComplete")
    void successfulRunShouldSettleTheFutureAfterOnComplete() {
        final ThrottledExecutor executor = new ThrottledExecutor("RunOrder");
        final AtomicBoolean futureDoneDuringOnComplete = new AtomicBoolean(true);
        final AtomicReference<ThrottledExecutor.TaskRun<String>> self = new AtomicReference<>();

        executor.addListener(new io.github.grishagrigorov.executors.spi.TaskExecutionListener<Void>() {
            @Override
            public void onComplete(io.github.grishagrigorov.executors.spi.TaskDescriptor t, Void s, TaskOutcome o) {
                futureDoneDuringOnComplete.set(self.get().future().isDone());
            }
        });

        final ThrottledExecutor.TaskRun<String> run = executor.newRun(() -> "value", "job", null, -1L);
        self.set(run);
        run.execute();

        assertThat(futureDoneDuringOnComplete)
                .as("record -> onComplete -> settle: inside onComplete the future is NOT yet done")
                .isFalse();
        assertThat(run.future().join()).isEqualTo("value");
    }
}
