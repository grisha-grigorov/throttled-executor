package io.github.grishagrigorov.executors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The {@code CALLER_RUNS} backend: task bodies run synchronously on the submitting thread, so a
 * test that uses the library gets deterministic ordering while still exercising the listener
 * fan-out, the completion state machine and failure aggregation.
 */
@DisplayName("ExecutorBackend.CALLER_RUNS")
class CallerRunsBackendTests {

    @Nested
    @DisplayName("execution")
    class Execution {

        @Test
        @DisplayName("the body runs on the submitting thread, before submit() returns")
        void runsInline() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Inline", ExecutorBackend.CALLER_RUNS);
            final AtomicReference<String> ranOn = new AtomicReference<>();

            executor.submit(() -> ranOn.set(Thread.currentThread().getName()));

            assertThat(ranOn.get())
                    .as("no worker thread was involved")
                    .isEqualTo(Thread.currentThread().getName());
            executor.close();
        }

        @Test
        @DisplayName("a throttled batch is finished by the time submitThrottled returns")
        void throttledBatchIsDoneOnReturn() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("InlineBatch", ExecutorBackend.CALLER_RUNS);
            final List<String> order = new ArrayList<>();

            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> order.add("small"), 100L, "small"),
                    ThrottledTask.<Runnable>of(() -> order.add("large"), 900L, "large")),
                    new MaxConcurrencyPolicy(4));

            assertThat(order)
                    .as("largest first, and already run — no wait needed")
                    .containsExactly("large", "small");
            executor.close();
        }

        @Test
        @DisplayName("failures still aggregate through waitForCompletion")
        void failuresStillAggregate() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("InlineFail", ExecutorBackend.CALLER_RUNS);

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                throw new IllegalStateException("boom");
            }, 10L, "bad")));

            assertThatThrownBy(executor::waitForCompletion)
                    .isInstanceOf(ThreadContextAwareCompletionException.class)
                    .hasMessageContaining("boom");
            executor.close();
        }

        @Test
        @DisplayName("listeners see the full lifecycle, in order, on the one thread")
        void listenersStillFire() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("InlineListen", ExecutorBackend.CALLER_RUNS);
            final List<String> events = new ArrayList<>();

            executor.addListener(new TaskExecutionListener<Void>() {
                @Override
                public Void onSubmit(TaskDescriptor task) {
                    events.add("submit");
                    return null;
                }

                @Override
                public void onStart(TaskDescriptor task, Void state) {
                    events.add("start");
                }

                @Override
                public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {
                    events.add("complete:" + outcome.status());
                }
            });

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "A")));

            assertThat(events).containsExactly("submit", "start", "complete:" + TaskStatus.COMPLETED);
            executor.close();
        }
    }

    @Nested
    @DisplayName("the backend's own answers")
    class Answers {

        @Test
        @DisplayName("it is always available, never creates a pool, and admits one at a time")
        void backendProperties() {

            assertThat(ExecutorBackend.CALLER_RUNS.isAvailable()).isTrue();
            assertThat(ExecutorBackend.CALLER_RUNS.isCreated())
                    .as("there is no pool to create")
                    .isFalse();
            assertThat(ExecutorBackend.CALLER_RUNS.defaultMaxConcurrency())
                    .as("one thread, one task")
                    .isEqualTo(1);
        }

        @Test
        @DisplayName("the system property selects it by name")
        void selectableByProperty() {

            final String previous = System.getProperty(ExecutorBackend.BACKEND_PROPERTY);
            try {
                System.setProperty(ExecutorBackend.BACKEND_PROPERTY, "caller_runs");
                assertThat(ExecutorBackend.defaultBackend()).isEqualTo(ExecutorBackend.CALLER_RUNS);

                System.setProperty(ExecutorBackend.BACKEND_PROPERTY, "callerRuns");
                assertThat(ExecutorBackend.defaultBackend()).isEqualTo(ExecutorBackend.CALLER_RUNS);
            } finally {
                if (previous == null) {
                    System.clearProperty(ExecutorBackend.BACKEND_PROPERTY);
                } else {
                    System.setProperty(ExecutorBackend.BACKEND_PROPERTY, previous);
                }
            }
        }
    }

    @Nested
    @DisplayName("liveness")
    class Liveness {

        @Test
        @DisplayName("a policy that refuses everything still makes progress — the floor is what saves it")
        void thefloorPreventsLivelock() {

            // Nothing else can complete to change the harness state, so without the liveness
            // floor this batch would poll every 100ms forever. With it, work drains serially.
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("InlineFloor", ExecutorBackend.CALLER_RUNS);
            final List<String> ran = new ArrayList<>();

            executor.submitThrottled(
                    List.of(ThrottledTask.<Runnable>of(() -> ran.add("a"), 10L, "a"),
                            ThrottledTask.<Runnable>of(() -> ran.add("b"), 10L, "b")),
                    new MaxConcurrencyPolicy(0));

            assertThat(ran).containsExactlyInAnyOrder("a", "b");
            executor.close();
        }
    }
}
