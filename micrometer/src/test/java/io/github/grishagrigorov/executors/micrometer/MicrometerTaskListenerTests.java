package io.github.grishagrigorov.executors.micrometer;

import java.util.List;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The Micrometer binder, driven through a real executor into a real {@link SimpleMeterRegistry}
 * and read back out of it — no mocking, so what is asserted is what a Prometheus scrape sees.
 */
@DisplayName("MicrometerTaskListener")
class MicrometerTaskListenerTests {

    @Nested
    @DisplayName("task meters")
    class TaskMeters {

        @Test
        @DisplayName("completed tasks are counted by status and timed")
        void countsAndTimes() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Metered", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "a"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "b")),
                    new MaxConcurrencyPolicy(4));
            executor.waitForCompletion();

            assertThat(registry.get("executors.tasks").tag("status", "COMPLETED").counter().count())
                    .isEqualTo(2);
            assertThat(registry.get("executors.task.duration").timer().count()).isEqualTo(2);
            assertThat(registry.get("executors.task.queued").timer().count()).isEqualTo(2);
            executor.close();
        }

        @Test
        @DisplayName("a failure is counted under its own status")
        void countsFailures() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("MeteredFail", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> {
                throw new IllegalStateException("boom");
            }, 10L, "bad")));

            try {
                executor.waitForCompletion();
            } catch (RuntimeException expected) {
                // the aggregate; the meters are what this test is about
            }

            assertThat(registry.get("executors.tasks").tag("status", "FAILED").counter().count())
                    .isEqualTo(1);
            executor.close();
        }

        @Test
        @DisplayName("meters are tagged by executor id, never by task name")
        void taggedByExecutorNotTaskName() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Tagged", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            // Unbounded task names are the classic way to melt a metrics backend.
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "voyage-1"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "voyage-2"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "voyage-3")));
            executor.waitForCompletion();

            assertThat(registry.find("executors.tasks").counters())
                    .as("one series, not one per task name")
                    .hasSize(1);
            assertThat(registry.get("executors.tasks").counter().getId().getTag("executor"))
                    .contains("Tagged");
            executor.close();
        }
    }

    @Nested
    @DisplayName("throttle meters")
    class ThrottleMeters {

        @Test
        @DisplayName("admission waits are timed")
        void timesAdmissionWaits() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("MeteredWait", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "a")),
                    new MaxConcurrencyPolicy(4));
            executor.waitForCompletion();

            assertThat(registry.get("executors.throttle.wait").timer().count()).isEqualTo(1);
            executor.close();
        }

        @Test
        @DisplayName("floor admissions are counted — the meter to alarm on")
        void countsFloorAdmissions() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("MeteredFloor", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "a"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "b")), refuseAll);
            executor.waitForCompletion();

            assertThat(registry.get("executors.throttle.floorAdmitted").counter().count())
                    .isEqualTo(2);
            executor.close();
        }

        @Test
        @DisplayName("a plain submit produces no throttle meters at all")
        void plainSubmitPublishesNoThrottleMeters() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("MeteredPlain", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry));

            executor.submit(() -> { });
            executor.waitForCompletion();

            assertThat(registry.find("executors.throttle.wait").timer()).isNull();
            assertThat(registry.find("executors.throttle.floorAdmitted").counter()).isNull();
            executor.close();
        }
    }

    @Nested
    @DisplayName("naming")
    class Naming {

        @Test
        @DisplayName("the prefix is configurable, so the meters can live under the application's own namespace")
        void customPrefix() {

            final MeterRegistry registry = new SimpleMeterRegistry();
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Prefixed", ExecutorBackend.CALLER_RUNS);
            executor.addListener(new MicrometerTaskListener(registry, "ship.reports"));

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "a")));
            executor.waitForCompletion();

            assertThat(registry.get("ship.reports.tasks").counter().count()).isEqualTo(1);
            assertThat(registry.find("executors.tasks").counter()).isNull();
            executor.close();
        }
    }
}
