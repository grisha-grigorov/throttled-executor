package io.github.grishagrigorov.executors.observability;

import java.util.List;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The recorder's view of admission: how much time a batch spent waiting to be let in, and how
 * much of it only ran because the liveness floor overrode the policy.
 */
@DisplayName("recording throttle waits")
class ThrottleWaitRecordingTests {

    @Test
    @DisplayName("throttled tasks are counted, with their total wait")
    void countsThrottledTasks() {

        final ExecutionRecorder recorder = new ExecutionRecorder(RecorderConfig.defaults());
        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Counted", ExecutorBackend.CALLER_RUNS);
        executor.addListener(recorder);

        executor.submitThrottled(List.of(
                ThrottledTask.<Runnable>of(() -> { }, 10L, "a"),
                ThrottledTask.<Runnable>of(() -> { }, 10L, "b")),
                new MaxConcurrencyPolicy(4));
        executor.waitForCompletion();

        assertThat(recorder.getThrottledTotal()).isEqualTo(2);
        assertThat(recorder.getThrottleWaitNanosTotal()).isGreaterThanOrEqualTo(0);
        executor.close();
    }

    @Test
    @DisplayName("the discovery path counts admissions too — not only an explicitly added recorder")
    void discoveryPathCountsAdmissions() {

        /*
         * root.addDiscoveredListeners() attaches DiscoveredExecutionRecorder, the ServiceLoader
         * facade, which forwards to ExecutionRecorder.shared(). It forwarded onSubmit, onStart
         * and onComplete — and not onThrottled, which fell through to the SPI's no-op default.
         * Every admission counter on the shared recorder stayed at zero for anyone using the
         * documented discovery path: exactly the "why was this batch slow" signal the counters
         * exist for, missing on the path most deployments use.
         *
         * The shared recorder is process-wide, so this measures deltas rather than absolutes.
         */
        final ExecutionRecorder shared = ExecutionRecorder.shared();
        final long throttledBefore = shared.getThrottledTotal();
        final long floorBefore = shared.getFloorAdmittedTotal();

        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Discovered", ExecutorBackend.CALLER_RUNS);
        executor.addDiscoveredListeners();

        final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;
        executor.submitThrottled(List.of(
                ThrottledTask.<Runnable>of(() -> { }, 10L, "a"),
                ThrottledTask.<Runnable>of(() -> { }, 10L, "b")), refuseAll);
        executor.waitForCompletion();

        assertThat(shared.getThrottledTotal() - throttledBefore)
                .as("both throttled admissions reached the shared recorder through discovery")
                .isEqualTo(2);
        assertThat(shared.getFloorAdmittedTotal() - floorBefore)
                .as("and both were floor admissions")
                .isEqualTo(2);
        executor.close();
    }

    @Test
    @DisplayName("floor admissions are counted separately — that is the number worth alarming on")
    void countsFloorAdmissions() {

        final ExecutionRecorder recorder = new ExecutionRecorder(RecorderConfig.defaults());
        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Floored", ExecutorBackend.CALLER_RUNS);
        executor.addListener(recorder);

        final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;
        executor.submitThrottled(List.of(
                ThrottledTask.<Runnable>of(() -> { }, 10L, "a"),
                ThrottledTask.<Runnable>of(() -> { }, 10L, "b")), refuseAll);
        executor.waitForCompletion();

        assertThat(recorder.getFloorAdmittedTotal())
                .as("the policy refused both; only the floor ran them")
                .isEqualTo(2);
        executor.close();
    }

    @Test
    @DisplayName("a plain submit is not throttled, so it is not counted")
    void plainSubmitIsNotCounted() {

        final ExecutionRecorder recorder = new ExecutionRecorder(RecorderConfig.defaults());
        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Plain", ExecutorBackend.CALLER_RUNS);
        executor.addListener(recorder);

        executor.submit(() -> { });
        executor.waitForCompletion();

        assertThat(recorder.getThrottledTotal()).isZero();
        assertThat(recorder.getFloorAdmittedTotal()).isZero();
        executor.close();
    }

    @Test
    @DisplayName("the totals reach both renderings, so the JMX console shows them")
    void totalsAreRendered() {

        final ExecutionRecorder recorder = new ExecutionRecorder(RecorderConfig.defaults());
        final ThrottledExecutor executor =
                ThrottledExecutor.withBackend("Rendered", ExecutorBackend.CALLER_RUNS);
        executor.addListener(recorder);

        final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;
        executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "a")), refuseAll);
        executor.waitForCompletion();

        assertThat(recorder.renderText()).contains("floor-admitted");
        assertThat(recorder.renderJson()).contains("floorAdmittedTotal");
        executor.close();
    }
}
