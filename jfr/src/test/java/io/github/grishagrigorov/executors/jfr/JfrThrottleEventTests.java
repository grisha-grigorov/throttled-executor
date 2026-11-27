package io.github.grishagrigorov.executors.jfr;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The throttle event, driven through a real Flight Recorder recording and read back from the
 * dump — so what is asserted is what JDK Mission Control would show.
 */
class JfrThrottleEventTests {

    @Test
    @DisplayName("a throttled batch emits one throttle event per task, with the executor's identity")
    void oneEventPerThrottledTask() throws Exception {

        final List<RecordedEvent> events = record(() -> {
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("JfrThrottle", ExecutorBackend.CALLER_RUNS);
            executor.addListener(JfrTaskListener.INSTANCE);
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "alpha"),
                    ThrottledTask.<Runnable>of(() -> { }, 10L, "beta")),
                    new MaxConcurrencyPolicy(4));
            executor.waitForCompletion();
            executor.close();
        });

        assertThat(events).hasSize(2);
        assertThat(events).allSatisfy(event -> {
            assertThat(event.getString("executorId")).contains("JfrThrottle");
            assertThat(event.getString("executorPath")).isNotBlank();
            assertThat(event.getLong("waitedNanos")).isGreaterThanOrEqualTo(0);
        });
        assertThat(events).extracting(event -> event.getString("taskName"))
                .containsExactlyInAnyOrder("alpha", "beta");
    }

    @Test
    @DisplayName("a task the liveness floor had to rescue is marked floorAdmitted")
    void floorAdmittedIsRecorded() throws Exception {

        final List<RecordedEvent> events = record(() -> {
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("JfrFloor", ExecutorBackend.CALLER_RUNS);
            executor.addListener(JfrTaskListener.INSTANCE);

            final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;
            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "rescued")), refuseAll);
            executor.waitForCompletion();
            executor.close();
        });

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getBoolean("floorAdmitted"))
                .as("the policy refused it; only the floor started it")
                .isTrue();
    }

    @Test
    @DisplayName("a plain submit produces no throttle event — it went through no policy")
    void plainSubmitEmitsNothing() throws Exception {

        final List<RecordedEvent> events = record(() -> {
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("JfrPlain", ExecutorBackend.CALLER_RUNS);
            executor.addListener(JfrTaskListener.INSTANCE);
            executor.submit(() -> { });
            executor.waitForCompletion();
            executor.close();
        });

        assertThat(events).isEmpty();
    }

    /** Runs {@code body} inside a recording of the throttle event and returns what it produced. */
    private static List<RecordedEvent> record(ThrowingRunnable body) throws Exception {

        final Path dump = Files.createTempFile("executors-jfr-throttle-", ".jfr");

        try (Recording recording = new Recording()) {
            // Threshold ZERO: this is an instantaneous event, and a non-zero threshold would
            // drop every one of them.
            recording.enable(ThrottleEvent.NAME).withThreshold(Duration.ZERO);
            recording.start();
            body.run();
            recording.stop();
            recording.dump(dump);
        }

        try {
            final List<RecordedEvent> ours = new ArrayList<>();
            for (RecordedEvent event : RecordingFile.readAllEvents(dump)) {
                if (event.getEventType().getName().equals(ThrottleEvent.NAME)) {
                    ours.add(event);
                }
            }
            return ours;
        } finally {
            Files.deleteIfExists(dump);
        }
    }

    @FunctionalInterface
    interface ThrowingRunnable {
        void run() throws Exception;
    }
}
