package io.github.grishagrigorov.executors.jfr;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ExecutorBackend;
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JFR satellite. Every test drives a <em>real</em> Flight Recorder recording,
 * dumps it to a file and reads the events back — no mocking — so what is asserted is what
 * JDK Mission Control would show.
 *
 * <p>This is also the proof the split exists for: a module that does something useful with
 * nothing but core's public SPI and the JDK.</p>
 */
class JfrTaskListenerTests {

    /** Runs {@code body} inside a recording of our event and returns the events it produced. */
    private static List<RecordedEvent> record(ThrowingRunnable body) throws Exception {
        final Path dump = Files.createTempFile("executors-jfr-", ".jfr");
        try (Recording recording = new Recording()) {
            // Threshold ZERO: a duration event shorter than the threshold is silently dropped,
            // and our bodies are often microseconds long.
            recording.enable(TaskEvent.NAME).withThreshold(Duration.ZERO).withStackTrace();
            recording.start();
            body.run();
            recording.stop();
            recording.dump(dump);
        }
        try {
            final List<RecordedEvent> ours = new ArrayList<>();
            for (RecordedEvent event : RecordingFile.readAllEvents(dump)) {
                if (event.getEventType().getName().equals(TaskEvent.NAME)) {
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

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Test
    @DisplayName("a completed task emits one event carrying its duration, name and executor path")
    void aCompletedTaskEmitsOneEventWithItsDurationAndPath() throws Exception {
        final ThrottledExecutor root = new ThrottledExecutor("JfrRoot");
        root.addListener(JfrTaskListener.INSTANCE);

        final List<RecordedEvent> events = record(() -> {
            root.submit((Runnable) () -> sleep(150));
            root.waitForCompletion();
        });

        assertThat(events).hasSize(1);
        final RecordedEvent event = events.get(0);
        assertThat(event.getString("status")).isEqualTo("COMPLETED");
        assertThat(event.getBoolean("bodyRan")).isTrue();
        assertThat(event.getString("taskName")).as("plain submissions get a synthesised name").startsWith("task#");
        assertThat(event.getString("executorId")).isEqualTo(root.executorId);
        assertThat(event.getString("executorPath")).contains(root.executorId);
        assertThat(event.getString("backend")).isEqualTo(root.backend().name());
        /*
         * begin() at onStart, end() at onComplete: the event's duration is the body's wall
         * time — nominally. The bound is deliberately loose (150 ms body, >= 40 ms
         * asserted) because JFR durations come from the JVM's TICK clock, and on Apple
         * Silicon that clock can drift from nanoTime after a host sleep/wake cycle:
         * measured here (standalone repro, two Liberica JDKs) reporting a 200 ms sleep as
         * ~81 ms — a ~2.5x under-read that heals on reboot and never appears in the Linux
         * CI container. The assertion proves begin/end genuinely bracket the body without
         * assuming the two clocks agree.
         */
        assertThat(event.getDuration())
                .as("the event brackets the body (loose: JFR ticks may under-read vs nanoTime on macOS after sleep/wake)")
                .isGreaterThanOrEqualTo(Duration.ofMillis(40));
        assertThat(event.getString("failure")).isNull();
    }

    @Test
    @DisplayName("a failed task records the failure class and message, not the throwable")
    void aFailedTaskRecordsTheFailureClassAndMessageWithoutTheThrowable() throws Exception {
        final ThrottledExecutor root = new ThrottledExecutor("JfrFailing");
        root.addListener(JfrTaskListener.INSTANCE);

        final List<RecordedEvent> events = record(() -> {
            final CompletableFuture<Void> f = root.submit((Runnable) () -> {
                throw new IllegalStateException("the reactor is leaking");
            });
            assertThat(f).failsWithin(Duration.ofSeconds(10));
            try {
                root.waitForCompletion();
            } catch (RuntimeException expected) {
                // the aggregate — fine
            }
        });

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getString("status")).isEqualTo("FAILED");
        assertThat(events.get(0).getString("failure"))
                .contains("IllegalStateException")
                .contains("the reactor is leaking");
    }

    @Test
    @DisplayName("a throttled task carries the ThrottledTask name and size estimate")
    void aThrottledTaskCarriesNameAndSizeEstimate() throws Exception {
        final ThrottledExecutor root = new ThrottledExecutor("JfrThrottled");
        root.addListener(JfrTaskListener.INSTANCE);

        final List<RecordedEvent> events = record(() -> {
            root.submitThrottled(
                    List.of(new ThrottledTask<>((Runnable) () -> { }, 1234L, "VoyageReport")),
                    new MaxConcurrencyPolicy(4));
            root.waitForCompletion();
        });

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getString("taskName")).isEqualTo("VoyageReport");
        assertThat(events.get(0).getLong("sizeEstimate")).isEqualTo(1234L);
    }

    /**
     * Cancelled while queued: saturate the ForkJoin backend's workers with blocked tasks so
     * the next submission is genuinely queued, cancel it, then release. The body never ran,
     * and that is precisely the kind of task worth seeing in a production recording.
     */
    @Test
    @DisplayName("a task cancelled before it started still emits an event, with no duration")
    void aCancelledBeforeStartTaskStillEmitsAnEvent() throws Exception {
        final ThrottledExecutor root = ThrottledExecutor.withBackend("JfrCancelQueued", ExecutorBackend.FORK_JOIN);
        root.addListener(JfrTaskListener.INSTANCE);

        final int available = Runtime.getRuntime().availableProcessors();
        final int poolSize = (available - 2) <= 2 ? available : available - 2;   // mirrors ExecutorBackend
        final CountDownLatch allBlocked = new CountDownLatch(poolSize);
        final CountDownLatch release = new CountDownLatch(1);

        final List<RecordedEvent> events = record(() -> {
            for (int i = 0; i < poolSize; i++) {
                root.submit((Runnable) () -> {
                    allBlocked.countDown();
                    try {
                        release.await(30, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
            }
            assertThat(allBlocked.await(30, TimeUnit.SECONDS)).as("every worker is now blocked").isTrue();

            final CompletableFuture<Void> queued = root.submit((Runnable) () -> { });
            assertThat(queued.cancel(true)).isTrue();
            release.countDown();
            try {
                root.waitForCompletion();
            } catch (RuntimeException expectedBecauseOfTheCancellation) {
                // a cancelled task makes the batch report as not clean — unchanged semantics
            }
        });

        final List<RecordedEvent> cancelled = new ArrayList<>();
        for (RecordedEvent e : events) {
            if (e.getString("status").equals("CANCELLED_BEFORE_START")) {
                cancelled.add(e);
            }
        }
        assertThat(cancelled).hasSize(1);
        assertThat(cancelled.get(0).getBoolean("bodyRan")).isFalse();
        assertThat(cancelled.get(0).getDuration()).isLessThan(Duration.ofMillis(5));
    }

    @Test
    @DisplayName("a rejected submission still emits an event")
    void aRejectedSubmissionStillEmitsAnEvent() throws Exception {
        final ThrottledExecutor root = new ThrottledExecutor("JfrRejected");
        root.addListener(JfrTaskListener.INSTANCE);
        root.close();

        final List<RecordedEvent> events = record(() -> {
            final CompletableFuture<Void> rejected = root.submit((Runnable) () -> { });
            assertThat(rejected.isCompletedExceptionally()).isTrue();
        });

        assertThat(events).hasSize(1);
        assertThat(events.get(0).getString("status")).isEqualTo("REJECTED");
        assertThat(events.get(0).getBoolean("bodyRan")).isFalse();
        assertThat(events.get(0).getString("failure")).contains("IllegalStateException");
    }

    @Test
    @DisplayName("with no recording active the listener keeps no per-task state")
    void noRecordingMeansNoEvents() {
        final ThrottledExecutor root = new ThrottledExecutor("JfrIdle");
        final TaskDescriptor descriptor = new TaskDescriptor(root, 1, null, -1, System.nanoTime(), System.currentTimeMillis());

        assertThat(JfrTaskListener.INSTANCE.onSubmit(descriptor))
                .as("no recording enables our event, so onSubmit hands back no state and the other hooks are no-ops")
                .isNull();
    }

    @Test
    @DisplayName("the listener is discoverable through addDiscoveredListeners()")
    void theListenerIsDiscoverableViaServiceLoader() throws Exception {
        final ThrottledExecutor root = new ThrottledExecutor("JfrDiscovered").addDiscoveredListeners();

        final List<RecordedEvent> events = record(() -> {
            root.submit((Runnable) () -> { });
            root.waitForCompletion();
        });

        assertThat(events)
                .as("the META-INF/services entry in this module registers JfrTaskListener")
                .hasSize(1);
    }
}
