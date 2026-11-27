package io.github.grishagrigorov.executors;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@code onThrottled} — the hook that answers "why was this batch slow?".
 *
 * <p>Queue latency was already derivable from the descriptor's timestamps, but nothing said
 * whether a task waited because the policy refused it or was let through by the liveness floor,
 * which is the difference between a throttle working and a throttle giving up.</p>
 */
@DisplayName("onThrottled")
class ThrottleWaitListenerTests {

    @Nested
    @DisplayName("when nothing had to wait")
    class NoWait {

        @Test
        @DisplayName("a task admitted on the first scan reports a negligible wait")
        void admittedImmediately() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("NoWait", ExecutorBackend.CALLER_RUNS);
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "A")),
                    new MaxConcurrencyPolicy(4));

            assertThat(recorder.events).hasSize(1);
            // Time-to-admission, not "did the coordinator block": a task admitted on the first
            // scan still spent the sub-millisecond it took to get there.
            assertThat(recorder.events.get(0).waitedNanos())
                    .isNotNegative()
                    .isLessThan(java.util.concurrent.TimeUnit.SECONDS.toNanos(1));
            executor.close();
        }

        @Test
        @DisplayName("a plain submit() is not throttled at all, so the hook never fires for it")
        void plainSubmitDoesNotFire() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Plain", ExecutorBackend.CALLER_RUNS);
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            executor.submit(() -> { });

            assertThat(recorder.events).isEmpty();
            executor.close();
        }
    }

    @Nested
    @DisplayName("when the policy makes it wait")
    class Waited {

        @Test
        @DisplayName("the wait is reported, and the hook fires between onSubmit and onStart")
        void reportsWaitAndOrdering() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("Waited");
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch occupied = new CountDownLatch(1);

            /*
             * The coordinator runs on its own thread. submitThrottled blocks until the whole
             * batch is admitted, so calling it here would leave the test thread unable to reach
             * release.countDown() until "first" had returned — which needs the release. The test
             * still passed, thirty seconds later, on the task's own await timeout; it was
             * measuring the timeout, not the ordering.
             */
            final List<ThrottledTask<Runnable>> batch = List.of(
                    ThrottledTask.<Runnable>of(() -> {
                        occupied.countDown();
                        try {
                            release.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, 900L, "first"),
                    ThrottledTask.<Runnable>of(() -> { }, 100L, "second"));

            // One at a time: the second task cannot be admitted until the first returns.
            final Thread coordinator = new Thread(
                    () -> executor.submitThrottled(batch, new MaxConcurrencyPolicy(1)), "waited");
            coordinator.start();

            assertThat(occupied.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(200);              // "second" is now genuinely waiting, not merely queued
            release.countDown();

            coordinator.join(10_000);
            assertThat(coordinator.isAlive()).as("the coordinator finished its batch").isFalse();
            executor.waitForCompletion();

            final Event second = recorder.forTask("second");
            assertThat(second.waitedNanos()).as("it really did wait").isPositive();
            assertThat(recorder.order)
                    .as("the hook belongs to the task's own lifecycle")
                    .containsSubsequence("submit:second", "throttled:second", "start:second", "complete:second");
            executor.close();
        }
    }

    @Nested
    @DisplayName("a burst of admissions")
    class Burst {

        @Test
        @DisplayName("every task freed at once reports its own wait, not just the first one")
        void allTasksInABurstReportTheirWait() throws Exception {

            /*
             * The regression this guards: waitedNanos used to be timed from the coordinator's
             * first refusal and reset after each admission, so when several slots freed together
             * only the first task admitted reported anything and the rest reported zero — while
             * having waited just as long.
             */
            final ThrottledExecutor executor = new ThrottledExecutor("Burst");
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch blocking = new CountDownLatch(1);

            final List<ThrottledTask<Runnable>> batch = new java.util.ArrayList<>();
            batch.add(ThrottledTask.of(() -> {
                blocking.countDown();
                try {
                    release.await(30, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }, 900L, "blocker"));
            for (int i = 0; i < 3; i++) {
                batch.add(ThrottledTask.of(() -> { }, 100L, "queued-" + i));
            }

            final Thread coordinator = new Thread(
                    () -> executor.submitThrottled(batch, new MaxConcurrencyPolicy(1)), "burst");
            coordinator.start();

            assertThat(blocking.await(5, TimeUnit.SECONDS)).isTrue();
            Thread.sleep(300);              // everything behind the blocker is now genuinely waiting
            release.countDown();
            coordinator.join(10_000);
            executor.waitForCompletion();

            assertThat(recorder.events).hasSize(4);
            assertThat(recorder.events)
                    .filteredOn(event -> event.name().startsWith("queued-"))
                    .as("each queued task waited behind the blocker and must say so")
                    .hasSize(3)
                    .allSatisfy(event -> assertThat(event.waitedNanos())
                            .isGreaterThan(java.util.concurrent.TimeUnit.MILLISECONDS.toNanos(200)));
            executor.close();
        }
    }

    @Nested
    @DisplayName("the floor")
    class Floor {

        @Test
        @DisplayName("a task the policy refused but the liveness floor let through is marked as such")
        void floorAdmissionIsVisible() {

            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("Floor", ExecutorBackend.CALLER_RUNS);
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            // Refuses everything; only the floor can start anything.
            final AdmissionPolicy refuseAll = (candidate, snapshot) -> false;

            executor.submitThrottled(List.of(ThrottledTask.<Runnable>of(() -> { }, 10L, "A")), refuseAll);

            assertThat(recorder.forTask("A").floorAdmitted())
                    .as("the throttle did not permit this — the floor overrode it")
                    .isTrue();
            executor.close();
        }

        @Test
        @DisplayName("an exclusive admission is not marked as a floor admission, even behind a floor-held candidate")
        void exclusiveAdmissionIsNotFloor() {

            /*
             * The scan holds the first admissible ordinary candidate and keeps walking in case
             * an exclusive one is behind it. Here A is refused by the policy and held by the
             * floor — chosenByFloor = true — and then B, exclusive, wins on the idle pool. The
             * flag was never cleared when B replaced A, so B's admission was reported as a floor
             * admission: the "serial batch wearing a parallel batch's clothes" alarm that
             * executors.throttle.floorAdmitted, getFloorAdmittedTotal() and the JFR field all
             * key on, fired for the throttle working exactly as designed.
             */
            final ThrottledExecutor executor =
                    ThrottledExecutor.withBackend("FloorVsExclusive", ExecutorBackend.CALLER_RUNS);
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            final AdmissionPolicy refuseAllButIsolateB = new AdmissionPolicy() {
                @Override
                public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
                    return false;
                }

                @Override
                public boolean requiresExclusive(ThrottledTask<?> task) {
                    return "B".equals(task.name);
                }
            };

            // Largest first, so A is offered ahead of B on every scan.
            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> { }, 900L, "A"),
                    ThrottledTask.<Runnable>of(() -> { }, 100L, "B")),
                    refuseAllButIsolateB);

            assertThat(recorder.forTask("B").floorAdmitted())
                    .as("isolation is its own admission reason, not the floor's")
                    .isFalse();
            assertThat(recorder.forTask("A").floorAdmitted())
                    .as("A really was let through by the floor, on the next scan")
                    .isTrue();
            executor.close();
        }

        @Test
        @DisplayName("a task the policy actually admitted is not marked as a floor admission")
        void ordinaryAdmissionIsNotFloor() throws Exception {

            final ThrottledExecutor executor = new ThrottledExecutor("NotFloor");
            final Recorder recorder = new Recorder();
            executor.addListener(recorder);

            final CountDownLatch release = new CountDownLatch(1);
            final CountDownLatch occupied = new CountDownLatch(1);

            executor.submitThrottled(List.of(
                    ThrottledTask.<Runnable>of(() -> {
                        occupied.countDown();
                        try {
                            release.await(30, TimeUnit.SECONDS);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                        }
                    }, 900L, "first"),
                    ThrottledTask.<Runnable>of(() -> { }, 100L, "second")),
                    new MaxConcurrencyPolicy(4));

            assertThat(occupied.await(5, TimeUnit.SECONDS)).isTrue();
            release.countDown();
            executor.waitForCompletion();

            assertThat(recorder.forTask("second").floorAdmitted())
                    .as("something was running, so this was a genuine policy admission")
                    .isFalse();
            executor.close();
        }
    }

    private record Event(String name, long waitedNanos, boolean floorAdmitted) {
    }

    /** Records the throttle events and the lifecycle order around them. */
    private static final class Recorder implements TaskExecutionListener<Void> {

        final List<Event> events = new CopyOnWriteArrayList<>();
        final List<String> order = new CopyOnWriteArrayList<>();

        @Override
        public Void onSubmit(TaskDescriptor task) {
            order.add("submit:" + task.name());
            return null;
        }

        @Override
        public void onThrottled(TaskDescriptor task, long waitedNanos, boolean floorAdmitted) {
            order.add("throttled:" + task.name());
            events.add(new Event(task.name(), waitedNanos, floorAdmitted));
        }

        @Override
        public void onStart(TaskDescriptor task, Void state) {
            order.add("start:" + task.name());
        }

        @Override
        public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {
            order.add("complete:" + task.name());
        }

        Event forTask(String name) {
            return events.stream()
                    .filter(event -> name.equals(event.name()))
                    .findFirst()
                    .orElseThrow(() -> new AssertionError("no throttle event for " + name + " in " + events));
        }
    }
}
