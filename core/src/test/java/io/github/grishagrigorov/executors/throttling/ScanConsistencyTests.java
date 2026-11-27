package io.github.grishagrigorov.executors.throttling;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the consistency of a single candidate-scan pass in {@code submitThrottled}.
 *
 * <p>The scan offers each queued candidate to the policy in turn and takes the first one
 * admitted. All candidates in one pass are meant to be judged against <i>one</i> view of
 * the pool: {@link PoolSnapshot} documents itself as a consistent view, and the loop
 * captures a single {@code stateVersion} before scanning for exactly that reason.</p>
 *
 * <p>Taking a fresh snapshot per candidate breaks that — candidates in the same pass get
 * judged against different states — and allocates one snapshot per candidate per pass,
 * which on a large batch is a lot of garbage for no benefit.</p>
 */
class ScanConsistencyTests {

    /**
     * An {@link AdmissionPolicy} that never admits anything and records the exact
     * {@link PoolSnapshot} instance it was handed on every call.
     *
     * <p>Refusing everything means the executor falls back to its liveness floor, which
     * admits one task whenever nothing is running — so the batch still drains, one task
     * at a time, and every intermediate pass scans the remaining candidates.</p>
     *
     * <p>Note the policy is consulted even on a pass the floor will win, so that
     * {@code onThrottled} can report whether the throttle actually permitted the task. Such a
     * pass therefore contributes a single reading rather than a pair, which is why this test
     * looks for <em>some</em> adjacent pair rather than assuming the first two belong together.</p>
     */
    private static final class RecordingPolicy implements AdmissionPolicy {

        private final List<PoolSnapshot> snapshotsSeen = new ArrayList<>();

        @Override
        public synchronized boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
            snapshotsSeen.add(snapshot);
            return false;
        }

        synchronized List<PoolSnapshot> snapshotsSeen() {
            return new ArrayList<>(snapshotsSeen);
        }
    }

    @Test
    @DisplayName("all candidates in one scan pass are judged against the same snapshot")
    void allCandidatesInOneScanPassShouldSeeTheSameSnapshotInstance() {

        final ThrottledExecutor executor = new ThrottledExecutor("ScanConsistency");
        final RecordingPolicy policy = new RecordingPolicy();

        /*
         * Three equal-sized tasks. The first pass admits one via the liveness floor
         * (nothing running yet). Every later pass, while that task runs, offers the two
         * remaining candidates to the policy — so recorded snapshots arrive in pairs,
         * one pair per pass.
         */
        final List<ThrottledTask<Runnable>> jobs = List.of(
                new ThrottledTask<>(() -> sleep(400), 1_000L, "task-a"),
                new ThrottledTask<>(() -> sleep(400), 1_000L, "task-b"),
                new ThrottledTask<>(() -> sleep(400), 1_000L, "task-c"));

        executor.submitThrottled(jobs, policy);
        executor.waitForCompletion();

        final List<PoolSnapshot> seen = policy.snapshotsSeen();

        assertThat(seen)
                .as("the always-refusing policy must have been consulted repeatedly "
                        + "while the floor-admitted task was running")
                .hasSizeGreaterThanOrEqualTo(2);

        boolean somePassJudgedTwoCandidatesTogether = false;

        for (int i = 0; i + 1 < seen.size(); i++) {
            if (seen.get(i) == seen.get(i + 1)) {
                somePassJudgedTwoCandidatesTogether = true;
                break;
            }
        }

        assertThat(somePassJudgedTwoCandidatesTogether)
                .as("candidates scanned in the same pass must be judged against one "
                        + "consistent snapshot, not a freshly allocated one each: %s", seen)
                .isTrue();
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
