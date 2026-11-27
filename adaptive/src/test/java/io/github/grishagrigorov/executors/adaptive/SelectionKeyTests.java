package io.github.grishagrigorov.executors.adaptive;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What the adaptive policy offers first.
 *
 * <p>Largest-first packs the memory budget. Longest-first is the classic
 * longest-processing-time heuristic: on a fixed pool it shortens the makespan, because the task
 * that would otherwise be started last — and leave every other worker idle while it finishes —
 * is started first.</p>
 */
@DisplayName("selection order")
class SelectionKeyTests {

    private static final long NM = Sample.NOT_MEASURED;

    @Nested
    @DisplayName("by size (the default)")
    class BySize {

        @Test
        @DisplayName("largest learned size first, and it stays the default")
        void largestFirst() {

            assertThat(AdaptiveConfig.defaults().selectionKey()).isEqualTo(SelectionKey.SIZE);

            final AdaptiveSizing sizing = sizing(AdaptiveConfig.defaults());
            learn(sizing, "small", 1_000L, 10_000_000L);
            learn(sizing, "big", 1_000L, 40_000_000L);

            assertThat(orderOf(sizing, "small", "big")).containsExactly("big", "small");
        }
    }

    @Nested
    @DisplayName("by duration")
    class ByDuration {

        @Test
        @DisplayName("longest learned run first, regardless of size")
        void longestFirst() {

            final AdaptiveSizing sizing = sizing(AdaptiveConfig.defaults().withSelectionKey(SelectionKey.DURATION));

            // The big one is quick; the small one grinds. Size ordering would get this backwards.
            learnTimed(sizing, "big-quick", 1_000L, 40_000_000L, 1_000_000L);
            learnTimed(sizing, "small-slow", 1_000L, 10_000_000L, 90_000_000L);

            assertThat(orderOf(sizing, "big-quick", "small-slow"))
                    .containsExactly("small-slow", "big-quick");
        }

        @Test
        @DisplayName("an unknown still goes first — it has to run alone, so it should do so early")
        void unknownStillFirst() {

            final AdaptiveSizing sizing = sizing(AdaptiveConfig.defaults().withSelectionKey(SelectionKey.DURATION));
            learnTimed(sizing, "known", 1_000L, 10_000_000L, 90_000_000L);

            // Declared -1 and never profiled: nothing can be said about it, which is what
            // "unknown" means. A task that declares a size is not unknown, however new it is.
            final List<ThrottledTask<?>> tasks = new ArrayList<>(List.of(
                    task("known", 1_000L), task("never-seen", -1L)));
            tasks.sort(sizing.policy((candidate, snapshot) -> true).selectionOrder());

            assertThat(namesOf(tasks)).containsExactly("never-seen", "known");
        }

        @Test
        @DisplayName("a task learned before durations were recorded sorts last, not first")
        void untimedSortsLast() {

            final AdaptiveSizing sizing = sizing(AdaptiveConfig.defaults().withSelectionKey(SelectionKey.DURATION));
            learn(sizing, "untimed", 1_000L, 40_000_000L);              // memory only, no duration
            learnTimed(sizing, "timed", 1_000L, 10_000_000L, 5_000L);

            // "Unknown duration" must not be read as "infinitely long" — that would put every
            // profile carried over from an older file in front of everything actually measured.
            assertThat(orderOf(sizing, "untimed", "timed")).containsExactly("timed", "untimed");
        }
    }

    @Nested
    @DisplayName("configuration")
    class Configuration {

        @Test
        @DisplayName("the key is a component with a wither, and null is rejected")
        void witherAndValidation() {

            assertThat(AdaptiveConfig.defaults().withSelectionKey(SelectionKey.DURATION).selectionKey())
                    .isEqualTo(SelectionKey.DURATION);

            org.assertj.core.api.Assertions
                    .assertThatThrownBy(() -> AdaptiveConfig.defaults().withSelectionKey(null))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("the shorter constructors still work and mean SIZE")
        void compatibilityConstructors() {

            assertThat(new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 20, Duration.ZERO).selectionKey())
                    .isEqualTo(SelectionKey.SIZE);
            assertThat(new AdaptiveConfig(16, 1.25, Estimator.ALLOCATION, 1L << 20, Duration.ZERO, "v1", null)
                    .selectionKey()).isEqualTo(SelectionKey.SIZE);
        }
    }

    private static AdaptiveSizing sizing(AdaptiveConfig config) {
        return AdaptiveSizing.of(new InMemoryProfileStore(16), config);
    }

    private static void learn(AdaptiveSizing sizing, String name, long declared, long alloc) {
        sizing.store().record(name, new Sample(declared, alloc, NM, 1L));
    }

    private static void learnTimed(AdaptiveSizing sizing, String name, long declared, long alloc, long durationNanos) {
        sizing.store().record(name, new Sample(declared, alloc, NM, 1L, null, durationNanos));
    }

    private static ThrottledTask<?> task(String name, long declaredSize) {
        return ThrottledTask.<Runnable>of(() -> { }, declaredSize, name);
    }

    /** Sorts the named tasks through the policy's comparator and reports the resulting order. */
    private static List<String> orderOf(AdaptiveSizing sizing, String... names) {

        final List<ThrottledTask<?>> tasks = new ArrayList<>();
        for (String name : names) {
            tasks.add(task(name, 1_000L));
        }

        tasks.sort(sizing.policy((candidate, snapshot) -> true).selectionOrder());
        return namesOf(tasks);
    }

    private static List<String> namesOf(List<ThrottledTask<?>> tasks) {
        final List<String> order = new ArrayList<>();
        for (ThrottledTask<?> task : tasks) {
            order.add(task.name);
        }
        return order;
    }
}
