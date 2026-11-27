package io.github.grishagrigorov.executors;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * The task the throttling API is written in terms of: a job, what it is expected to cost, an
 * optional name the sizing can learn against, and optional tags a policy can meter on.
 */
@DisplayName("ThrottledTask")
class ThrottledTaskTests {

    private static final Runnable JOB = () -> { };

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("the three-argument form is untagged, and tags are never null")
        void threeArgFormIsUntagged() {

            final ThrottledTask<Runnable> task = new ThrottledTask<>(JOB, 1024L, "Report");

            assertThat(task.job).isSameAs(JOB);
            assertThat(task.size).isEqualTo(1024L);
            assertThat(task.name).isEqualTo("Report");
            assertThat(task.tags).isEmpty();
        }

        @Test
        @DisplayName("a null tag set means untagged, not a null field")
        void nullTagsMeanUntagged() {

            assertThat(new ThrottledTask<>(JOB, 1L, "A", null).tags).isEmpty();
        }

        @Test
        @DisplayName("tags are copied and immutable — a later change to the caller's set cannot reach the task")
        void tagsAreCopiedAndImmutable() {

            final Set<String> mutable = new HashSet<>(Set.of("hsqldb"));
            final ThrottledTask<Runnable> task = new ThrottledTask<>(JOB, 1L, "A", mutable);

            mutable.add("spmbb");

            assertThat(task.tags).containsExactly("hsqldb");
            assertThatThrownBy(() -> task.tags.add("late")).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("a null tag inside the set is rejected — a policy could never meter on it")
        void nullTagIsRejected() {

            final Set<String> withNull = new HashSet<>();
            withNull.add(null);

            assertThatThrownBy(() -> new ThrottledTask<>(JOB, 1L, "A", withNull))
                    .isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("the factories cover the three shapes; an unnamed task is allowed")
        void factories() {

            assertThat(ThrottledTask.of(JOB, 8L).name).isNull();
            assertThat(ThrottledTask.of(JOB, 8L).tags).isEmpty();
            assertThat(ThrottledTask.of(JOB, 8L, "A").name).isEqualTo("A");
            assertThat(ThrottledTask.of(JOB, 8L, "A", Set.of("db")).tags).containsExactly("db");
        }
    }

    @Nested
    @DisplayName("identity")
    class Identity {

        @Test
        @DisplayName("two tasks describing the same work are still two distinct tasks")
        void identityNotValueSemantics() {

            // Load-bearing: the adaptive decorator keys an IdentityHashMap on task instances,
            // one ledger entry per claim, so equal-looking tasks must never collapse into one.
            final ThrottledTask<Runnable> a = ThrottledTask.of(JOB, 8L, "A");
            final ThrottledTask<Runnable> b = ThrottledTask.of(JOB, 8L, "A");

            assertThat(a).isNotSameAs(b);
            assertThat(a).isNotEqualTo(b);
        }
    }

    @Nested
    @DisplayName("hasTag")
    class HasTag {

        @Test
        @DisplayName("answers for the tags it carries, and false for anything else")
        void hasTag() {

            final ThrottledTask<Runnable> task = ThrottledTask.of(JOB, 8L, "A", Set.of("hsqldb", "slow"));

            assertThat(task.hasTag("hsqldb")).isTrue();
            assertThat(task.hasTag("slow")).isTrue();
            assertThat(task.hasTag("spmbb")).isFalse();
            assertThat(ThrottledTask.of(JOB, 8L).hasTag("anything")).isFalse();
        }
    }

    @Nested
    @DisplayName("toString")
    class ToStringFormat {

        @Test
        @DisplayName("sub-megabyte sizes report in KB, so they do not all truncate to 0MB")
        void subMegabyteReportsKb() {

            assertThat(ThrottledTask.of(JOB, 512L * 1024, "A").toString()).contains("512KB");
            assertThat(ThrottledTask.of(JOB, 4L * 1024 * 1024, "A").toString()).contains("4MB");
        }

        @Test
        @DisplayName("the -1 sentinel reads as 'unsized', not as 0KB")
        void noEstimateIsNotZero() {

            // This string is what appears in submitThrottled's IllegalArgumentException and in
            // policy-hook error logs, where "declared no estimate" and "declared nothing needed"
            // are meaningfully different diagnoses.
            assertThat(ThrottledTask.of(JOB, -1L, "A").toString())
                    .contains("unsized")
                    .doesNotContain("0KB");
        }

        @Test
        @DisplayName("tags show only when there are some")
        void tagsShowOnlyWhenPresent() {

            assertThat(ThrottledTask.of(JOB, 1L, "A").toString()).doesNotContain("Tags");
            assertThat(ThrottledTask.of(JOB, 1L, "A", Set.of("db")).toString()).contains("db");
        }
    }
}
