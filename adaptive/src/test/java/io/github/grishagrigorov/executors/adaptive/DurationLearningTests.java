package io.github.grishagrigorov.executors.adaptive;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Learning how long a task takes, alongside how much memory it needs.
 *
 * <p>The timestamps were always there — {@code TaskOutcome.durationNanos()} — so this costs the
 * listener nothing, and it is what makes longest-processing-time ordering possible.</p>
 */
@DisplayName("duration learning")
class DurationLearningTests {

    private static final long NM = Sample.NOT_MEASURED;

    @Nested
    @DisplayName("the sample")
    class TheSample {

        @Test
        @DisplayName("a duration is optional — the older constructors mean 'not measured'")
        void olderConstructorsMeanNotMeasured() {

            assertThat(new Sample(-1, 100, NM, 1L).durationNanos()).isEqualTo(NM);
            assertThat(new Sample(-1, 100, NM, 1L, "v1").durationNanos()).isEqualTo(NM);
            assertThat(new Sample(-1, 100, NM, 1L, "v1", 5_000L).durationNanos()).isEqualTo(5_000L);
        }

        @Test
        @DisplayName("a negative duration other than the sentinel is rejected")
        void rejectsNonsenseDurations() {

            org.assertj.core.api.Assertions
                    .assertThatThrownBy(() -> new Sample(-1, 100, NM, 1L, null, -2L))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Nested
    @DisplayName("the profile")
    class TheProfile {

        @Test
        @DisplayName("reports the longest run it has seen, and nothing when none was timed")
        void longestRun() {

            TaskProfile profile = TaskProfile.empty("Report")
                    .append(new Sample(-1, 100, NM, 1L, null, 5_000L), 16)
                    .append(new Sample(-1, 100, NM, 2L, null, 20_000L), 16)
                    .append(new Sample(-1, 100, NM, 3L, null, 9_000L), 16);

            assertThat(profile.duration()).hasValue(20_000L);

            assertThat(TaskProfile.empty("Untimed")
                    .append(new Sample(-1, 100, NM, 1L), 16)
                    .duration()).isEmpty();
        }

        @Test
        @DisplayName("the maximum, like the memory aggregates — a long run is the one that matters for scheduling")
        void usesMaximumNotAverage() {

            TaskProfile profile = TaskProfile.empty("Spiky")
                    .append(new Sample(-1, 100, NM, 1L, null, 1_000L), 16)
                    .append(new Sample(-1, 100, NM, 2L, null, 100_000L), 16);

            assertThat(profile.duration()).hasValue(100_000L);
        }
    }

    @Nested
    @DisplayName("the store")
    class TheStore {

        @Test
        @DisplayName("v3 round-trips the duration through the file")
        void v3RoundTrip(@TempDir Path dir) throws Exception {

            final Path file = dir.resolve("profiles.tsv");
            final FileProfileStore store = new FileProfileStore(file, 16);
            store.record("Report", new Sample(-1, 100, NM, 7L, "build-1", 42_000L));
            store.save();

            assertThat(Files.readAllLines(file).get(0)).isEqualTo(FileProfileStore.HEADER);

            final FileProfileStore reloaded = new FileProfileStore(file, 16);
            final Sample sample = reloaded.find("Report").samples().get(0);

            assertThat(sample.durationNanos()).isEqualTo(42_000L);
            assertThat(sample.version()).isEqualTo("build-1");
        }

        @Test
        @DisplayName("v1 and v2 files still load — their samples are simply untimed")
        void olderFilesStillLoad(@TempDir Path dir) throws Exception {

            final Path file = dir.resolve("old.tsv");
            Files.write(file, List.of(
                    "#executors-adaptive v1",
                    "one\t-1\t100\t-1\t1",                 // v1: five fields
                    "two\t-1\t200\t-1\t2\tbuild-9"));      // v2: six fields

            final FileProfileStore store = new FileProfileStore(file, 16);

            assertThat(store.find("one").samples().get(0).durationNanos()).isEqualTo(NM);
            assertThat(store.find("one").samples().get(0).version()).isNull();
            assertThat(store.find("two").samples().get(0).durationNanos()).isEqualTo(NM);
            assertThat(store.find("two").samples().get(0).version()).isEqualTo("build-9");
        }

        @Test
        @DisplayName("a file with the wrong number of columns is still skipped, not fatal")
        void malformedLinesAreSkipped(@TempDir Path dir) throws Exception {

            final Path file = dir.resolve("bad.tsv");
            Files.write(file, List.of(
                    FileProfileStore.HEADER,
                    "too\tfew",
                    "good\t-1\t100\t-1\t1\t\t7000"));

            final FileProfileStore store = new FileProfileStore(file, 16);

            assertThat(store.find("too")).isNull();
            assertThat(store.find("good").samples().get(0).durationNanos()).isEqualTo(7_000L);
        }
    }
}
