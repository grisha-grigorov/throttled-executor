package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.FileTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * The two {@link ProfileStore}s. The in-memory one is the data structure; the file one is
 * the same structure with a TSV behind it — loaded once on construction, written by
 * {@code save()} atomically and only when something changed, never touched by
 * {@code record()} (that runs on the worker inside {@code onComplete}).
 */
class ProfileStoreTests {

    private static final long NM = Sample.NOT_MEASURED;

    private static Sample sample(long declared, long alloc, long heap, long at) {
        return new Sample(declared, alloc, heap, at);
    }

    private static List<Sample> samplesOf(ProfileStore store, String name) {
        final TaskProfile profile = store.find(name);
        return profile == null ? List.of() : profile.samples();
    }

    /**
     * 8 threads x 1000 records over 4 names with unique allocation figures: every record
     * must land, in some order, and the ring must hold.
     */
    private static void hammer(ProfileStore store, int expectedPerName) throws Exception {
        final int threads = 8;
        final int perThread = 1_000;
        final int names = 4;
        final ExecutorService pool = Executors.newFixedThreadPool(threads);
        final CountDownLatch go = new CountDownLatch(1);
        final List<Future<?>> futures = new ArrayList<>();
        try {
            for (int t = 0; t < threads; t++) {
                final int thread = t;
                futures.add(pool.submit(() -> {
                    go.await();
                    for (int i = 0; i < perThread; i++) {
                        final long unique = (long) thread * perThread + i;
                        store.record("n-" + (i % names), sample(-1, unique, NM, unique));
                    }
                    return null;
                }));
            }
            go.countDown();
            for (Future<?> f : futures) {
                f.get(30, TimeUnit.SECONDS);
            }
        } finally {
            pool.shutdownNow();
        }
        for (int n = 0; n < names; n++) {
            final List<Sample> samples = samplesOf(store, "n-" + n);
            assertThat(samples).as("n-%d", n).hasSize(expectedPerName);
            final Set<Long> distinct = new HashSet<>();
            samples.forEach(s -> distinct.add(s.allocatedBytes()));
            assertThat(distinct).as("no sample recorded twice for n-%d", n).hasSize(samples.size());
        }
    }

    @Nested
    @DisplayName("InMemoryProfileStore")
    class InMemory {

        @Test
        @DisplayName("finds nothing for an unseen or null name")
        void findsNothingForUnseenOrNull() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);

            assertThat(store.find("unseen")).isNull();
            assertThat(store.find(null)).isNull();
            assertThat(store.snapshot()).isEmpty();
        }

        @Test
        @DisplayName("record appends in order and the ring drops the oldest")
        void recordAppendsAndRingDrops() {
            final InMemoryProfileStore store = new InMemoryProfileStore(3);
            for (long i = 1; i <= 5; i++) {
                store.record("job", sample(-1, i, NM, i));
            }

            assertThat(samplesOf(store, "job")).extracting(Sample::allocatedBytes).containsExactly(3L, 4L, 5L);
            assertThat(store.find("job").name()).isEqualTo("job");
        }

        @Test
        @DisplayName("snapshot is an unmodifiable copy taken at call time")
        void snapshotIsACopy() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            store.record("a", sample(-1, 1, NM, 1));
            final Map<String, TaskProfile> snapshot = store.snapshot();
            store.record("b", sample(-1, 2, NM, 2));

            assertThat(snapshot).containsOnlyKeys("a");
            assertThat(store.snapshot()).containsOnlyKeys("a", "b");
            assertThatThrownBy(() -> snapshot.put("c", TaskProfile.empty("c"))).isInstanceOf(UnsupportedOperationException.class);
        }

        @Test
        @DisplayName("8 threads x 1000 records: nothing lost, nothing duplicated")
        void concurrentRecordsLoseNothing() throws Exception {
            hammer(new InMemoryProfileStore(10_000), 2_000);
        }

        @Test
        @DisplayName("8 threads x 1000 records under a small ring: exactly ringSize kept per name")
        void concurrentRecordsRespectTheRing() throws Exception {
            hammer(new InMemoryProfileStore(16), 16);
        }

        @Test
        @DisplayName("rejects a ring smaller than one, a null name and a null sample")
        void rejectsBadArguments() {
            assertThatThrownBy(() -> new InMemoryProfileStore(0)).isInstanceOf(IllegalArgumentException.class);
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            assertThatThrownBy(() -> store.record(null, sample(-1, 1, NM, 1))).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> store.record("job", null)).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("forget() drops the name's profile and says whether there was one; null is nothing")
        void forgetDropsTheProfile() {
            final InMemoryProfileStore store = new InMemoryProfileStore(16);
            store.record("job", sample(-1, 1, NM, 1));
            store.record("other", sample(-1, 2, NM, 2));

            assertThat(store.forget("job")).isTrue();
            assertThat(store.find("job")).isNull();
            assertThat(store.snapshot()).containsOnlyKeys("other");
            assertThat(store.forget("job")).as("already gone").isFalse();
            assertThat(store.forget("never")).isFalse();
            assertThat(store.forget(null)).isFalse();
        }

        @Test
        @DisplayName("save is a no-op")
        void saveIsANoOp() {
            assertThatCode(() -> new InMemoryProfileStore(16).save()).doesNotThrowAnyException();
        }
    }

    @Nested
    @DisplayName("FileProfileStore")
    class OnFile {

        @TempDir
        Path dir;

        private Path file() {
            return dir.resolve("profiles.tsv");
        }

        @Test
        @DisplayName("a missing file is an empty store, and stays missing until save()")
        void missingFileIsEmpty() {
            final FileProfileStore store = new FileProfileStore(file(), 16);

            assertThat(store.find("job")).isNull();
            assertThat(store.snapshot()).isEmpty();
            assertThat(file()).doesNotExist();
        }

        @Test
        @DisplayName("record() performs no I/O — the file appears only on save()")
        void recordDoesNoIo() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("job", sample(-1, 1, NM, 1));

            assertThat(file()).doesNotExist();
            store.save();
            assertThat(file()).exists();
        }

        @Test
        @DisplayName("round-trips every sample, in order, through a fresh instance")
        void roundTripsThroughAFreshInstance() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("read", sample(1_000, 2_000, NM, 10));
            store.record("read", sample(1_000, NM, 3_000, 11));
            store.record("read", sample(-1, 0, 0, 12));
            store.record("report", sample(0, 5, 6, 13));
            store.record("report", sample(Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE, Long.MAX_VALUE));
            store.save();

            final FileProfileStore reloaded = new FileProfileStore(file(), 16);

            assertThat(reloaded.snapshot()).containsOnlyKeys("read", "report");
            assertThat(samplesOf(reloaded, "read")).containsExactlyElementsOf(samplesOf(store, "read"));
            assertThat(samplesOf(reloaded, "report")).containsExactlyElementsOf(samplesOf(store, "report"));
        }

        @Test
        @DisplayName("the version survives the round trip — escaped like the name, empty for none")
        void versionRoundTrips() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("job", new Sample(-1, 1, NM, 1, "2026.08.22"));
            store.record("job", new Sample(-1, 2, NM, 2, null));
            store.record("job", new Sample(-1, 3, NM, 3, "tab\tin\\version #1"));
            store.save();

            final FileProfileStore reloaded = new FileProfileStore(file(), 16);

            assertThat(samplesOf(reloaded, "job")).containsExactlyElementsOf(samplesOf(store, "job"));
            assertThat(samplesOf(reloaded, "job")).extracting(Sample::version).containsExactly("2026.08.22", null, "tab\tin\\version #1");
        }

        @Test
        @DisplayName("a v1 file (five columns) loads as unversioned samples and is rewritten as v3")
        void v1FileLoadsUnversioned() throws IOException {
            Files.write(file(), List.of(
                    "#executors-adaptive v1",
                    "job\t-1\t100\t-1\t1",
                    "job\t10\t-1\t20\t2"), StandardCharsets.UTF_8);

            final FileProfileStore store = new FileProfileStore(file(), 16);

            assertThat(samplesOf(store, "job")).containsExactly(sample(-1, 100, NM, 1), sample(10, NM, 20, 2));
            assertThat(samplesOf(store, "job")).extracting(Sample::version).containsOnlyNulls();
            store.record("job", new Sample(-1, 5, NM, 3, "v"));
            store.save();
            final List<String> lines = Files.readAllLines(file(), StandardCharsets.UTF_8);
            assertThat(lines.get(0)).isEqualTo(FileProfileStore.HEADER);
            assertThat(lines).allSatisfy(line -> {
                if (!line.startsWith("#")) {
                    assertThat(line.split("\t", -1)).as("'%s'", line).hasSize(7);
                }
            });
        }

        @Test
        @DisplayName("malformed lines are skipped too: a bad escape in the version, a non-numeric duration, eight columns")
        void malformedV2LinesAreSkipped() throws IOException {
            Files.write(file(), List.of(
                    "#executors-adaptive v2",
                    "good\t-1\t100\t-1\t1\tv1",
                    "bad-escape\t-1\t100\t-1\t1\tv\\q",
                    "bad-duration\t-1\t100\t-1\t1\tv1\tsoon",
                    "eight\t-1\t100\t-1\t1\tv1\t500\textra",
                    "also-good\t-1\t100\t-1\t1\t"), StandardCharsets.UTF_8);

            final FileProfileStore store = new FileProfileStore(file(), 16);

            assertThat(store.snapshot()).containsOnlyKeys("good", "also-good");
            assertThat(samplesOf(store, "good").get(0).version()).isEqualTo("v1");
            assertThat(samplesOf(store, "also-good").get(0).version()).isNull();
        }

        @Test
        @DisplayName("forget() is a change: the next save() rewrites the file without the name")
        void forgetDirtiesTheStore() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("job", sample(-1, 1, NM, 1));
            store.record("keep", sample(-1, 2, NM, 2));
            store.save();
            final FileTime longAgo = FileTime.from(Instant.parse("2000-01-01T00:00:00Z"));
            Files.setLastModifiedTime(file(), longAgo);

            assertThat(store.forget("missing")).isFalse();
            store.save();
            assertThat(Files.getLastModifiedTime(file())).as("forgetting nothing changes nothing").isEqualTo(longAgo);

            assertThat(store.forget("job")).isTrue();
            store.save();
            assertThat(Files.getLastModifiedTime(file())).as("rewritten").isGreaterThan(longAgo);
            assertThat(new FileProfileStore(file(), 16).snapshot()).containsOnlyKeys("keep");
        }

        @Test
        @DisplayName("a smaller ring on reload keeps the newest samples")
        void smallerRingOnReloadKeepsNewest() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            for (long i = 1; i <= 10; i++) {
                store.record("job", sample(-1, i, NM, i));
            }
            store.save();

            final FileProfileStore reloaded = new FileProfileStore(file(), 4);

            assertThat(samplesOf(reloaded, "job")).extracting(Sample::allocatedBytes).containsExactly(7L, 8L, 9L, 10L);
        }

        @Test
        @DisplayName("names with tabs, newlines, carriage returns, backslashes and unicode survive the TSV")
        void hostileNamesSurvive() throws IOException {
            final List<String> names = List.of(
                    "tab\there", "new\nline", "cr\rlf\r\n", "back\\slash", "trailing\\", "\\t-literal",
                    "üñí©ødé 🚢 ⚓", " leading and trailing ", "#not-a-comment", "");
            final FileProfileStore store = new FileProfileStore(file(), 16);
            long i = 0;
            for (String name : names) {
                store.record(name, sample(-1, ++i, NM, i));
            }
            store.save();

            final FileProfileStore reloaded = new FileProfileStore(file(), 16);

            assertThat(reloaded.snapshot().keySet()).containsExactlyInAnyOrderElementsOf(names);
            for (String name : names) {
                assertThat(samplesOf(reloaded, name)).as("'%s'", name).containsExactlyElementsOf(samplesOf(store, name));
            }
        }

        @Test
        @DisplayName("malformed lines are skipped, the good ones around them load")
        void malformedLinesAreSkipped() throws IOException {
            Files.write(file(), List.of(
                    "#executors-adaptive v1",
                    "good\t-1\t100\t-1\t1",
                    "garbage",
                    "too\tfew\tfields",
                    "not-numeric\tx\t100\t-1\t1",
                    "invalid-sample\t-1\t-1\t-1\t1",
                    "below-minus-one\t-5\t100\t-1\t1",
                    "",
                    "# a comment line",
                    "also-good\t10\t-1\t20\t2"), StandardCharsets.UTF_8);

            final FileProfileStore store = new FileProfileStore(file(), 16);

            assertThat(store.snapshot()).containsOnlyKeys("good", "also-good");
            assertThat(samplesOf(store, "good")).containsExactly(sample(-1, 100, NM, 1));
            assertThat(samplesOf(store, "also-good")).containsExactly(sample(10, NM, 20, 2));
        }

        @Test
        @DisplayName("save() writes the v3 header, creates parent directories, and leaves no temp file behind")
        void saveWritesHeaderCreatesParentsLeavesNoTemp() throws IOException {
            final Path nested = dir.resolve("nested").resolve("deeper").resolve("profiles.tsv");
            final FileProfileStore store = new FileProfileStore(nested, 16);
            store.record("job", sample(-1, 1, NM, 1));
            store.save();

            final List<String> lines = Files.readAllLines(nested, StandardCharsets.UTF_8);
            assertThat(lines.get(0)).isEqualTo("#executors-adaptive v3").isEqualTo(FileProfileStore.HEADER);
            assertThat(lines).hasSize(2);
            assertThat(lines.get(1)).as("seven columns; an unversioned, untimed sample writes an empty version and -1").isEqualTo("job\t-1\t1\t-1\t1\t\t-1");
            try (Stream<Path> siblings = Files.list(nested.getParent())) {
                assertThat(siblings).containsExactly(nested);
            }
        }

        @Test
        @DisplayName("save() rewrites the file only when something was recorded since the last save")
        void saveOnlyWhenDirty() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("job", sample(-1, 1, NM, 1));
            store.save();
            final FileTime longAgo = FileTime.from(Instant.parse("2000-01-01T00:00:00Z"));
            Files.setLastModifiedTime(file(), longAgo);

            store.save();
            assertThat(Files.getLastModifiedTime(file())).as("clean store: untouched").isEqualTo(longAgo);

            store.record("job", sample(-1, 2, NM, 2));
            store.save();
            assertThat(Files.getLastModifiedTime(file())).as("dirty store: rewritten").isGreaterThan(longAgo);
            assertThat(samplesOf(new FileProfileStore(file(), 16), "job")).hasSize(2);
        }

        @Test
        @DisplayName("a sample the profile discards does not dirty the store")
        void discardedSampleDoesNotDirty() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 1);
            store.record("job", new Sample(-1, 1, NM, 1L, null, 5L));
            store.save();
            final FileTime longAgo = FileTime.from(Instant.parse("2000-01-01T00:00:00Z"));
            Files.setLastModifiedTime(file(), longAgo);

            // Ring of one, holding a memory sample: a timing has nothing it may evict.
            store.record("job", new Sample(-1, NM, NM, 2L, null, 99L));
            store.save();

            assertThat(Files.getLastModifiedTime(file())).as("nothing changed, nothing rewritten").isEqualTo(longAgo);
        }

        @Test
        @DisplayName("a freshly loaded store is clean: save() without records does not rewrite")
        void loadedStoreIsClean() throws IOException {
            final FileProfileStore store = new FileProfileStore(file(), 16);
            store.record("job", sample(-1, 1, NM, 1));
            store.save();
            final FileTime longAgo = FileTime.from(Instant.parse("2000-01-01T00:00:00Z"));
            Files.setLastModifiedTime(file(), longAgo);

            new FileProfileStore(file(), 16).save();

            assertThat(Files.getLastModifiedTime(file())).isEqualTo(longAgo);
        }

        @Test
        @DisplayName("8 threads x 1000 records, then save and reload: nothing lost")
        void concurrentRecordsSurviveSaveAndReload() throws Exception {
            final FileProfileStore store = new FileProfileStore(file(), 10_000);
            hammer(store, 2_000);
            store.save();

            hammerVerifyOnly(new FileProfileStore(file(), 10_000));
        }

        private void hammerVerifyOnly(ProfileStore reloaded) {
            for (int n = 0; n < 4; n++) {
                assertThat(samplesOf(reloaded, "n-" + n)).hasSize(2_000);
            }
        }

        @Test
        @DisplayName("rejects a null file and a ring smaller than one")
        void rejectsBadArguments() {
            assertThatThrownBy(() -> new FileProfileStore(null, 16)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> new FileProfileStore(file(), 0)).isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("the file path is exposed")
        void exposesThePath() {
            assertThat(new FileProfileStore(file(), 16).file()).isEqualTo(file());
        }
    }

    @Nested
    @DisplayName("AdaptiveSizing.onFile")
    class Facade {

        @TempDir
        Path dir;

        @Test
        @DisplayName("learning saved through the facade is visible to a fresh facade without running anything")
        void learningSurvivesAFreshFacade() throws IOException {
            final Path file = dir.resolve("profiles.tsv");
            final AdaptiveSizing first = AdaptiveSizing.onFile(file);
            assertThat(first.store()).isInstanceOf(FileProfileStore.class);
            assertThat(first.config()).isEqualTo(AdaptiveConfig.defaults());
            first.store().record("job", sample(-1, 800, NM, 1));
            assertThat(first.effectiveSize("job", -1)).isEqualTo(1_000);
            first.save();

            final AdaptiveSizing second = AdaptiveSizing.onFile(file);

            assertThat(second.effectiveSize("job", -1)).isEqualTo(1_000);
            assertThat(second.effectiveSize("never-seen", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
        }

        @Test
        @DisplayName("forget() through the facade: the name is unknown again, and that survives a save")
        void forgetThroughTheFacade() throws IOException {
            final Path file = dir.resolve("profiles.tsv");
            final AdaptiveSizing first = AdaptiveSizing.onFile(file);
            first.store().record("job", sample(-1, 800, NM, 1));
            first.save();

            assertThat(first.forget("job")).isTrue();
            assertThat(first.effectiveSize("job", -1)).isEqualTo(AdaptiveSizing.UNKNOWN);
            assertThat(first.forget("job")).isFalse();
            first.save();

            assertThat(AdaptiveSizing.onFile(file).profile("job")).isNull();
        }

        @Test
        @DisplayName("onFile(file, config) uses that config and its ring size")
        void onFileWithConfig() {
            final Path file = dir.resolve("profiles.tsv");
            final AdaptiveConfig config = AdaptiveConfig.defaults().withVersion("b1");

            final AdaptiveSizing sizing = AdaptiveSizing.onFile(file, config);

            assertThat(sizing.config()).isSameAs(config);
            assertThat(sizing.store()).isInstanceOf(FileProfileStore.class);
            assertThatThrownBy(() -> AdaptiveSizing.onFile(file, null)).isInstanceOf(NullPointerException.class);
            assertThatThrownBy(() -> AdaptiveSizing.onFile(null, config)).isInstanceOf(NullPointerException.class);
        }

        @Test
        @DisplayName("onFile rejects a null path")
        void rejectsNullPath() {
            assertThatThrownBy(() -> AdaptiveSizing.onFile(null)).isInstanceOf(NullPointerException.class);
        }
    }
}
