package io.github.grishagrigorov.executors.adaptive;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link ProfileStore} backed by a TSV file: loaded once on construction, held in memory
 * exactly like {@link InMemoryProfileStore}, written back by {@link #save()} — atomically
 * (temp file + move) and only when something was recorded since the last save.
 *
 * <p>{@link #record} never touches the disk: it runs on the worker thread inside the
 * listener's {@code onComplete}, where a syscall would break core's fast-hooks contract.
 * Call {@link #save()} (or {@link AdaptiveSizing#close()}) at a quiet moment.</p>
 *
 * <h2>Format ({@value #HEADER})</h2>
 * <pre>
 * #executors-adaptive v3
 * name&lt;TAB&gt;declaredSize&lt;TAB&gt;allocatedBytes&lt;TAB&gt;soloHeapPeakBytes&lt;TAB&gt;epochMillis&lt;TAB&gt;version&lt;TAB&gt;durationNanos
 * </pre>
 * One line per sample, a name's samples oldest first, names sorted. In the name and the
 * version, a backslash, tab, LF and CR are escaped as {@code \\ \t \n \r}, and a leading
 * {@code #} as {@code \#} so it cannot be mistaken for a comment line; an empty version
 * column is "unversioned", and {@code -1} in a measurement column is "not measured". The
 * column count decides the version on load, never the header: five columns is v1
 * (unversioned, untimed), six is v2 (untimed), seven is v3. Older files load, and are
 * rewritten as v3 on the next save. Lines that are empty or start with {@code #} are
 * ignored; a malformed line is logged and skipped, never fatal — losing one learned sample
 * is cheaper than refusing to start.
 *
 * <h2>Footprint</h2>
 * <p>Every completed run of a named task records a sample (a duration-only one when its
 * memory could not be measured), so the store holds up to {@code ringSize} samples per
 * distinct name and puts no cap on names; the file is rewritten in full on a dirty save and
 * loaded in full on construction. Name the kind of work, not the instance — an unbounded
 * name space is an unbounded file.</p>
 */
public final class FileProfileStore implements ProfileStore {

    private static final Logger LOG = LoggerFactory.getLogger(FileProfileStore.class);

    /** First line of every file this store writes. */
    public static final String HEADER = "#executors-adaptive v3";

    private static final int V1_FIELDS = 5;
    private static final int V2_FIELDS = 6;
    private static final int V3_FIELDS = 7;

    private final Path file;
    private final int ringSize;
    private final ConcurrentHashMap<String, TaskProfile> profiles = new ConcurrentHashMap<>();
    /** Bumped after every record; compared with {@link #savedVersion} to skip clean saves. */
    private final AtomicLong version = new AtomicLong();
    private final Object saveLock = new Object();
    private long savedVersion;                       // guarded by saveLock

    /**
     * Loads {@code file} if it exists; a missing file is an empty store.
     *
     * @param file     the profile file; need not exist yet
     * @param ringSize samples kept per task name, {@code >= 1}; a file holding more per
     *                 name keeps the newest
     */
    public FileProfileStore(Path file, int ringSize) {
        this.file = Objects.requireNonNull(file, "file");
        if (ringSize < 1) {
            throw new IllegalArgumentException("ringSize must be >= 1, got " + ringSize);
        }
        this.ringSize = ringSize;
        load();
    }

    /** @return the profile file */
    public Path file() {
        return file;
    }

    @Override
    public TaskProfile find(String name) {
        return name == null ? null : profiles.get(name);
    }

    @Override
    public void record(String name, Sample sample) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(sample, "sample");
        if (append(name, sample)) {
            version.incrementAndGet();       // after the append, so a save that saw this version saw the sample
        }
    }

    /** @return whether the profile changed — a sample the ring cannot place leaves it as it was */
    private boolean append(String name, Sample sample) {
        final TaskProfile[] before = new TaskProfile[1];
        final TaskProfile after = profiles.compute(name, (n, profile) -> {
            before[0] = profile;
            return (profile == null ? TaskProfile.empty(n) : profile).append(sample, ringSize);
        });
        return after != before[0];
    }

    @Override
    public boolean forget(String name) {
        if (name == null || profiles.remove(name) == null) {
            return false;
        }
        version.incrementAndGet();           // a dropped profile is a change the next save must write
        return true;
    }

    @Override
    public Map<String, TaskProfile> snapshot() {
        return Map.copyOf(profiles);
    }

    /**
     * Writes every profile to the file, atomically, unless nothing was recorded since the
     * last save (or since loading). Creates missing parent directories.
     *
     * @throws IOException when the file cannot be written or moved into place
     */
    @Override
    public void save() throws IOException {
        synchronized (saveLock) {
            final long current = version.get();
            if (current == savedVersion) {
                return;
            }
            final Path parent = file.toAbsolutePath().getParent();
            if (parent != null) {
                Files.createDirectories(parent);
            }
            final Path tmp = file.resolveSibling(file.getFileName() + ".tmp");
            try {
                try (BufferedWriter out = Files.newBufferedWriter(tmp, StandardCharsets.UTF_8)) {
                    out.write(HEADER);
                    out.newLine();
                    final List<String> names = new ArrayList<>(profiles.keySet());
                    Collections.sort(names);
                    for (String name : names) {
                        final TaskProfile profile = profiles.get(name);
                        if (profile == null) {
                            continue;
                        }
                        final String escaped = escape(name);
                        for (Sample s : profile.samples()) {
                            out.write(escaped);
                            out.write('\t');
                            out.write(Long.toString(s.declaredSize()));
                            out.write('\t');
                            out.write(Long.toString(s.allocatedBytes()));
                            out.write('\t');
                            out.write(Long.toString(s.soloHeapPeakBytes()));
                            out.write('\t');
                            out.write(Long.toString(s.epochMillis()));
                            out.write('\t');
                            if (s.version() != null) {
                                out.write(escape(s.version()));
                            }
                            out.write('\t');
                            out.write(Long.toString(s.durationNanos()));
                            out.newLine();
                        }
                    }
                }
                try {
                    Files.move(tmp, file, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
                } catch (AtomicMoveNotSupportedException e) {
                    Files.move(tmp, file, StandardCopyOption.REPLACE_EXISTING);
                }
            } finally {
                Files.deleteIfExists(tmp);
            }
            savedVersion = current;
        }
    }

    private void load() {
        if (!Files.isRegularFile(file)) {
            return;
        }
        final List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            LOG.warn("executors-adaptive: cannot read {} — starting with no learned profiles", file, e);
            return;
        }
        int lineNumber = 0;
        int skipped = 0;
        for (String line : lines) {
            lineNumber++;
            if (line.isEmpty() || line.charAt(0) == '#') {
                continue;
            }
            try {
                final String[] fields = line.split("\t", -1);
                if (fields.length < V1_FIELDS || fields.length > V3_FIELDS) {
                    throw new IllegalArgumentException("expected " + V1_FIELDS + " to " + V3_FIELDS
                            + " tab-separated fields, found " + fields.length);
                }
                // Width tells the version: 5 = v1 (unversioned, untimed), 6 = v2 (untimed),
                // 7 = v3. The header line is a comment and is never parsed, so an older file
                // loads on its shape rather than on trusting what it says about itself.
                final String version = fields.length >= V2_FIELDS && !fields[5].isEmpty() ? unescape(fields[5]) : null;
                final long durationNanos = fields.length == V3_FIELDS
                        ? Long.parseLong(fields[6]) : Sample.NOT_MEASURED;
                append(unescape(fields[0]), new Sample(
                        Long.parseLong(fields[1]), Long.parseLong(fields[2]), Long.parseLong(fields[3]),
                        Long.parseLong(fields[4]), version, durationNanos));
            } catch (RuntimeException e) {
                skipped++;
                LOG.warn("executors-adaptive: {}:{} skipped — {}", file, lineNumber, e.getMessage());
            }
        }
        if (skipped > 0) {
            LOG.warn("executors-adaptive: {} malformed line(s) skipped in {}", skipped, file);
        }
    }

    static String escape(String name) {
        final StringBuilder sb = new StringBuilder(name.length() + 8);
        for (int i = 0; i < name.length(); i++) {
            final char c = name.charAt(i);
            switch (c) {
                case '\\': sb.append("\\\\"); break;
                case '\t': sb.append("\\t"); break;
                case '\n': sb.append("\\n"); break;
                case '\r': sb.append("\\r"); break;
                case '#':
                    if (i == 0) {
                        sb.append("\\#");
                    } else {
                        sb.append(c);
                    }
                    break;
                default: sb.append(c);
            }
        }
        return sb.toString();
    }

    static String unescape(String escaped) {
        if (escaped.indexOf('\\') < 0) {
            return escaped;
        }
        final StringBuilder sb = new StringBuilder(escaped.length());
        for (int i = 0; i < escaped.length(); i++) {
            final char c = escaped.charAt(i);
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (++i >= escaped.length()) {
                throw new IllegalArgumentException("dangling escape at end of name");
            }
            final char e = escaped.charAt(i);
            switch (e) {
                case '\\': sb.append('\\'); break;
                case 't': sb.append('\t'); break;
                case 'n': sb.append('\n'); break;
                case 'r': sb.append('\r'); break;
                case '#': sb.append('#'); break;
                default: throw new IllegalArgumentException("unknown escape \\" + e + " in name");
            }
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "FileProfileStore[" + file + ", profiles=" + profiles.size() + "]";
    }
}
