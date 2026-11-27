package io.github.grishagrigorov.executors.adaptive;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A {@link ProfileStore} that forgets everything when the JVM exits. Lock-free reads; each
 * {@link #record} is one {@link ConcurrentHashMap#compute} on the name's entry, so
 * concurrent records for different names never contend and records for the same name
 * never lose an update.
 */
public final class InMemoryProfileStore implements ProfileStore {

    private final int ringSize;
    private final ConcurrentHashMap<String, TaskProfile> profiles = new ConcurrentHashMap<>();

    /**
     * @param ringSize samples kept per task name, {@code >= 1}
     */
    public InMemoryProfileStore(int ringSize) {
        if (ringSize < 1) {
            throw new IllegalArgumentException("ringSize must be >= 1, got " + ringSize);
        }
        this.ringSize = ringSize;
    }

    @Override
    public TaskProfile find(String name) {
        return name == null ? null : profiles.get(name);
    }

    @Override
    public void record(String name, Sample sample) {
        Objects.requireNonNull(name, "name");
        Objects.requireNonNull(sample, "sample");
        profiles.compute(name, (n, profile) -> (profile == null ? TaskProfile.empty(n) : profile).append(sample, ringSize));
    }

    @Override
    public boolean forget(String name) {
        return name != null && profiles.remove(name) != null;
    }

    @Override
    public Map<String, TaskProfile> snapshot() {
        return Map.copyOf(profiles);
    }

    /** Nothing to persist to — a no-op. */
    @Override
    public void save() {
    }
}
