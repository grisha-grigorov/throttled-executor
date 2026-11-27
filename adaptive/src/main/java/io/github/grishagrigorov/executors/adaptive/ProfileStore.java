package io.github.grishagrigorov.executors.adaptive;

import java.io.IOException;
import java.util.Map;

/**
 * Where learned {@link TaskProfile}s live. Implementations must be thread-safe;
 * {@link #find} and {@link #record} are on the hot path (the submitting thread under the
 * harness lock, and the worker thread inside {@code onComplete} respectively) and must be
 * cheap and non-blocking — no I/O there.
 */
public interface ProfileStore {

    /**
     * @param name the task name
     * @return the profile, or {@code null} when nothing has been learned about the name
     */
    TaskProfile find(String name);

    /**
     * Appends a sample to the name's profile, creating the profile on first sight.
     *
     * @param name   the task name
     * @param sample the measured run
     */
    void record(String name, Sample sample);

    /**
     * Drops everything learned about a name, so that its next run is treated as the first —
     * the manual lever for "this part of the application changed, measure it again" (the
     * automatic ones are {@link AdaptiveConfig#version()} and
     * {@link AdaptiveConfig#maxSampleAge()}). Optional operation: both shipped stores
     * support it.
     *
     * @param name the task name; {@code null} is nothing
     * @return whether there was a profile to drop
     * @throws UnsupportedOperationException when the store cannot forget
     */
    default boolean forget(String name) {
        throw new UnsupportedOperationException(getClass().getName() + " cannot forget");
    }

    /** @return an immutable copy of every profile, keyed by task name */
    Map<String, TaskProfile> snapshot();

    /**
     * Persists the profiles, where the implementation has somewhere to persist to.
     *
     * @throws IOException when persisting fails
     */
    void save() throws IOException;
}
