package io.github.grishagrigorov.executors.adaptive;

/**
 * One measured run of a task.
 *
 * @param declaredSize      the size the submission declared ({@code -1} = none)
 * @param allocatedBytes    bytes the worker thread allocated while the body ran, or
 *                          {@link #NOT_MEASURED}
 * @param soloHeapPeakBytes growth of heap-in-use while the body ran alone, or
 *                          {@link #NOT_MEASURED}
 * @param epochMillis       when the run completed
 * @param version           the application version the run was measured under
 *                          ({@link AdaptiveConfig#version()}), or {@code null} when the
 *                          configuration named none. What was learned under one version
 *                          is not trusted under another — see
 *                          {@link TaskProfile#fresh(String, long)}
 * @param durationNanos     how long the body ran, or {@link #NOT_MEASURED}. Wall time, not CPU
 *                          time: what matters for scheduling is how long the task occupies a
 *                          worker, including whatever it spent blocked
 */
public record Sample(long declaredSize, long allocatedBytes, long soloHeapPeakBytes, long epochMillis,
        String version, long durationNanos) {

    /** Sentinel for a measurement that was not taken. */
    public static final long NOT_MEASURED = -1L;

    /**
     * Validates: each of the three sizes is {@code >= -1}, at least one measurement was
     * taken — a sample that measured nothing teaches nothing — and the version, if any, is
     * not empty (an empty string would be indistinguishable from "none" in the profile file).
     */
    public Sample {
        if (declaredSize < -1) {
            throw new IllegalArgumentException("declaredSize must be >= -1 (-1 = none), got " + declaredSize);
        }
        if (allocatedBytes < NOT_MEASURED) {
            throw new IllegalArgumentException("allocatedBytes must be >= -1 (-1 = not measured), got " + allocatedBytes);
        }
        if (soloHeapPeakBytes < NOT_MEASURED) {
            throw new IllegalArgumentException("soloHeapPeakBytes must be >= -1 (-1 = not measured), got " + soloHeapPeakBytes);
        }
        if (durationNanos < NOT_MEASURED) {
            throw new IllegalArgumentException("durationNanos must be >= -1 (-1 = not measured), got " + durationNanos);
        }
        if (allocatedBytes < 0 && soloHeapPeakBytes < 0 && durationNanos < 0) {
            throw new IllegalArgumentException(
                    "a sample needs at least one measurement (allocatedBytes, soloHeapPeakBytes or durationNanos >= 0)");
        }
        if (version != null && version.isEmpty()) {
            throw new IllegalArgumentException("version must be null (unversioned) or non-empty");
        }
    }

    /**
     * @return whether this sample carries a memory measurement — as opposed to a timing alone,
     *         which teaches the sizing nothing and is kept in the ring on different terms
     *         (see {@link TaskProfile#append})
     */
    public boolean measuresMemory() {
        return allocatedBytes >= 0 || soloHeapPeakBytes >= 0;
    }

    /**
     * A sample with no duration recorded — what every sample written before durations were
     * learned looks like when it is read back.
     *
     * @param declaredSize      the size the submission declared ({@code -1} = none)
     * @param allocatedBytes    bytes allocated, or {@link #NOT_MEASURED}
     * @param soloHeapPeakBytes solo heap growth, or {@link #NOT_MEASURED}
     * @param epochMillis       when the run completed
     * @param version           the application version, or {@code null}
     */
    public Sample(long declaredSize, long allocatedBytes, long soloHeapPeakBytes, long epochMillis, String version) {
        this(declaredSize, allocatedBytes, soloHeapPeakBytes, epochMillis, version, NOT_MEASURED);
    }

    /**
     * An unversioned sample.
     *
     * @param declaredSize      the size the submission declared ({@code -1} = none)
     * @param allocatedBytes    bytes allocated, or {@link #NOT_MEASURED}
     * @param soloHeapPeakBytes solo heap growth, or {@link #NOT_MEASURED}
     * @param epochMillis       when the run completed
     */
    public Sample(long declaredSize, long allocatedBytes, long soloHeapPeakBytes, long epochMillis) {
        this(declaredSize, allocatedBytes, soloHeapPeakBytes, epochMillis, null, NOT_MEASURED);
    }
}
