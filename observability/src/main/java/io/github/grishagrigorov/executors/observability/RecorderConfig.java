package io.github.grishagrigorov.executors.observability;

/**
 * Bounds for an {@link ExecutionRecorder}. Every axis of growth has one:
 *
 * @param ringSize              recent {@link TaskRecord}s kept per executor node; older
 *                              records scroll off into the node's permanent aggregates
 * @param terminalChildrenLimit collected child nodes kept in full per parent; older ones
 *                              are folded into the parent's aggregates with an eviction count
 * @param failureClassLimit     distinct root-cause class keys kept per node; further
 *                              classes are counted under the {@code "(other)"} key, so the
 *                              failure map is O(1) even under pathological class churn
 */
public record RecorderConfig(int ringSize, int terminalChildrenLimit, int failureClassLimit) {

    public RecorderConfig {
        if (ringSize < 1 || terminalChildrenLimit < 0 || failureClassLimit < 1) {
            throw new IllegalArgumentException("ringSize >= 1, terminalChildrenLimit >= 0, failureClassLimit >= 1 required");
        }
    }

    /** Ring of 256 records, 64 terminal children, 64 distinct failure classes. */
    public static RecorderConfig defaults() {
        return new RecorderConfig(256, 64, 64);
    }
}
