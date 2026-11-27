package io.github.grishagrigorov.executors.observability;

/**
 * The JMX face of an {@link ExecutionRecorder}: totals as attributes, the two renderings
 * as operations — so {@code jconsole} or JDK Mission Control against a running ship
 * application becomes the troubleshooting console, with no custom tooling.
 *
 * <p>Open types only (longs and Strings), as MXBeans require.</p>
 */
public interface ExecutionRecorderMXBean {

    /** @return tasks submitted through this recorder, all executors, all time */
    long getSubmittedTotal();

    /** @return tasks whose completion this recorder has recorded */
    long getRecordedTotal();

    /** @return recorded tasks whose status was FAILED */
    long getFailedTotal();

    /** @return tasks that went through an admission policy (plain submissions excluded) */
    long getThrottledTotal();

    /** @return total nanoseconds throttled tasks spent waiting to be admitted */
    long getThrottleWaitNanosTotal();

    /**
     * Throttled tasks the policy <b>refused</b> and the liveness floor started anyway.
     *
     * <p>The number to alarm on. A few are the floor doing its job; a total approaching
     * {@link #getThrottledTotal()} means the policy is refusing everything and the floor is the
     * only thing making progress — the batch is running serially.</p>
     *
     * @return the count
     */
    long getFloorAdmittedTotal();

    /** @return history nodes currently held (folded nodes excluded) */
    int getExecutorNodeCount();

    /** @return the indented text rendering of the current snapshot */
    String renderText();

    /** @return the JSON rendering of the current snapshot */
    String renderJson();
}
