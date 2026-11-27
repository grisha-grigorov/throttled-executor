package io.github.grishagrigorov.executors.exceptions;

/**
 * Stateless utility for walking {@link Throwable} cause chains.
 * Implemented as an empty enum to prevent instantiation.
 *
 * @author Grigor Grigorov
 */
public enum RootCauseExceptionUtil {
    ;

    /**
     * No legitimate cause chain is this deep; only a cycle reaches the cap. 64 keeps the
     * result exact for anything real while making the walk terminate unconditionally.
     */
    private static final int MAX_CAUSE_DEPTH = 64;

    /**
     * Walks the cause chain and returns the deepest cause — the {@code Throwable} that
     * started it all.
     *
     * <p>The walk is <b>depth-capped</b>. {@code initCause} forbids only self-causes, so a
     * cyclic chain of length two or more is constructible, and an uncapped walk would loop
     * forever — on this library's own failure-recording path that meant silently wedging
     * the worker thread that ran the failing task. On a cycle the cap makes the result an
     * arbitrary element of the cycle, which is as good as any; terminating is the contract.</p>
     *
     * @param throwable the top of the chain; must not be null
     * @return the deepest cause reachable within the cap; {@code throwable} itself if it has none
     */
    public static Throwable getRootCause(Throwable throwable) {

        Throwable rootCause = throwable;
        int hops = 0;

        while (rootCause.getCause() != null && ++hops < MAX_CAUSE_DEPTH) {
            rootCause = rootCause.getCause();
        }

        return rootCause;
    }
}
