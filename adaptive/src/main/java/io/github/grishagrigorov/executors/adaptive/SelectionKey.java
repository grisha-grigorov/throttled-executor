package io.github.grishagrigorov.executors.adaptive;

/**
 * What the adaptive policy sorts queued candidates by.
 *
 * <p>Both orders put <em>unknown</em> tasks first regardless: an unknown has to run alone to be
 * measured, so the sooner it does, the sooner the batch can go parallel.</p>
 *
 * @see AdaptiveConfig#selectionKey()
 */
public enum SelectionKey {

    /**
     * Largest learned size first. The default, and the right choice when the binding constraint
     * is memory: offering the biggest task first is what lets it in while there is still room,
     * instead of leaving it stranded behind a crowd of small ones.
     */
    SIZE,

    /**
     * Longest learned run first — the classic longest-processing-time heuristic.
     *
     * <p>On a fixed pool this shortens the makespan: the task that would otherwise be started
     * last, and leave every other worker idle while it finishes, is started first. Worth choosing
     * when durations vary much more than sizes do.</p>
     *
     * <p>A task whose profile carries no duration — anything learned before durations were
     * recorded, or read from a v1/v2 profile file — sorts <em>last</em>, not first. Treating
     * "unknown duration" as "infinitely long" would put every carried-over profile in front of
     * everything actually measured.</p>
     */
    DURATION
}
