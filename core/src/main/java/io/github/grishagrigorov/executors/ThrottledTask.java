package io.github.grishagrigorov.executors;

import java.util.Set;

/**
 * A unit of work offered to {@link ThrottledExecutor#submitThrottled(java.util.List)}: the
 * job itself, what it is expected to cost, an optional name, and optional tags.
 *
 * <p>The four parts answer four different questions, and only the first is required:</p>
 *
 * <ul>
 *   <li>{@link #job} — a {@code Runnable}, {@code Callable} or {@code Supplier}. Anything else is
 *       rejected at submission.</li>
 *   <li>{@link #size} — <em>your estimate</em> of the peak heap the body needs, in bytes. The
 *       memory policies compare sums of this number against a ceiling; nothing measures it, so a
 *       wrong estimate throttles wrongly. {@code -1} means "no estimate", which is what
 *       {@code executors-adaptive} learns and fills in.</li>
 *   <li>{@link #name} — the key the sizing learns against. Two tasks that do the same work should
 *       share a name; a task with no name can never be learned about.</li>
 *   <li>{@link #tags} — free-form labels naming the <em>resources</em> the body uses
 *       ({@code "hsqldb"}, {@code "spmbb"}), which a permit-style policy can meter separately from
 *       overall concurrency. Empty by default.</li>
 * </ul>
 *
 * <p><b>Identity, not value.</b> Two tasks describing the same work are two distinct tasks, and
 * this class deliberately does not override {@code equals}/{@code hashCode}. The throttling
 * machinery keeps one ledger entry per <em>claim</em>, keyed on the instance, so submitting the
 * same description twice is two pieces of work that are charged and released independently.</p>
 *
 * <pre>{@code
 * List<ThrottledTask<Void>> batch = List.of(
 *         ThrottledTask.of(() -> buildNoonReport(day), 40L * 1024 * 1024, "NoonReport"),
 *         ThrottledTask.of(() -> syncVoyage(id), -1, "VoyageSync", Set.of("hsqldb")));
 *
 * executor.submitThrottled(batch);
 * executor.waitForCompletion();
 * }</pre>
 *
 * @param <T> the job type — {@code Runnable}, {@code Callable<T>} or {@code Supplier<T>}
 */
public final class ThrottledTask<T> {

    /** The body to run: a {@code Runnable}, {@code Callable} or {@code Supplier}. */
    public final T job;

    /** Estimated peak heap in bytes, or {@code -1} for "no estimate". */
    public final long size;

    /** The name the sizing learns against, or {@code null} for an anonymous task. */
    public final String name;

    /** Resource labels a permit policy can meter on; never {@code null}, empty when untagged. */
    public final Set<String> tags;

    /**
     * An untagged task.
     *
     * @param job  the body — {@code Runnable}, {@code Callable} or {@code Supplier}
     * @param size estimated peak heap in bytes, or {@code -1} for "no estimate"
     * @param name the learning key, or {@code null}
     */
    public ThrottledTask(T job, long size, String name) {
        this(job, size, name, null);
    }

    /**
     * A task carrying resource tags.
     *
     * @param job  the body — {@code Runnable}, {@code Callable} or {@code Supplier}
     * @param size estimated peak heap in bytes, or {@code -1} for "no estimate"
     * @param name the learning key, or {@code null}
     * @param tags resource labels; {@code null} means untagged. The set is copied, so a later
     *             change to the caller's set cannot reach the task. Null <em>elements</em> are
     *             rejected — a policy could never meter on one.
     */
    public ThrottledTask(T job, long size, String name, Set<String> tags) {

        this.job = job;
        this.size = size;
        this.name = name;
        // Set.copyOf both snapshots and rejects null elements, which is exactly the contract.
        this.tags = tags == null || tags.isEmpty() ? Set.of() : Set.copyOf(tags);
    }

    /**
     * An anonymous, untagged task.
     *
     * @param <T>  the job type
     * @param job  the body
     * @param size estimated peak heap in bytes, or {@code -1}
     * @return the task
     */
    public static <T> ThrottledTask<T> of(T job, long size) {
        return new ThrottledTask<>(job, size, null, null);
    }

    /**
     * A named, untagged task.
     *
     * @param <T>  the job type
     * @param job  the body
     * @param size estimated peak heap in bytes, or {@code -1}
     * @param name the learning key
     * @return the task
     */
    public static <T> ThrottledTask<T> of(T job, long size, String name) {
        return new ThrottledTask<>(job, size, name, null);
    }

    /**
     * A named, tagged task.
     *
     * @param <T>  the job type
     * @param job  the body
     * @param size estimated peak heap in bytes, or {@code -1}
     * @param name the learning key
     * @param tags resource labels
     * @return the task
     */
    public static <T> ThrottledTask<T> of(T job, long size, String name, Set<String> tags) {
        return new ThrottledTask<>(job, size, name, tags);
    }

    /**
     * Whether this task carries {@code tag}.
     *
     * @param tag the label to look for
     * @return {@code true} if the task was created with that tag
     */
    public boolean hasTag(String tag) {
        return tags.contains(tag);
    }

    @Override
    public String toString() {
        /*
         * "unsized" rather than a number for the -1 sentinel: this string is what appears in the
         * IllegalArgumentException from submitThrottled and in policy-hook error logs, and
         * "0KB" there reads as "declared nothing needed" rather than "declared no estimate" —
         * two meaningfully different diagnoses. Sub-1MB sizes would truncate to "0MB", so those
         * are reported in KB.
         */
        final String humanSize = size < 0
                ? "unsized"
                : size < (1024 * 1024)
                        ? (size / 1024) + "KB"
                        : (size / (1024 * 1024)) + "MB";
        final String tagPart = tags.isEmpty() ? "" : ", Tags: " + tags;
        return "Task Name:" + name + " Size: " + humanSize + tagPart + ", Job: " + job;
    }
}
