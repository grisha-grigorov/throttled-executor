package io.github.grishagrigorov.executors.throttling;

import java.util.HashMap;
import java.util.Map;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * Caps how many in-flight tasks may hold each named resource.
 *
 * <p>{@link MaxConcurrencyPolicy} bounds the pool as a whole, which says nothing about the four
 * connections the database will actually give you, or the one socket the SPMBB will talk over.
 * This policy meters those separately, keyed on {@link ThrottledTask#tags}:</p>
 *
 * <pre>{@code
 * AdmissionPolicy policy = CompositePolicy.allOf(
 *         new MaxConcurrencyPolicy(16),                                  // the pool
 *         new ResourcePermitPolicy(Map.of("hsqldb", 4, "spmbb", 2)));    // what is behind it
 *
 * executor.submitThrottled(List.of(
 *         ThrottledTask.of(() -> loadVoyages(), 40L << 20, "Voyages", Set.of("hsqldb")),
 *         ThrottledTask.of(() -> pollSensors(), 4L << 20,  "Sensors", Set.of("spmbb"))), policy);
 * }</pre>
 *
 * <p>A task holding several tags needs room for <em>all</em> of them, and takes them together or
 * not at all — a task admitted with some of its resources would be able to hold them while
 * waiting for the rest, which is how deadlocks get built. Tags with no configured limit, and
 * tasks with no tags, are unconstrained: this policy only ever says no about resources it was
 * explicitly told to meter.</p>
 *
 * <p>Counts live in the {@code onTaskStart}/{@code onTaskEnd} hooks and are guarded by one lock,
 * so an instance may be shared across executors to enforce a process-wide cap on a resource that
 * is itself process-wide — which a connection pool usually is.</p>
 *
 * <p>{@link #inFlightTaskCount()} is left at {@code -1}: a resource being busy is not a reason to
 * disable the liveness floor, and under the floor a lone task will run even if its resource is
 * nominally full — the alternative being a batch that never finishes.</p>
 *
 * <h2>Known limit: shared across trees, a permit can be overshot</h2>
 *
 * <p>{@code admit} and {@code onTaskStart} take this policy's lock separately; what makes them
 * one atomic decision is the <em>harness</em> lock held across both. There is one harness per
 * executor tree, so an instance shared by two trees — which the process-wide use suggested above
 * is exactly — can have both pass {@code admit} at limit&minus;1 and then both claim, putting one
 * task over the limit.</p>
 *
 * <p>The same transient window {@link GlobalMemoryBudgetPolicy} documents, and the same
 * judgement: a fail-safe cap, not an exact reservation system. It is bounded by the number of
 * trees sharing the instance and lasts only until one of them finishes, and the alternative — a
 * lock spanning two harnesses — is a deadlock waiting to happen. Where a resource genuinely
 * cannot tolerate one extra holder, size the limit one below what it can take, or hold a real
 * semaphore inside the task body, which is the only place that can be exact.</p>
 */
public final class ResourcePermitPolicy implements AdmissionPolicy {

    private final Map<String, Integer> limits;

    /** Tag to number of permits currently held; guarded by its own monitor. */
    private final Map<String, Integer> inUse = new HashMap<>();

    /**
     * @param limits maximum concurrent tasks per tag; copied, so later changes to the caller's map
     *               do not reach the policy
     * @throws IllegalArgumentException if any limit is below 1 — that would mean "never", which is
     *                                  a configuration mistake rather than a throttle
     */
    public ResourcePermitPolicy(Map<String, Integer> limits) {

        for (Map.Entry<String, Integer> limit : limits.entrySet()) {
            if (limit.getValue() == null || limit.getValue() < 1) {
                throw new IllegalArgumentException(
                        "limit for '" + limit.getKey() + "' must be at least 1, was " + limit.getValue());
            }
        }

        this.limits = Map.copyOf(limits);
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {

        if (candidate.tags.isEmpty()) {
            return true;
        }

        synchronized (inUse) {
            for (String tag : candidate.tags) {
                final Integer limit = limits.get(tag);
                if (limit != null && inUse.getOrDefault(tag, 0) >= limit) {
                    return false;
                }
            }
            return true;
        }
    }

    @Override
    public void onTaskStart(ThrottledTask<?> task) {

        if (task.tags.isEmpty()) {
            return;
        }

        synchronized (inUse) {
            for (String tag : task.tags) {
                if (limits.containsKey(tag)) {
                    inUse.merge(tag, 1, Integer::sum);
                }
            }
        }
    }

    @Override
    public void onTaskEnd(ThrottledTask<?> task) {

        if (task.tags.isEmpty()) {
            return;
        }

        synchronized (inUse) {
            for (String tag : task.tags) {
                if (!limits.containsKey(tag)) {
                    continue;
                }
                // Remove at zero rather than leaving it: the map is then bounded by the number of
                // resources actually in use, not by every tag ever seen.
                inUse.compute(tag, (ignored, held) -> held == null || held <= 1 ? null : held - 1);
            }
        }
    }

    /**
     * How many permits for {@code tag} are currently held.
     *
     * @param tag the resource label
     * @return the count, zero if none are out
     */
    public int inUse(String tag) {
        synchronized (inUse) {
            return inUse.getOrDefault(tag, 0);
        }
    }

    @Override
    public String toString() {
        synchronized (inUse) {
            return "ResourcePermitPolicy" + limits + " in use " + inUse;
        }
    }
}
