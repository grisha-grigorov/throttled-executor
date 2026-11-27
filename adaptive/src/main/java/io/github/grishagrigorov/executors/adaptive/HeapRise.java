package io.github.grishagrigorov.executors.adaptive;

/**
 * One-pass accumulator for the solo heap peak: the largest rise of heap-in-use above the
 * <em>lowest reading seen so far</em>, over a sequence of readings that begins with the
 * reading at the task's start.
 *
 * <p>Why not "peak minus the start reading": a collection early in the run — typically
 * triggered by the task's own first allocations — drops heap-in-use below the start
 * reading, and everything the task builds afterwards would then read as zero. Against the
 * running low it reads as what it is. A collection late in the run costs nothing either:
 * the peak before it was already seen. What remains unmeasurable is a collection that
 * reclaims more than the task builds with nothing built afterwards — that reads as zero,
 * and the profile's maximum over the ring is what absorbs such samples.</p>
 *
 * <p>Not thread-safe; the owner synchronises.</p>
 */
final class HeapRise {

    private long low;
    private long rise;

    /**
     * @param baseline heap-in-use at the task's start
     */
    HeapRise(long baseline) {
        this.low = baseline;
    }

    /**
     * Folds one reading in; negative readings (a failed meter) are ignored.
     *
     * @param used heap-in-use now
     */
    void observe(long used) {
        if (used < 0) {
            return;
        }
        if (used - low > rise) {
            rise = used - low;
        }
        if (used < low) {
            low = used;
        }
    }

    /** @return the largest rise seen, {@code >= 0} */
    long value() {
        return rise;
    }
}
