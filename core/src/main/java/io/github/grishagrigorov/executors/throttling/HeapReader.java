package io.github.grishagrigorov.executors.throttling;

/**
 * How full the heap is, as a fraction of its maximum.
 *
 * <p>The one thing {@link HeapPressurePolicy} needs from the outside world, separated so the
 * policy can be tested without one — a real heap is not something a unit test can put at 90%
 * on demand.</p>
 *
 * <pre>{@code
 * // A policy that refuses while a test says the heap is full:
 * HeapReader fake = () -> 0.95;
 * new HeapPressurePolicy(0.85, 0.75, Duration.ZERO, fake, InstantSource.system());
 * }</pre>
 *
 * @see HeapPressurePolicy#platformReader()
 */
@FunctionalInterface
public interface HeapReader {

    /**
     * @return heap occupancy as a fraction of the maximum, normally in {@code [0, 1]}. A negative
     *         value means "unknown", which {@link HeapPressurePolicy} treats as "no pressure" —
     *         a policy that cannot see the heap must not be the reason a batch stops.
     */
    double occupancyFraction();
}
