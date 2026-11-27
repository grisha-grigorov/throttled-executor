package io.github.grishagrigorov.executors.adaptive;

import java.lang.management.ManagementFactory;
import java.util.function.LongSupplier;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads the current thread's cumulative allocated bytes through
 * {@code com.sun.management.ThreadMXBean}, or {@code -1} when that is not available:
 * non-HotSpot runtimes, allocation accounting disabled, or — on JDK 21+ — a virtual
 * thread, for which the JDK itself answers {@code -1}. Probed once; every read is
 * guarded, because a measurement must never fail a task.
 *
 * <p>{@code src/main} never calls {@code Thread.isVirtual()} (a 21 API; the floor is 17):
 * the JDK's own guard is what makes virtual workers read as {@code -1}.</p>
 */
final class AllocationMeter implements LongSupplier {

    private static final Logger LOG = LoggerFactory.getLogger(AllocationMeter.class);
    private static final AllocationMeter PLATFORM = new AllocationMeter(probe());

    private final com.sun.management.ThreadMXBean bean;          // null when unsupported

    private AllocationMeter(com.sun.management.ThreadMXBean bean) {
        this.bean = bean;
    }

    /** @return the process-wide meter */
    static AllocationMeter platform() {
        return PLATFORM;
    }

    /** @return whether this JVM can account allocation per thread at all */
    static boolean supported() {
        return PLATFORM.bean != null;
    }

    /** @return the current thread's allocated bytes so far, or {@code -1} when not measurable */
    @Override
    public long getAsLong() {
        if (bean == null) {
            return -1L;
        }
        try {
            final long bytes = bean.getCurrentThreadAllocatedBytes();
            return bytes < 0 ? -1L : bytes;
        } catch (RuntimeException e) {
            return -1L;
        }
    }

    private static com.sun.management.ThreadMXBean probe() {
        try {
            final java.lang.management.ThreadMXBean mx = ManagementFactory.getThreadMXBean();
            if (mx instanceof com.sun.management.ThreadMXBean sun && sun.isThreadAllocatedMemorySupported()) {
                if (!sun.isThreadAllocatedMemoryEnabled()) {
                    sun.setThreadAllocatedMemoryEnabled(true);
                }
                return sun;
            }
            LOG.info("executors-adaptive: per-thread allocation accounting unavailable on this JVM — learning through heap peaks only");
        } catch (RuntimeException | LinkageError e) {
            LOG.info("executors-adaptive: per-thread allocation accounting unavailable ({}) — learning through heap peaks only", e.toString());
        }
        return null;
    }
}
