package io.github.grishagrigorov.executors.adaptive;

import java.time.Duration;
import java.util.concurrent.locks.LockSupport;
import java.util.function.LongSupplier;

/**
 * One lazily started daemon thread that, while a solo task runs, reads heap-in-use every
 * {@code period} and folds each reading into a {@link HeapRise} — so a transient peak the
 * task released before it finished is still seen. Parked (not polling) when no solo task
 * runs; started on the first {@link #begin}; stopped for good by {@link #close()}.
 *
 * <p>At most one solo task exists at a time by definition (solo means nothing else was in
 * flight when it started), so one sampling session is enough — but the session is
 * <em>identified</em>: {@link #begin} hands back a session id and {@link #end} answers only
 * for that id. A stale {@code end} (its session was superseded or already ended) and an
 * {@code end} after {@link #close()} both say "not measured" rather than returning a zero
 * the listener could store as a real heap peak, and neither touches a newer session.</p>
 */
final class HeapSampler {

    /** Name of the sampler thread. */
    static final String THREAD_NAME = "executors-adaptive-heap-sampler";

    /** {@link #end}'s answer when the session asked about is not the open one. */
    static final long NOT_MEASURED = -1L;

    /** A session id {@link #begin} never returns, so a {@link #end} with it is always stale. */
    static final long NO_SESSION = 0L;

    private final LongSupplier heapUsed;
    private final long periodNanos;
    private final Object lock = new Object();
    private Thread thread;                 // guarded by lock; created on first begin()
    private boolean active;                // guarded by lock
    private boolean closed;                // guarded by lock
    private HeapRise rise;                 // guarded by lock; the current session
    private long session = NO_SESSION;     // guarded by lock; id of the current session

    HeapSampler(LongSupplier heapUsed, Duration period) {
        this.heapUsed = heapUsed;
        this.periodNanos = Math.max(1L, period.toNanos());
    }

    /**
     * Starts a sampling session from {@code baseline}, superseding any open one.
     *
     * @param baseline heap-in-use at the task's start
     * @return the new session's id, to hand to {@link #end}; {@link #NO_SESSION} after
     *         {@link #close()}, when nothing is started
     */
    long begin(long baseline) {
        synchronized (lock) {
            if (closed) {
                return NO_SESSION;
            }
            rise = new HeapRise(baseline);
            session++;
            active = true;
            if (thread == null) {
                thread = new Thread(this::run, THREAD_NAME);
                thread.setDaemon(true);
                thread.start();
            }
            lock.notifyAll();
            return session;
        }
    }

    /**
     * Ends the given session, folding in the reading at the task's end.
     *
     * @param session      the id {@link #begin} returned
     * @param finalReading heap-in-use at the task's end, or {@code -1}
     * @return the largest rise seen during the session, {@code >= 0}; {@link #NOT_MEASURED}
     *         when {@code session} is not the open one — never begun, already ended,
     *         superseded by a later {@code begin}, or begun after {@link #close()}
     */
    long end(long session, long finalReading) {
        synchronized (lock) {
            if (rise == null || session != this.session) {
                return NOT_MEASURED;
            }
            active = false;
            rise.observe(finalReading);
            final long value = rise.value();
            rise = null;
            return value;
        }
    }

    /** Stops the thread; later {@link #begin} calls are no-ops. */
    void close() {
        synchronized (lock) {
            closed = true;
            active = false;
            lock.notifyAll();
        }
    }

    private void run() {
        while (true) {
            synchronized (lock) {
                while (!active && !closed) {
                    try {
                        lock.wait();
                    } catch (InterruptedException e) {
                        return;
                    }
                }
                if (closed) {
                    return;
                }
            }
            long used;
            try {
                used = heapUsed.getAsLong();
            } catch (RuntimeException e) {
                used = -1L;
            }
            synchronized (lock) {
                if (active && rise != null) {
                    rise.observe(used);
                }
            }
            LockSupport.parkNanos(periodNanos);
        }
    }
}
