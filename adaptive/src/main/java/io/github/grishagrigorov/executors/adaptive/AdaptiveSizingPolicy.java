package io.github.grishagrigorov.executors.adaptive;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.ForwardingAdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import java.util.ArrayDeque;
import java.util.Comparator;
import java.util.IdentityHashMap;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An {@link AdmissionPolicy} decorator that shows its delegate a <em>resized</em> copy of
 * every task — the learned estimate instead of the declared size — and quarantines tasks
 * nothing is known about so they run alone, once. Built by
 * {@link AdaptiveSizing#policy(AdmissionPolicy)}; the delegate never learns it is being fed
 * learned numbers.
 *
 * <h2>How the pieces fit the harness</h2>
 * <ul>
 *   <li>{@link #admit} is pure: it resolves the candidate's effective size and asks the
 *       delegate about a resized copy, handing it a snapshot whose tracked bytes are this
 *       decorator's own sum of <em>effective</em> sizes in flight (the harness's figure sums
 *       declared sizes, which is the wrong currency here).</li>
 *   <li>The original→resized pairing is created in {@link #onTaskStart}, never in
 *       {@code admit}: the harness's liveness floor bypasses {@code admit} entirely. The
 *       same resized instance is forwarded to {@link #onTaskEnd}, so a delegate ledger
 *       balances even when the estimate changed while the task ran. The pairing is
 *       <em>per claim</em>, not per instance: core admits the same {@code ThrottledTask}
 *       once per occurrence in a batch (and again when a later batch re-submits it while
 *       the earlier run is still going) and charges each claim, so a second start of an
 *       instance already in flight is a second entry and a second charge, and each end
 *       releases one.</li>
 *   <li>An <b>unknown</b> task — no declared size and nothing learned — is declared
 *       {@linkplain #requiresExclusive exclusive}, so the harness admits it only into an
 *       empty pool and admits nothing beside it. While it runs it is shown to the delegate at
 *       {@link AdaptiveConfig#unknownReserveBytes()}, so its heap peak is attributable. One
 *       such run makes it known. A task the <em>delegate</em> isolates is exclusive too — the
 *       decorator adds a reason for isolation, it never removes one.</li>
 *   <li><b>Wrapping this policy</b> in a decorator of your own: extend
 *       {@link ForwardingAdmissionPolicy} — this class does — which forwards the whole SPI
 *       (a reflective test refuses any SPI method it does not); or, hand-rolled, forward
 *       {@link #inFlightTaskCount()} and {@link #ledgerSpansExecutors()} as well as
 *       {@link #requiresExclusive} and {@link #mayRequireExclusive} as well as the three
 *       hooks (see {@link AdmissionPolicy#mayRequireExclusive()}). The harness asks the
 *       outermost policy, and only if it says it may isolate; a pass-through that
 *       forwards {@code admit}/{@code onTaskStart}/{@code onTaskEnd} alone hides the
 *       quarantine completely — {@code admit} refuses the unknown, the liveness floor admits
 *       it anyway, known work starts beside it on the next scan, and nothing is logged.
 *       Forward {@link AdmissionPolicy#onScanStart()} too, unconditionally — it is the
 *       lifecycle announcement this policy refreshes its delegate's answer on, and unlike a
 *       query an announcement cannot be short-circuited.</li>
 *   <li>{@link #inFlightTaskCount()} is the larger of the own ledger and the delegate's
 *       <em>while the ledger has entries</em>: an idle root cannot floor-admit past another
 *       root's work when both share this instance. With an empty ledger the delegate's answer
 *       is returned as is — a snapshot-based delegate answers {@code -1}, and the harness
 *       behaves exactly as it would undecorated. The {@code -1} is not interchangeable with
 *       {@code 0}: for the counts themselves the harness takes the larger of ledger and
 *       tree anyway, and {@link #ledgerSpansExecutors()} is forwarded on the same shape —
 *       own entries span whatever trees share this instance, otherwise the delegate's
 *       explicit answer stands, so wrapping cannot widen a per-tree delegate's
 *       parked-claims scope. Report {@code -1} for "no ledger", never {@code 0}.</li>
 * </ul>
 *
 * <h2>Known limits</h2>
 * <p>Two roots sharing this instance are not atomic at a global zero — the same window
 * {@code GlobalMemoryBudgetPolicy} documents.</p>
 *
 * <p><b>Quarantine holds the pool per executor tree, not per decorator.</b> Exclusivity is
 * enforced by the {@code ThrottleHarness}, and there is one of those per root tree, whereas the
 * private census this replaced lived on the decorator instance and so spanned every tree
 * sharing it. An unknown does wait for the <em>ledger</em> to drain before it starts — the
 * harness measures "alone" against {@link #inFlightTaskCount()}, which counts every tree sharing
 * this instance — but once it runs, only its own tree is held: another root sharing the
 * instance can still start known work beside it. With a memory-budget delegate that is covered
 * — an unknown reserves {@link AdaptiveConfig#unknownReserveBytes()}, which by default saturates
 * the budget and keeps the other tree out anyway — but with a delegate that only counts tasks,
 * such as a bare {@code MaxConcurrencyPolicy}, it is not. On the virtual backend, where
 * allocation is unavailable and the solo heap peak is the only measurement, an overlapped run
 * measures nothing and the task is quarantined again next batch. Give each root its own
 * {@code policy(...)} — the ledger is per call already — or keep a memory budget in the
 * delegate.</p>
 */
public final class AdaptiveSizingPolicy extends ForwardingAdmissionPolicy {

    private static final Logger LOG = LoggerFactory.getLogger(AdaptiveSizingPolicy.class);

    private final AdaptiveSizing sizing;
    private final Object ledgerLock = new Object();
    /** One entry per claim, queued per instance (FIFO) — see the class doc. */
    private final IdentityHashMap<ThrottledTask<?>, ArrayDeque<ThrottledTask<?>>> inFlight = new IdentityHashMap<>();  // guarded by ledgerLock
    private int inFlightCount;                                                                  // guarded by ledgerLock
    private long effectiveBytesInFlight;                                                        // guarded by ledgerLock

    /*
     * One wrapped snapshot per scan. The harness takes one snapshot per admitFirst() and
     * offers every remaining candidate against it; with a saturated budget that is the whole
     * queue on every completion. Wrapping per candidate allocated one object per candidate
     * per scan for the same (snapshot, own bytes) pair — core itself allocates nothing per
     * candidate on that path. A single-entry memo keyed on the raw snapshot's identity and
     * the ledger figure it was wrapped with makes a scan share one wrapper.
     */
    /*
     * There is deliberately no memo for effective() HERE. One was tried, keyed on the scan's
     * snapshot identity so that the questions the harness asks about a candidate would share a
     * store lookup — and it never returned a cached value once. Only admit() had a snapshot to
     * key on; requiresExclusive() and the ordering comparator have none, pass null, and run
     * FIRST, so every read found a memo that the previous null-scan write had already
     * invalidated. What remained was two lock acquisitions and a WeakReference allocation per
     * call, paid on the harness's locked path, for nothing.
     *
     * The cost it was aiming at is real, and got worse: the harness asks requiresExclusive() for
     * EVERY candidate in the window on every scan (it has to — a blocked exclusive candidate
     * anywhere in the window must refuse the whole scan), then admit() for the ones ahead of the
     * chosen candidate. With maxSampleAge configured, TaskProfile's allocation-free fast path
     * stops firing for a profiled name in steady state, so each of those questions copied the
     * ring and re-ran the aggregation.
     *
     * Widening the key out here is the wrong repair: an entry that outlives one scan answers
     * "unknown" for a candidate a sample has since landed for, which keeps it demanding
     * exclusivity and claiming fairness priority forever. The right place is the immutable
     * profile — TaskProfile.fresh() now keeps its last filtered view alongside the exact cutoff
     * range that view answers, so it is a memo rather than a cache (never stale), and record()
     * invalidates it for free by returning a different instance.
     */

    /**
     * The delegate's {@link AdmissionPolicy#mayRequireExclusive()}, refreshed once per scan by
     * {@link #onScanStart()} — the harness's per-scan lifecycle announcement — rather than
     * read per candidate: the per-candidate question runs under the harness lock
     * for every remaining candidate on every scan, where a composite's member walk is real
     * cost and one volatile read is not. The SPI fixes the answer for the length of a scan
     * anyway. Seeded at construction so direct calls before any scan see a sane value; a
     * thrown answer is read as "may" — the framework's convention for this hook, the side
     * that costs a scan rather than an isolation.
     */
    private volatile boolean delegateMayIsolate;

    private PoolSnapshot lastRaw;                                                               // guarded by ledgerLock
    private long lastOwnBytes;                                                                  // guarded by ledgerLock
    private PoolSnapshot lastWrapped;                                                           // guarded by ledgerLock

    AdaptiveSizingPolicy(AdaptiveSizing sizing, AdmissionPolicy delegate) {
        super(delegate);
        this.sizing = Objects.requireNonNull(sizing, "sizing");
        this.delegateMayIsolate = guardedDelegateMayIsolate(delegate);
    }

    /** @return the policy that decides on the resized tasks */
    public AdmissionPolicy delegate() {
        return delegate;
    }

    /**
     * What the delegate is shown for {@code task}. An unnamed task is invisible to the
     * decorator — nothing can ever be learned about it, so it is shown exactly as declared
     * and is never {@linkplain #unknown unknown}; quarantining it would serialise the batch
     * around it on every run, forever, with nothing gained.
     */
    private long effective(ThrottledTask<?> task) {

        if (task.name == null) {
            return task.size;
        }

        return sizing.effectiveSize(task.name, task.size);
    }


    /** Nothing declared, nothing learned — and a name, so a solo run can teach something. */
    private static boolean unknown(ThrottledTask<?> task, long effective) {
        return task.name != null && effective == AdaptiveSizing.UNKNOWN;
    }

    /**
     * An unprofiled task must run alone, so that what it costs can be measured.
     *
     * <p>This used to be a private census: refuse every candidate while {@code unknownInFlight}
     * was above zero, and override {@link #inFlightTaskCount()} so the liveness floor — which
     * exists to stop the system throttling to a standstill, not to isolate anything — would let
     * the unknown in. It worked, and it starved: an unknown could only start at a moment of
     * emptiness, and a steady stream of known tasks from another coordinator never produced one.
     * The framework now owns both halves, and within one root tree the caveat is gone with
     * them: a blocked exclusive waiter earns priority on its harness at once. That priority is
     * per harness, though — two roots sharing this instance share a ledger but not a fairness
     * queue, so a steady stream of known work on a busy sibling root can keep the ledger from
     * ever draining and an unknown on the idle root waiting with it. Give each root its own
     * {@code policy(...)} call if that matters.</p>
     */
    @Override
    public boolean requiresExclusive(ThrottledTask<?> task) {
        /*
         * Own reason first, then the delegate's. This is a decorator: every other question the
         * harness asks is forwarded, and this one was not — a delegate that isolated a task lost
         * that isolation the moment it was wrapped, with nothing logged, because the interface
         * default is false and false is an ordinary answer. The delegate sees the task as
         * declared, not resized: isolation keys on what a task IS (its name, its tags), and the
         * resized copy exists only so admit() can weigh it.
         *
         * Cost matters here more than anywhere else the decorator is asked: because this class
         * overrides requiresExclusive, mayRequireExclusive() is true, and the harness then asks
         * this question of EVERY remaining candidate on every scan — a List batch of N
         * declared-size tasks is N² questions per batch. Only a task declared -1 can ever be
         * unknown — AdaptiveSizing.canBeUnknown owns that rule, beside the effectiveSize
         * implementation it abbreviates — so for every other task the answer is the delegate's
         * alone, and not even a call when the delegate said at the scan's start that it
         * cannot currently isolate (see the delegateMayIsolate field). The scan stays O(N)
         * per step; this makes each step free of store lookups and clock reads.
         */
        if (AdaptiveSizing.canBeUnknown(task.size) && unknown(task, effective(task))) {
            return true;
        }
        return delegateMayIsolate && delegate.requiresExclusive(task);
    }

    /**
     * The scan boundary: forwarded to the delegate first (it may hang refreshes of its own
     * here), then the delegate's {@link AdmissionPolicy#mayRequireExclusive()} is re-read
     * for this scan's {@link #requiresExclusive} calls to use. The SPI fixes that answer
     * per scan, not per construction: a delegate that starts isolating is honoured from the
     * next scan on. ({@code mayRequireExclusive} itself stays a pure query — this class's
     * derived answer is always "may", it quarantines unknowns on its own account.)
     */
    @Override
    public void onScanStart() {
        try {
            delegate.onScanStart();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            LOG.error("Delegate policy {} threw from onScanStart — ignored.",
                    delegate.getClass().getName(), hookFailure);
        }
        delegateMayIsolate = guardedDelegateMayIsolate(delegate);
    }

    /**
     * A thrown answer is "may" — the side that costs a scan rather than an isolation, and the
     * framework's convention for this hook everywhere it is read. Errors short of a
     * {@code VirtualMachineError} are absorbed too (a lazily linked flag store throws
     * {@code NoClassDefFoundError}), exactly as the harness's and {@code CompositePolicy}'s
     * guards do — this runs at wiring time as well, where an escape would abort executor
     * construction instead of degrading to full scans.
     */
    private static boolean guardedDelegateMayIsolate(AdmissionPolicy delegate) {
        try {
            return delegate.mayRequireExclusive();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            LOG.error("Delegate policy {} threw from mayRequireExclusive — treated as \"may\" (full scans).",
                    delegate.getClass().getName(), hookFailure);
            return true;
        }
    }

    /** The framework's fatal set, spelled the way core's three copies spell it. */
    private static void rethrowIfFatal(Throwable hookFailure) {
        if (hookFailure instanceof VirtualMachineError fatal) {
            throw fatal;
        }
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        final long effective = effective(candidate);
        if (unknown(candidate, effective)) {
            // The harness admits it exclusively, without asking; saying yes here would let it
            // start beside other work and be measured against noise.
            return false;
        }
        final PoolSnapshot wrapped;
        synchronized (ledgerLock) {
            if (snapshot != lastRaw || effectiveBytesInFlight != lastOwnBytes || lastWrapped == null) {
                lastRaw = snapshot;
                lastOwnBytes = effectiveBytesInFlight;
                lastWrapped = wrap(snapshot, effectiveBytesInFlight);
            }
            wrapped = lastWrapped;
        }
        return delegate.admit(resized(candidate, effective), wrapped);
    }

    @Override
    public void onTaskStart(ThrottledTask<?> task) {
        final long effective = effective(task);
        final boolean unknown = unknown(task, effective);
        final long shown = unknown ? sizing.config().unknownReserveBytes() : effective;
        final ThrottledTask<?> resized = resized(task, shown);
        synchronized (ledgerLock) {
            inFlight.computeIfAbsent(task, ignored -> new ArrayDeque<>(1)).addLast(resized);
            inFlightCount++;
            effectiveBytesInFlight += bytesOf(shown);
        }
        delegate.onTaskStart(resized);
    }

    @Override
    public void onTaskEnd(ThrottledTask<?> task) {
        ThrottledTask<?> resized = null;
        synchronized (ledgerLock) {
            final ArrayDeque<ThrottledTask<?>> claims = inFlight.get(task);
            if (claims != null) {
                resized = claims.pollFirst();
                if (claims.isEmpty()) {
                    inFlight.remove(task);
                }
            }
            if (resized != null) {
                inFlightCount--;
                effectiveBytesInFlight -= bytesOf(resized.size);
            }
        }
        if (resized == null) {
            LOG.warn("executors-adaptive: onTaskEnd for a task this policy never saw start — dropped: {}", task);
            return;                                      // forwarding would unbalance the delegate's ledger
        }
        delegate.onTaskEnd(resized);
    }

    /**
     * Unknowns first (they can only start when nothing runs, so offer them while that is
     * true), then by {@link AdaptiveConfig#selectionKey()} descending — effective size, or
     * longest learned run. Each comparator memoizes the ranks it has seen for the tasks it has
     * seen, so learning that lands during a sort cannot make the ordering inconsistent; the
     * executor asks for a fresh comparator per submission.
     */
    @Override
    public Comparator<ThrottledTask<?>> selectionOrder() {
        final IdentityHashMap<ThrottledTask<?>, Long> ranks = new IdentityHashMap<>();
        return (a, b) -> Long.compare(rank(b, ranks), rank(a, ranks));
    }

    private long rank(ThrottledTask<?> task, IdentityHashMap<ThrottledTask<?>, Long> ranks) {
        Long rank = ranks.get(task);
        if (rank == null) {
            rank = computeRank(task);
            ranks.put(task, rank);
        }
        return rank;
    }

    private long computeRank(ThrottledTask<?> task) {

        final long effective = effective(task);

        if (unknown(task, effective)) {
            return Long.MAX_VALUE;
        }

        if (sizing.config().selectionKey() != SelectionKey.DURATION) {
            return effective;
        }

        /*
         * Zero, not Long.MAX_VALUE, for a task with no measured duration. Reading "unknown
         * duration" as "infinitely long" would put every profile carried over from a v1/v2 file
         * — none of which carry durations — in front of everything actually measured, which is
         * the opposite of what the heuristic is for.
         */
        final TaskProfile profile = task.name == null ? null : sizing.freshProfile(task.name);

        return profile == null ? 0L : profile.duration().orElse(0L);
    }

    /** A size of {@code -1} (none declared, nothing learned) weighs nothing in the byte ledger. */
    private static long bytesOf(long size) {
        return Math.max(0L, size);
    }

    @Override
    public int inFlightTaskCount() {
        final int own;
        synchronized (ledgerLock) {
            own = inFlightCount;
        }
        final int delegates = delegate.inFlightTaskCount();
        return own > 0 ? Math.max(own, delegates) : delegates;     // an empty ledger has nothing to add
    }

    /**
     * Spans while the own ledger has entries — they live on whatever trees share this
     * instance — otherwise the delegate's own answer stands, so wrapping a per-tree
     * delegate cannot silently widen its parked-claims scope to process-wide. (The
     * inherited default would: it derives from the forwarded count's sign.) A delegate
     * that cannot answer is read as per-tree, the side that refuses an exemption.
     */
    @Override
    public boolean ledgerSpansExecutors() {
        synchronized (ledgerLock) {
            if (inFlightCount > 0) {
                return true;
            }
        }
        try {
            return delegate.ledgerSpansExecutors();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            LOG.error("Delegate policy {} threw from ledgerSpansExecutors — treated as per-tree.",
                    delegate.getClass().getName(), hookFailure);
            return false;
        }
    }

    private static ThrottledTask<?> resized(ThrottledTask<?> task, long size) {
        // Everything but the size is carried over — a delegate that meters on tags must still
        // see them, or wrapping a resource policy in this decorator would silently un-meter it.
        return size == task.size ? task : new ThrottledTask<>(task.job, size, task.name, task.tags);
    }

    private static PoolSnapshot wrap(PoolSnapshot raw, long effectiveBytes) {
        return new PoolSnapshot() {
            @Override
            public int runningTasks() {
                return raw.runningTasks();
            }

            @Override
            public long trackedSizeBytes() {
                return effectiveBytes;
            }
        };
    }

    @Override
    public String toString() {
        synchronized (ledgerLock) {
            return "AdaptiveSizingPolicy[inFlight=" + inFlightCount + ", effectiveBytes=" + effectiveBytesInFlight
                    + ", delegate=" + delegate + "]";
        }
    }
}
