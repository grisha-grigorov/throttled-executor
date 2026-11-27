package io.github.grishagrigorov.executors.internal;

import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Framework-owned throttling mechanics: in-flight counters, the blocking
 * wait, and completion signalling. One instance per {@code ThrottledExecutor};
 * <b>not</b> user-extensible — policies plug in via {@link AdmissionPolicy},
 * this class guarantees the mechanics they must never break:
 *
 * <ul>
 *   <li>{@link #onEnd} signals waiting coordinators <b>unconditionally</b> on
 *       every task completion — a policy bug can never cause a lost wake-up.</li>
 *   <li>{@link #awaitStateChange} is a <i>timed</i> wait keyed to a state
 *       version, so coordinators self-heal from any missed signal and can poll
 *       time-based policies such as {@link RateLimitPolicy}.</li>
 *   <li><b>Exclusivity.</b> A candidate the policy marks
 *       {@link AdmissionPolicy#requiresExclusive(ThrottledTask) exclusive} is admitted only
 *       into an empty pool, and owns the pool while it runs — decided and claimed under the
 *       same lock as everything else, so no coordinator can slip in beside it.</li>
 *   <li><b>Fairness.</b> Each coordinator holds a {@link Ticket} for its batch. A ticket
 *       refused {@value #PRIORITY_AFTER_REFUSALS} scans in a row — or once, if what it holds
 *       is exclusive — takes priority, and every other coordinator yields until the oldest
 *       privileged ticket has claimed. At rest the queue is empty and costs nothing; under
 *       contention no coordinator can be passed over indefinitely.</li>
 * </ul>
 *
 * <p>Public only so {@code ThrottledExecutor} (a different package) can drive it;
 * application code should never call these methods.</p>
 */
public final class ThrottleHarness {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottleHarness.class);

    private final ReentrantLock lock = new ReentrantLock();
    private final Condition stateChanged = lock.newCondition();

    private final AtomicInteger runningTasks = new AtomicInteger(0);
    private final AtomicLong trackedSizeBytes = new AtomicLong(0);

    /** Incremented (under {@link #lock}) on every task completion; volatile for unlocked reads. */
    private volatile long stateVersion = 0;

    /**
     * Throttle claims held by coordinators parked in {@link #awaitStateChange} on this harness.
     * Guarded by {@link #lock}.
     *
     * <p>What bounds the re-entrancy exemption. A nested coordinator is exempt from the policy
     * only while everything the policy counts in flight is itself waiting to nest — that is the
     * only situation in which nothing will ever finish on its own. Every claim a parked thread
     * holds is one in-flight task that cannot end until that thread is unparked, so
     * {@code effectiveInFlight <= parked + own} (the scanning thread's own claims) means
     * "nobody out there is going to release anything". Above that, unrelated work is running
     * and will end by itself; the nested coordinator waits for it like anyone else rather than
     * being admitted on top of it.</p>
     */
    private int parkedClaims;

    /**
     * Throttle claims held by the current thread — how many throttled task bodies it is inside.
     *
     * <p>The harness owns this because the harness is what a claim is a claim <em>on</em>.
     * Maintained by {@link #holdingAClaim}, read by {@link #claimsHeldByCurrentThread()}. A
     * count rather than a flag: on {@code CALLER_RUNS} a body nested two deep runs on the
     * thread that holds both claims, and an exemption that credited that thread with one
     * would refuse the third level for ever — everything in flight was held by the scanning
     * thread itself, and nothing was parked.</p>
     */
    private static final ThreadLocal<int[]> CLAIMS_HELD = ThreadLocal.withInitial(() -> new int[1]);

    /**
     * The same count across every harness in the process, for a policy whose ledger spans
     * trees. Scoped like the thing that blocks: when a policy reports its own in-flight count,
     * the parked holders that count could be waiting on ANY harness — a task in root A parked
     * on root B's harness is still one of the tasks A's shared budget is counting.
     */
    private static final AtomicInteger PARKED_CLAIMS_EVERYWHERE = new AtomicInteger();

    /**
     * Wraps a throttled body so that, while it runs, its thread is known to hold one more
     * claim. Executors wrap every throttled body with this; nothing else should.
     *
     * @param <T>  the body's result type
     * @param body the throttled task body
     * @return the body, counted
     */
    public static <T> java.util.concurrent.Callable<T> holdingAClaim(java.util.concurrent.Callable<T> body) {
        return () -> {
            final int[] held = CLAIMS_HELD.get();
            held[0]++;
            try {
                return body.call();
            } finally {
                if (--held[0] == 0) {
                    CLAIMS_HELD.remove();   // a pooled worker must not carry it onward
                }
            }
        };
    }

    /**
     * @return how many throttled task bodies the current thread is inside; {@code 0} for a
     *         thread that holds no claim, which includes a plain {@code submit()} body
     */
    public static int claimsHeldByCurrentThread() {
        return CLAIMS_HELD.get()[0];
    }

    /**
     * The exclusive task currently in flight, or {@code null}.
     *
     * <p>Compared by identity: the ledgers charge per claim, so the holder is a specific
     * instance, not "some task that looks like this one".</p>
     */
    private ThrottledTask<?> exclusiveInFlight;

    /**
     * How many claims the exclusive holder has outstanding.
     *
     * <p>Normally one — nothing else is admitted beside an exclusive task. But the same
     * {@code ThrottledTask} instance may legitimately be claimed twice (a batch is allowed to
     * list it twice, and the re-entrancy exemption can admit it from inside its own body), and
     * {@code exclusiveInFlight} is identity-compared, so the first {@code onEnd} would otherwise
     * release exclusivity while the other claim was still running.</p>
     */
    private int exclusiveClaims;

    /**
     * How many consecutive refused scans earn a coordinator priority.
     *
     * <p>Not 1: strict FIFO would let a coordinator whose only candidate does not fit block
     * every other coordinator's work behind it, which trades starvation for head-of-line
     * blocking. Waiting a few scans first means the fairness machinery costs nothing in the
     * ordinary case and only engages when someone is actually being passed over. At the
     * coordinator poll interval of 100&nbsp;ms this is a fraction of a second.</p>
     */
    public static final int PRIORITY_AFTER_REFUSALS = 3;

    /** Source of ticket numbers; only meaningful in comparisons, so wrap-around is irrelevant. */
    private long nextTicketNumber;

    /**
     * Coordinators that have been refused often enough to take priority, oldest first.
     *
     * <p>Empty in the ordinary case — this is the fairness mechanism at rest.</p>
     */
    private final java.util.TreeMap<Long, Ticket> privileged = new java.util.TreeMap<>();

    private record Snapshot(int runningTasks, long trackedSizeBytes) implements PoolSnapshot {
    }

    /**
     * One coordinator's place in the queue for admission.
     *
     * <p>The harness had no notion of <em>who</em> was waiting: {@code awaitStateChange} is a
     * bare {@code signalAll}, so after a completion whichever thread woke first won, every time.
     * With several coordinators sharing a subtree's harness that is not merely unfair, it is
     * unbounded — a steady stream of work from one coordinator could keep another's candidate
     * out forever, which is what the adaptive quarantine's starvation caveat described.</p>
     *
     * <p>A ticket accumulates refusals. After {@value #PRIORITY_AFTER_REFUSALS} of them it
     * becomes <b>privileged</b>, and while any privileged ticket exists only the oldest one may
     * claim. A candidate needing exclusivity is privileged at once, since it can never be
     * admitted while other work keeps arriving.</p>
     *
     * <p>Close it when the batch is done — including when it is abandoned — so a coordinator
     * that walks away cannot hold up the rest. It is {@link AutoCloseable} for exactly that.</p>
     */
    public final class Ticket implements AutoCloseable {

        private long number = -1;
        private int refusals;
        private boolean privilegedNow;
        private boolean lastAdmissionByFloor;

        /**
         * Whether this ticket's last <em>scan</em> ended with "my candidate needs the pool to
         * itself".
         *
         * <p>The difference between a waiter that will certainly be satisfied and one that may
         * never be. A ticket blocked on isolation is waiting for {@code runningTasks == 0} and
         * takes its admission on its very next scan once that happens; a ticket refused by a
         * policy is waiting on the policy, which owes it nothing. Only the first kind may
         * outrank the liveness floor — see the yield in {@code admitFirst}.</p>
         *
         * <p>Written only by {@link #refused(boolean)}, which reports a scan's outcome. The two
         * early returns that yield WITHOUT scanning go through {@link #yielded()} and leave it
         * alone: they learned nothing about the candidate, so they have nothing to say about
         * it. Today that distinction is belt-and-braces — a ticket blocked on isolation is
         * privileged and, by the yield rule itself, the oldest, so it never reaches a yield —
         * but a flag that a non-scan could clear is a flag whose meaning depends on an
         * invariant nobody tests, and one that a non-scan cannot clear is simply true.</p>
         */
        private boolean waitingForEmptiness;

        private Ticket() {
        }

        /**
         * Whether the most recent admission on this ticket came from the liveness floor rather
         * than from the policy agreeing.
         *
         * <p>Reported here rather than returned from {@code admitFirst} so the scan stays
         * allocation-free: a wrapper object per admitted task is a real cost at sixty thousand
         * tasks a round, and the ticket is already this coordinator's scratchpad. Only
         * meaningful immediately after a non-null {@code admitFirst}.</p>
         *
         * @return {@code true} if the floor overrode a refusing policy
         */
        public boolean lastAdmissionByFloor() {
            return lastAdmissionByFloor;
        }

        /**
         * A scan ended without a claim. Caller must hold {@link #lock}.
         *
         * @param blockedOnIsolation whether it was an exclusive candidate that could not be
         *                           alone — which earns priority at once, since a steady stream
         *                           of ordinary work never produces the empty pool it needs
         */
        private void refused(boolean blockedOnIsolation) {
            waitingForEmptiness = blockedOnIsolation;
            countRefusal(blockedOnIsolation);
        }

        /**
         * Turned away before scanning — an exclusive task is running, or an older waiter has
         * priority. Counts toward earning priority, says nothing about the candidate. Caller
         * must hold {@link #lock}.
         */
        private void yielded() {
            countRefusal(false);
        }

        private void countRefusal(boolean wantsPriorityNow) {

            refusals++;

            if (privilegedNow || !(wantsPriorityNow || refusals >= PRIORITY_AFTER_REFUSALS)) {
                return;
            }

            number = nextTicketNumber++;
            privilegedNow = true;
            privileged.put(number, this);
        }

        /** Caller must hold {@link #lock}. A ticket that got what it wanted goes to the back. */
        private void satisfied() {

            refusals = 0;
            waitingForEmptiness = false;

            if (privilegedNow) {
                privileged.remove(number);
                privilegedNow = false;
            }
        }

        /**
         * Gives up this coordinator's place, whether or not it ever had one.
         *
         * <p>Idempotent, and the reason an abandoned batch cannot wedge the harness.</p>
         */
        @Override
        public void close() {
            lock.lock();
            try {
                satisfied();
            } finally {
                lock.unlock();
            }
        }
    }

    /**
     * A new place in the admission queue, for one coordinator's batch.
     *
     * @return the ticket; {@link Ticket#close()} it when the batch ends, abandoned or not
     */
    public Ticket newTicket() {
        return new Ticket();
    }

    /**
     * @return an immutable snapshot of the current in-flight state
     */
    public PoolSnapshot snapshot() {
        return new Snapshot(runningTasks.get(), trackedSizeBytes.get());
    }

    /**
     * @return the current state version; increments on every task completion.
     *         Capture before scanning candidates, pass to {@link #awaitStateChange}.
     */
    public long stateVersion() {
        return stateVersion;
    }

    /**
     * Offers {@code candidates} to {@code policy} in order and claims the first admitted
     * one, atomically.
     *
     * <p><b>Scanning and claiming must not be separable.</b> When several coordinators
     * share a harness — which is exactly what an executor subtree does — performing the
     * check and the claim as two steps lets every one of them observe
     * {@code runningTasks() == 0}, take the liveness floor, and start simultaneously. The
     * budget is then exceeded by a factor of the number of coordinators. Holding the lock
     * across both steps is what makes the floor mean "exactly one", rather than "one per
     * thread that happened to look at the same moment".</p>
     *
     * <p>All candidates in a pass are judged against ONE snapshot, matching
     * {@link PoolSnapshot}'s documented contract as a consistent view.</p>
     *
     * <p>{@code policy.admit} and {@code policy.onTaskStart} run under the lock, which is
     * why the policy contract requires them to be fast and side-effect-free beyond their
     * own accounting.</p>
     *
     * <p>The admitted candidate is <b>removed from {@code candidates}</b>, by identity and
     * through its iterator, so the collection must be mutable. Removing here rather than
     * letting the caller do a by-value {@code remove} afterwards is what keeps admission O(1)
     * for the common case — the front of an {@link java.util.ArrayDeque} — instead of an
     * identity scan plus array shift over the whole remaining batch per task, which made
     * large batches quadratic (measured: a 20,000-task batch ran at 60% of the
     * throughput of 1,000-task batches before this change).</p>
     *
     * <p>The scan itself is bounded the same way. It stops at the first admissible candidate
     * unless the policy {@linkplain AdmissionPolicy#mayRequireExclusive() may require
     * exclusivity}, in which case the rest of the window is walked once for an exclusive
     * candidate — the only thing that can outrank a candidate already chosen.</p>
     *
     * @param candidates mutable, in policy order; the admitted element is removed from it
     * @return the claimed task, already counted as started, or {@code null} if none was
     *         admitted
     */
    public <V> ThrottledTask<V> admitFirst(java.util.Collection<ThrottledTask<V>> candidates, AdmissionPolicy policy) {
        return admitFirst(candidates, policy, false);
    }

    /**
     * As {@link #admitFirst(java.util.Collection, AdmissionPolicy)}, for a coordinator that may be
     * running <em>inside</em> a task this harness has already claimed.
     *
     * <p>Nested submission is a first-class pattern — the executor tree and transitive
     * {@code waitForCompletion} exist for it — and it is the one shape that can deadlock the
     * throttle. The coordinator's own thread is executing a body whose claim is still held here,
     * so if the policy refuses, the only thread that could ever release that claim is the thread
     * now waiting for admission. The ordinary liveness floor does not help: it asks whether
     * <em>nothing</em> is running, and something is — the caller.</p>
     *
     * <p>Such a coordinator is therefore allowed to claim past the policy — but only
     * <b>one task at a time</b>, which is the caller's job to enforce and what
     * {@code reentrantExemption} means. The bound matters: without it the flag short-circuits
     * every scan of the batch, so a nested {@code submitThrottled} of ten thousand tasks
     * dispatches all ten thousand at once with the memory budget and the concurrency cap
     * bypassed entirely — trading a deadlock for the OOM this library exists to prevent. One at
     * a time is all deadlock-freedom needs: the coordinator can always place a task, and that
     * task's completion lets it place the next.</p>
     *
     * <p>And only <b>while the deadlock is actually there</b>. Holding a claim is necessary but
     * not sufficient: the deadlock exists when everything the policy counts in flight is itself
     * waiting to nest, so that nothing will ever finish on its own. While unrelated work is
     * running it will end by itself, and the nested coordinator waits for it like any other —
     * it is not admitted past the budget on top of it. The test is
     * {@code effectiveInFlight <= parked claims + own claims}; see {@link #parkedClaims}.
     * Without that bound, a task holding one claim could submit a thousand tasks under a rate
     * limit and have every completion re-grant the exemption, or two hundred throttled bodies
     * could each place a task past a budget that fits two.</p>
     *
     * @param candidates         mutable, in policy order; the admitted element is removed from it
     * @param policy             the admission policy
     * @param reentrantExemption {@code true} when this coordinator is inside a task claimed on
     *                           this harness <em>and</em> currently has none of its own in
     *                           flight, so admitting one more cannot run away
     * @return the claimed task, already counted as started, or {@code null} if none was admitted
     */
    public <V> ThrottledTask<V> admitFirst(java.util.Collection<ThrottledTask<V>> candidates,
            AdmissionPolicy policy, boolean reentrantExemption) {

        // A ticket that lives for one scan never accumulates refusals, so it never takes
        // priority — but it still yields to coordinators that have. Correct for a one-shot
        // caller; a coordinator running a batch should hold its own ticket across the batch.
        try (Ticket ticket = newTicket()) {
            return admitFirst(candidates, policy, ticket, reentrantExemption);
        }
    }

    /**
     * As {@link #admitFirst(java.util.Collection, AdmissionPolicy)}, but keeping this
     * coordinator's place in the admission queue across the scans of one batch.
     *
     * @param candidates mutable, in policy order; the admitted element is removed from it
     * @param policy     the admission policy
     * @param ticket     this coordinator's ticket, from {@link #newTicket()}
     * @return the claimed task, already counted as started, or {@code null} if none was admitted
     */
    public <V> ThrottledTask<V> admitFirst(java.util.Collection<ThrottledTask<V>> candidates,
            AdmissionPolicy policy, Ticket ticket) {
        return admitFirst(candidates, policy, ticket, false);
    }

    /**
     * The full form: this coordinator's place in the queue, and whether it is re-entrant.
     *
     * @param candidates mutable, in policy order; the admitted element is removed from it
     * @param policy     the admission policy
     * @param ticket     this coordinator's ticket, from {@link #newTicket()}
     * @param reentrantExemption  whether this coordinator is itself inside a task claimed on this harness
     * @return the claimed task, already counted as started, or {@code null} if none was admitted
     */
    public <V> ThrottledTask<V> admitFirst(java.util.Collection<ThrottledTask<V>> candidates,
            AdmissionPolicy policy, Ticket ticket, boolean reentrantExemption) {

        lock.lock();
        try {
            final PoolSnapshot snapshot = snapshot();

            /*
             * The liveness floor keys on the most informed count available.
             *
             * Normally that is this harness's own counter. But a policy carrying its own
             * cross-executor ledger (a shared GlobalMemoryBudgetPolicy) knows about work in
             * OTHER trees that this harness cannot see: an idle tree flooring on its local
             * zero would admit unconditionally, straight past a globally-occupied budget,
             * once per idle tree. And the reverse blindness is just as real — two
             * coordinators on one tree may run different policies, so a ledger that is
             * empty can belong to a tree that is not. When the policy reports a count, the
             * larger of the two is the informed one; deferring to the ledger ALONE let an
             * empty ledger floor-admit refused work beside tasks running under another
             * policy, waive the re-entrancy comparison, and (before the gate below grew a
             * second check) start an exclusive task on a busy tree.
             *
             * The floor's guarantee is preserved either way — when the whole system is
             * genuinely idle, both counts are zero and the next candidate is admitted, so no
             * policy bug can throttle to a standstill.
             */
            // One spelling: the -1 "no ledger" sentinel loses the max for free. Whether the
            // ledger reaches past this tree is a separate question — ledgerSpansExecutors(),
            // read in the exempt conjunct below, and only when an exemption is weighed.
            final int effectiveInFlight = Math.max(policy.inFlightTaskCount(), snapshot.runningTasks());

            /*
             * Whether this coordinator's re-entrancy claim actually earns it the exemption.
             * Scoped as the policy declares its ledger to reach (ledgerSpansExecutors): a
             * ledger spanning trees counts work whose holders may be parked on any harness;
             * a per-tree one counts only local parkers. `own > 0` is checked HERE, not left to the caller's flag:
             * the flag says "the caller believes it is nested and has nothing of its own in
             * flight"; whether this thread actually holds a claim is the harness's own knowledge,
             * and a thread holding none has no deadlock to be exempted from — whatever it
             * believes. Computed before the exclusive-holder check below, which it also waives.
             * The scope read hides behind the claim check: on the default it re-invokes
             * inFlightTaskCount(), a second ledger-lock acquisition under the admission lock,
             * and every ordinary scan would discard the answer.
             */
            final int own = claimsHeldByCurrentThread();
            final boolean exempt = reentrantExemption && own > 0
                    && effectiveInFlight <= (guardedLedgerSpansExecutors(policy)
                            ? PARKED_CLAIMS_EVERYWHERE.get() : parkedClaims) + own;

            /*
             * An exclusive task owns the whole harness while it runs. Checked before the
             * fairness yield and the scan, and before the liveness floor: the floor exists so
             * the system cannot throttle to a standstill, and it cannot here — the exclusive
             * task is running, so progress is being made and its completion will release
             * everyone. Only a genuinely exempt nested coordinator — the exclusive holder's own
             * nested work, or another holder that nothing else can release — gets past it.
             */
            if (exclusiveInFlight != null && !exempt) {
                ticket.yielded();
                return null;
            }

            /*
             * Somebody else has been waiting longer and has earned priority. Yield, and count
             * this refusal so that this coordinator eventually earns priority too — which is
             * what turns the mechanism into a queue rather than a fixed pecking order.
             *
             * Below the floor, not above it. Fairness is an ordering among coordinators that
             * could otherwise all proceed; it is not a reason for an idle system to admit nobody.
             * A privileged ticket only clears when its holder is satisfied, and a coordinator
             * whose candidate the policy will never accept is never satisfied — above the floor,
             * such a ticket held the harness against every peer indefinitely, with nothing
             * running and nothing to wait for.
             *
             * The one waiter that does outrank the floor is one blocked on isolation
             * ({@code waitingForEmptiness}): an empty pool is precisely what it is waiting for,
             * so it claims on its next scan and releases the privilege. Letting the floor admit
             * ordinary work ahead of it instead is how an exclusive task gets starved — its
             * coordinator only ever sees a pool somebody else has just refilled.
             *
             * Re-entrancy is exempt for the older reason: a coordinator that becomes privileged
             * after three refusals would otherwise block the nested submission of the very task
             * it is waiting on. Measured before that carve-out: a plain MaxConcurrencyPolicy(1)
             * and two nesting tasks wedged permanently.
             */
            final java.util.Map.Entry<Long, Ticket> oldest = privileged.firstEntry();

            /*
             * Keyed on THIS harness's count, not effectiveInFlight. Fairness is an ordering
             * among coordinators sharing this harness, and what a peer refills is this harness;
             * effectiveInFlight can be a policy's process-wide ledger, and unrelated trees
             * keeping that above zero made every local peer yield to a privileged ticket whose
             * candidate the budget would never take — a locally empty harness admitting nothing
             * for as long as those trees stayed busy. The liveness floor is the one place that
             * is rightly keyed on the wider count.
             */
            if (!exempt && oldest != null && oldest.getValue() != ticket
                    && (snapshot.runningTasks() != 0 || oldest.getValue().waitingForEmptiness)) {
                ticket.yielded();
                return null;
            }

            /*
             * One pass that only decides. Claiming from inside the scan was the bug: an ordinary
             * candidate ordered AHEAD of a blocked exclusive one was admitted and called
             * ticket.satisfied(), discarding the very privilege that makes the pool drain — the
             * old guard was a break, which only ever protected candidates ordered BEHIND it. The
             * streaming window re-sorts on every refill, so a comparator that orders by size
             * keeps pushing a small exclusive task backwards and it never gets a turn. A blocked
             * exclusive candidate anywhere in the window now refuses the whole scan; its own
             * batch waits behind it too, which is what "runs alone" has to mean.
             */
            ThrottledTask<V> chosen = null;
            boolean chosenIsExclusive = false;
            boolean chosenByFloor = false;
            boolean blockedExclusive = false;

            /*
             * Asked once per scan, so the per-candidate question need not be. The tail of the
             * window past the chosen candidate exists only to find an exclusive one; for a
             * policy that can never produce one, walking it is a requiresExclusive call per
             * remaining candidate under this lock, for nothing — quadratic in the batch for the
             * List overload, and exactly the 20,000-task throughput cliff the javadoc above
             * records fixing. A thrown answer is read as "may": that costs a scan, the other
             * mistake costs an isolation.
             */
            if (!exempt) {
                // The one lifecycle announcement: this scan is happening. Before the
                // exclusivity and admission questions below, so a policy refreshing per-scan
                // state on it (the adaptive decorator) answers them from this scan's state.
                // The floor and exemption inputs above stay live reads — they are not scan
                // state. An exempt pass asks no questions and announces nothing.
                guardedOnScanStart(policy);
            }
            final boolean scanForExclusive = !exempt && guardedMayRequireExclusive(policy);

            /*
             * Removal happens through the iterator wherever the choice is final at the point
             * it is made — O(1) at the front of a deque, which is the common case. Only a
             * candidate held while the rest of the window is walked for an exclusive one has
             * to be found again afterwards, and that walk already cost more than finding it.
             */
            boolean removed = false;

            for (java.util.Iterator<ThrottledTask<V>> it = candidates.iterator(); it.hasNext(); ) {
                final ThrottledTask<V> candidate = it.next();

                if (exempt) {
                    // Exempt re-entrant work skips both isolation and admit(): it holds a claim
                    // only it can release, nothing else could release anything, so the first
                    // candidate goes in and the scan is over.
                    chosen = candidate;
                    it.remove();
                    removed = true;
                    break;
                }

                if (scanForExclusive && guardedRequiresExclusive(candidate, policy)) {

                    // "Alone" is measured against effectiveInFlight — the max of the
                    // policy's ledger and this tree's own count, so it covers both work in
                    // other trees sharing the ledger (which admit() is never asked about
                    // here, and which the reserve would overcharge) and work on this tree
                    // under another policy the ledger never saw.
                    if (effectiveInFlight != 0) {
                        blockedExclusive = true;
                        chosen = null;
                        break;
                    }

                    // Admitted WITHOUT consulting admit(): a policy that asked for isolation
                    // should not also have to approve the task on its merits. This is the case
                    // the adaptive quarantine had to fake by hijacking the liveness floor.
                    chosen = candidate;
                    chosenIsExclusive = true;
                    // It may be REPLACING an ordinary candidate the floor was holding. That
                    // flag is about the candidate it was set for, not this one: left in place,
                    // an exclusive admission was reported as the floor overriding the policy.
                    chosenByFloor = false;
                    it.remove();
                    removed = true;
                    break;
                }

                if (chosen != null) {
                    // Already holding a candidate; still scanning only to find an exclusive one.
                    continue;
                }

                /*
                 * The policy is asked even when the floor would admit anyway, so the reported
                 * flag means what an operator reads it as: "the throttle did NOT permit this,
                 * the floor overrode it" — rather than the weaker "the floor short-circuited",
                 * which is also true of an idle pool the policy was perfectly happy with.
                 *
                 * Two consequences, both deliberate. The call is GUARDED: it used to be
                 * unreachable on an idle pool, so a policy whose admit() throws could not stop
                 * the floor; asking first would have handed that power back and broken the
                 * standing promise that no policy bug can throttle the system to a standstill.
                 * And admit() is only contractually pure — HeapPressurePolicy, in this very
                 * package, samples MXBeans and drives its hysteresis logging from it — so an
                 * otherwise-idle harness now samples on admissions it previously skipped. That
                 * shifts a sampling cadence, not a decision.
                 */
                final boolean policyAdmits = guardedAdmit(candidate, snapshot, policy);

                if (policyAdmits || effectiveInFlight == 0) {
                    // Held, not taken — the rest of the window may still hold an exclusive task.
                    chosen = candidate;
                    chosenByFloor = !policyAdmits;

                    if (!scanForExclusive) {
                        it.remove();        // and for this policy it cannot, so the scan is over
                        removed = true;
                        break;
                    }
                }
            }

            if (chosen == null) {
                /*
                 * A blocked exclusive candidate takes priority immediately rather than after
                 * three refusals: an exclusive task only ever starts at a moment of emptiness,
                 * and a steady stream of ordinary work never produces one on its own.
                 */
                ticket.refused(blockedExclusive);
                return null;
            }

            if (!removed) {
                removeByIdentity(candidates, chosen);
            }
            claimLocked(chosen, policy);

            /*
             * Re-entrancy and exclusivity are admission reasons of their own, not the floor's.
             * Reporting either as a floor admission fires the "serial batch wearing a parallel
             * batch's clothes" alarm — executors.throttle.floorAdmitted, getFloorAdmittedTotal()
             * and the JFR field all key on this — every time a task submits nested work while
             * the pool is busy, which is the common case and the throttle working as intended.
             */
            ticket.lastAdmissionByFloor = chosenByFloor;

            if (chosenIsExclusive) {
                exclusiveInFlight = chosen;
                exclusiveClaims = 1;
            } else if (chosen == exclusiveInFlight) {
                /*
                 * The re-entrant path can hand out a SECOND claim on the instance already holding
                 * the harness — an exclusive task submitting itself onward from inside its own
                 * body. exclusiveInFlight is identity-compared and onEnd decrements for every
                 * claim, so counting only in the exclusive branch (which is unreachable while
                 * something is already exclusive) meant the first end released the pool with the
                 * other claim still running.
                 */
                exclusiveClaims++;
            }

            ticket.satisfied();
            return chosen;

        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the claimed candidate — the instance, not an equal.
     *
     * <p>{@code Collection.remove(Object)} goes by {@code equals}, which today agrees with
     * identity only because {@code ThrottledTask} deliberately defines neither {@code equals}
     * nor {@code hashCode}. The executor's position bookkeeping is an {@code IdentityHashMap};
     * if the task ever grew value semantics, a by-value remove would delete a different-but-equal
     * occurrence than the one claimed and hand a future to the wrong batch position. Walking
     * the iterator keeps the common case — the front of an {@code ArrayDeque} — O(1), which is
     * the whole reason the harness removes here rather than leaving it to the caller.</p>
     */
    private static void removeByIdentity(java.util.Collection<?> candidates, Object chosen) {
        for (java.util.Iterator<?> it = candidates.iterator(); it.hasNext(); ) {
            if (it.next() == chosen) {
                it.remove();
                return;
            }
        }
    }

    /** Announces the scan; a throwing hook is logged and the scan proceeds. */
    private static void guardedOnScanStart(AdmissionPolicy policy) {
        try {
            policy.onScanStart();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            LOGGER.error("AdmissionPolicy {} threw from onScanStart — ignored.",
                    policy.getClass().getName(), hookFailure);
        }
    }

    /**
     * Reads the policy's declared ledger scope; a thrown answer is "per tree" — the side
     * that refuses an exemption rather than granting one past unrelated running work.
     */
    private static boolean guardedLedgerSpansExecutors(AdmissionPolicy policy) {
        try {
            return policy.ledgerSpansExecutors();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            LOGGER.error("AdmissionPolicy {} threw from ledgerSpansExecutors — treated as per-tree.",
                    policy.getClass().getName(), hookFailure);
            return false;
        }
    }

    /**
     * Asks the policy whether it could ever require exclusivity, treating a thrown answer as
     * "yes" — the side that costs a scan rather than an isolation.
     */
    private static boolean guardedMayRequireExclusive(AdmissionPolicy policy) {
        try {
            return policy.mayRequireExclusive();
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            // Said out loud, like every other guarded hook: silently paying the full
            // per-candidate scan on every pass leaves nothing in the log explaining why.
            LOGGER.error("AdmissionPolicy {} threw from mayRequireExclusive — treated as \"may\" (full scans).",
                    policy.getClass().getName(), hookFailure);
            return true;
        }
    }

    /** Counter and hook updates for a claim. Caller must hold {@link #lock}. */
    private void claimLocked(ThrottledTask<?> task, AdmissionPolicy policy) {
        runningTasks.incrementAndGet();
        trackedSizeBytes.addAndGet(task.size);
        guardedOnTaskStart(task, policy);
    }

    /**
     * Rate limit in front of one "a policy hook threw" message.
     *
     * <p>{@code admit} is consulted on every candidate of every scan, and a refused coordinator
     * re-scans on every completion, so a policy whose {@code admit} throws once throws on a spin
     * loop: one ERROR line with a full stack trace per candidate per scan, thousands per second
     * under load, burying the very first one that said what broke. A throwing hook is a policy
     * bug the operator has to see — once, with the stack trace, and then a periodic reminder
     * carrying the count rather than the trace.</p>
     *
     * <p>One instance per hook site <em>per policy class</em> per harness — see
     * {@link #failureLog}. Per harness alone was not enough: two coordinators on one tree may
     * run different policies, and once one policy's {@code admit} threw on a spin loop it held
     * the hook's slot for a minute at a time. A second policy's first-ever failure inside that
     * minute was dropped with no message, no class name and no stack trace, its count merged
     * into the other policy's "more like it" tail — the opposite of what this exists for.</p>
     */
    static final class HookFailureLog {

        private final long quietPeriodNanos;
        private final java.util.function.LongSupplier clock;

        private long lastLoggedNanos;
        private boolean everLogged;
        private long suppressed;

        HookFailureLog(long quietPeriodNanos, java.util.function.LongSupplier clock) {
            this.quietPeriodNanos = quietPeriodNanos;
            this.clock = clock;
        }

        HookFailureLog() {
            this(java.util.concurrent.TimeUnit.MINUTES.toNanos(1), System::nanoTime);
        }

        /**
         * Records a failure and decides whether it is this one's turn to be logged.
         *
         * @return the number of failures suppressed since the last logged one — {@code 0} for
         *         the first ever — or a negative value to stay quiet
         */
        synchronized long claim() {

            final long now = clock.getAsLong();

            if (everLogged && now - lastLoggedNanos < quietPeriodNanos) {
                suppressed++;
                return -1;
            }

            everLogged = true;
            lastLoggedNanos = now;

            final long sinceLastLine = suppressed;
            suppressed = 0;
            return sinceLastLine;
        }
    }

    /**
     * Renders the "and {@code n} more like it" tail, empty when there were none.
     */
    private static String alsoSuppressed(long suppressed) {
        return suppressed == 0 ? "" : " (" + suppressed + " more like it went unlogged)";
    }

    /** The four guarded policy calls, each with its own failure log. */
    enum Hook {
        ADMIT, REQUIRES_EXCLUSIVE, ON_TASK_START, ON_TASK_END
    }

    /**
     * Failure logs keyed by policy <em>class</em>, one per {@link Hook} each.
     *
     * <p>Class, not instance, and deliberately: policy instances are frequently one per
     * {@code submitThrottled} call, so an instance-keyed map would grow without bound for the
     * life of the tree (or need weak keys, which reintroduce the "slot held by a stranger"
     * problem through a different door). The number of policy <em>classes</em> is small and
     * fixed, the message names the class, and the stack trace is the same code path — so two
     * instances of one class sharing a log lose nothing an operator could act on.</p>
     */
    private final java.util.concurrent.ConcurrentHashMap<Class<?>, HookFailureLog[]> hookFailures =
            new java.util.concurrent.ConcurrentHashMap<>();

    /**
     * The failure log for {@code hook} on {@code policy}'s class, created on first use.
     *
     * @param hook   the guarded call that failed
     * @param policy the policy that failed it
     * @return the rate limit that decides whether this failure is logged
     */
    HookFailureLog failureLog(Hook hook, AdmissionPolicy policy) {
        return hookFailures.computeIfAbsent(policy.getClass(), unused -> {
            final HookFailureLog[] perHook = new HookFailureLog[Hook.values().length];
            for (int i = 0; i < perHook.length; i++) {
                perHook[i] = new HookFailureLog();
            }
            return perHook;
        })[hook.ordinal()];
    }

    /**
     * Asks the policy whether to admit {@code candidate}, treating a thrown answer as "no".
     *
     * <p>The liveness floor is evaluated after this call, so a refusal here — thrown or
     * returned — never prevents an idle harness from starting work. That is what keeps
     * "no policy bug can throttle the system to a standstill" true now that {@code admit} is
     * consulted before the floor rather than after it.</p>
     */
    private boolean guardedAdmit(ThrottledTask<?> candidate, PoolSnapshot snapshot, AdmissionPolicy policy) {
        try {
            return policy.admit(candidate, snapshot);
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            final long suppressed = failureLog(Hook.ADMIT, policy).claim();

            if (suppressed >= 0) {
                LOGGER.error("AdmissionPolicy {} threw from admit for {} — treated as a refusal.{}",
                        policy.getClass().getName(), candidate, alsoSuppressed(suppressed), hookFailure);
            }
            return false;
        }
    }

    /**
     * Asks the policy whether {@code task} needs isolation, treating a thrown answer as "no".
     *
     * <p>Guarded like the lifecycle hooks, and for a sharper reason: this runs inside the scan
     * loop with the harness lock held, so an escaping exception would leave the coordinator
     * mid-scan with the lock unwound and the batch abandoned. Refusing isolation is the safe
     * reading of a policy that cannot answer — the task still goes through {@code admit}.</p>
     */
    private boolean guardedRequiresExclusive(ThrottledTask<?> task, AdmissionPolicy policy) {
        try {
            return policy.requiresExclusive(task);
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            final long suppressed = failureLog(Hook.REQUIRES_EXCLUSIVE, policy).claim();

            if (suppressed >= 0) {
                LOGGER.error("AdmissionPolicy {} threw from requiresExclusive for {} — treated as not exclusive.{}",
                        policy.getClass().getName(), task, alsoSuppressed(suppressed), hookFailure);
            }
            return false;
        }
    }

    /**
     * Invokes {@code policy.onTaskStart} so that a throwing hook cannot corrupt the harness.
     *
     * <p>The counters are incremented BEFORE the hook runs. If the hook's exception were
     * allowed to escape here it would propagate out of {@code admitFirst} with the claim
     * already counted and no matching {@code onEnd} ever coming — a permanent budget leak
     * for the life of the executor tree. A policy hook that throws is a policy bug; it is
     * logged and the claim stands.</p>
     */
    private void guardedOnTaskStart(ThrottledTask<?> task, AdmissionPolicy policy) {
        try {
            policy.onTaskStart(task);
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            final long suppressed = failureLog(Hook.ON_TASK_START, policy).claim();

            if (suppressed >= 0) {
                LOGGER.error("AdmissionPolicy {} threw from onTaskStart for {} — ignored; the claim stands.{}",
                        policy.getClass().getName(), task, alsoSuppressed(suppressed), hookFailure);
            }
        }
    }

    private void guardedOnTaskEnd(ThrottledTask<?> task, AdmissionPolicy policy) {
        try {
            policy.onTaskEnd(task);
        } catch (Throwable hookFailure) {
            rethrowIfFatal(hookFailure);
            final long suppressed = failureLog(Hook.ON_TASK_END, policy).claim();

            if (suppressed >= 0) {
                LOGGER.error("AdmissionPolicy {} threw from onTaskEnd for {} — ignored.{}",
                        policy.getClass().getName(), task, alsoSuppressed(suppressed), hookFailure);
            }
        }
    }

    /** A hook that runs the JVM out of memory or stack is not something to log and carry on from. */
    private static void rethrowIfFatal(Throwable t) {
        if (t instanceof VirtualMachineError vmError) {
            throw vmError;
        }
    }

    /**
     * Registers a task completion: updates counters, invokes
     * {@code policy.onTaskEnd(task)}, then signals all waiting coordinators
     * unconditionally — even if the policy hook throws.
     */
    public void onEnd(ThrottledTask<?> task, AdmissionPolicy policy) {
        /*
         * Counters are released UNDER THE LOCK, matching admitFirst. If a release could
         * interleave with an admission scan, a coordinator could judge candidates against a
         * snapshot that was already stale by the time it claimed one.
         */
        lock.lock();
        try {
            runningTasks.decrementAndGet();
            trackedSizeBytes.addAndGet(-task.size);

            // Identity, not equality: the holder is one specific instance — and it is released
            // only when its LAST claim ends, not its first.
            if (exclusiveInFlight == task && --exclusiveClaims <= 0) {
                exclusiveInFlight = null;
                exclusiveClaims = 0;
            }

            try {
                // Contained, not merely survived: the old finally released the counters and
                // signalled but still re-threw, into a discarded whenComplete stage where the
                // exception silently vanished.
                guardedOnTaskEnd(task, policy);
            } finally {
                stateVersion++;
                stateChanged.signalAll();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * Blocks until the state version advances past {@code observedVersion},
     * the timeout elapses, or the thread is interrupted. Returns immediately
     * if the version already advanced — no signal can be lost between a
     * candidate scan and this call.
     *
     * @param observedVersion the version captured before the failed scan
     * @param timeoutMillis   upper bound on the wait (self-heal / poll interval)
     * @throws InterruptedException if the coordinator thread is interrupted
     */
    public void awaitStateChange(long observedVersion, long timeoutMillis) throws InterruptedException {
        awaitStateChange(observedVersion, timeoutMillis, false);
    }

    /**
     * As {@link #awaitStateChange(long, long)}, declaring whether the waiting coordinator is
     * itself running inside a throttled task — see {@link #parkedClaims}.
     *
     * @param observedVersion the version captured before the scan that found nothing
     * @param timeoutMillis   the poll interval
     * @param holdsClaim      whether this coordinator's thread holds throttle claims of its own
     *                        (all of them are counted as parked while it waits)
     * @throws InterruptedException if interrupted while waiting
     */
    public void awaitStateChange(long observedVersion, long timeoutMillis, boolean holdsClaim)
            throws InterruptedException {
        lock.lock();
        try {
            if (stateVersion != observedVersion) {
                return;
            }
            final int own = holdsClaim ? claimsHeldByCurrentThread() : 0;
            parkedClaims += own;
            PARKED_CLAIMS_EVERYWHERE.addAndGet(own);
            try {
                stateChanged.await(timeoutMillis, TimeUnit.MILLISECONDS);
            } finally {
                parkedClaims -= own;
                PARKED_CLAIMS_EVERYWHERE.addAndGet(-own);
            }
        } finally {
            lock.unlock();
        }
    }
}
