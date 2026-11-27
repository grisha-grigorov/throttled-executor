package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;
import java.util.Comparator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AND-combinator over multiple {@link AdmissionPolicy} instances: a candidate
 * is admitted only if <b>every</b> member policy admits it. Evaluation
 * short-circuits on the first refusal.
 *
 * <p>{@link #selectionOrder()} delegates to the <i>first</i> member;
 * {@link #onTaskStart}/{@link #onTaskEnd} fan out to all members.</p>
 *
 * <pre>{@code
 * AdmissionPolicy policy = CompositePolicy.allOf(
 *         new MemoryBudgetPolicy(2_000_000_000L, 1.0),
 *         new MaxConcurrencyPolicy(8));
 * }</pre>
 */
public final class CompositePolicy implements AdmissionPolicy {

    private static final Logger LOGGER = LoggerFactory.getLogger(CompositePolicy.class);

    private final List<AdmissionPolicy> members;

    private CompositePolicy(List<AdmissionPolicy> members) {
        this.members = members;
    }

    /**
     * @param policies member policies, evaluated in the given order
     * @return a policy admitting only what every member admits
     * @throws IllegalArgumentException if no policies are given
     */
    public static CompositePolicy allOf(AdmissionPolicy... policies) {
        if (policies == null || policies.length == 0) {
            throw new IllegalArgumentException("CompositePolicy requires at least one member policy");
        }
        return new CompositePolicy(List.of(policies));
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        for (AdmissionPolicy member : members) {
            if (!member.admit(candidate, snapshot)) {
                return false; // short-circuit on the first refusal
            }
        }
        return true;
    }

    /*
     * Fan-out is guarded PER MEMBER. One throwing member must not stop the hook reaching
     * the members after it — otherwise a bug in one policy silently desynchronises every
     * other policy's accounting (a ledger that saw onTaskStart but never onTaskEnd, say).
     */

    @Override
    public void onTaskStart(ThrottledTask<?> task) {
        for (AdmissionPolicy member : members) {
            try {
                member.onTaskStart(task);
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from onTaskStart for {} — ignored.",
                        member.getClass().getName(), task, hookFailure);
            }
        }
    }

    @Override
    public void onTaskEnd(ThrottledTask<?> task) {
        for (AdmissionPolicy member : members) {
            try {
                member.onTaskEnd(task);
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from onTaskEnd for {} — ignored.",
                        member.getClass().getName(), task, hookFailure);
            }
        }
    }

    private static void rethrowIfFatal(Throwable t) {
        if (t instanceof VirtualMachineError vmError) {
            throw vmError;
        }
    }

    @Override
    public Comparator<ThrottledTask<?>> selectionOrder() {
        // get(0), not getFirst(): SequencedCollection is Java 21 and this module's floor is 17.
        return members.get(0).selectionOrder();
    }

    /**
     * The maximum of the members' own counts, or {@code -1} when no member tracks any.
     *
     * <p>Maximum, because the floor must defer to the <em>most informed</em> member: if
     * any member's ledger says work is in flight somewhere, unconditional floor admission
     * is not safe, whatever the less-informed members think.</p>
     */
    @Override
    public int inFlightTaskCount() {
        int max = -1;
        for (AdmissionPolicy member : members) {
            max = Math.max(max, member.inFlightTaskCount());
        }
        return max;
    }

    /**
     * Exclusive if <b>any</b> member says so.
     *
     * <p>OR, not AND, and for the same reason {@code admit} is AND: this composite is a
     * conjunction of constraints, and "must run alone" is a constraint. A member that needs
     * isolation cannot have it voted away by members that do not care.</p>
     */
    @Override
    public boolean requiresExclusive(ThrottledTask<?> task) {
        for (AdmissionPolicy member : members) {
            /*
             * Guarded per member like the hooks above, and for a sharper reason than
             * accounting: an unguarded fan-out that a buggy member aborted was read by the
             * harness as "the whole composite says no", so the quarantine policy standing
             * BEHIND the buggy one was never asked and its task ran beside everything. A member
             * that cannot answer is treated as not requiring isolation — the others still are.
             */
            try {
                if (member.requiresExclusive(task)) {
                    return true;
                }
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from requiresExclusive for {} — treated as not exclusive.",
                        member.getClass().getName(), task, hookFailure);
            }
        }
        return false;
    }

    /**
     * May, if <b>any</b> member may. Guarded the same way, and a member that cannot answer is
     * read as "may" — the side that costs a scan rather than an isolation. A pure query
     * again (per-scan refreshes ride on {@link #onScanStart()} now), so it short-circuits:
     * once one member may isolate the composite's answer is settled.
     */
    @Override
    public boolean mayRequireExclusive() {
        for (AdmissionPolicy member : members) {
            try {
                if (member.mayRequireExclusive()) {
                    return true;
                }
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from mayRequireExclusive — treated as \"may\" (full scans).",
                        member.getClass().getName(), hookFailure);
                return true;
            }
        }
        return false;
    }

    /**
     * Announced to <b>every</b> member, unconditionally — a lifecycle hook is not a query:
     * a member whose per-scan refresh were skipped would act on stale state.
     */
    @Override
    public void onScanStart() {
        for (AdmissionPolicy member : members) {
            try {
                member.onScanStart();
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from onScanStart — ignored.",
                        member.getClass().getName(), hookFailure);
            }
        }
    }

    /**
     * Spans, if <b>any</b> member's ledger spans — the widest member decides whose parked
     * claims the exemption compares against. A member that cannot answer is read as
     * per-tree, the side that refuses an exemption rather than granting one.
     *
     * <p>Note the pairing this buys: the composite's {@link #inFlightTaskCount()} is the max
     * over members, so a per-tree member's count can end up compared against a spanning
     * sibling's parked scope. Tasks admitted through this composite charge every member's
     * ledger together, which keeps count and scope aligned; a member whose ledger is also
     * fed from outside the composite desyncs them — prefer giving such a member its own
     * root.</p>
     */
    @Override
    public boolean ledgerSpansExecutors() {
        for (AdmissionPolicy member : members) {
            try {
                if (member.ledgerSpansExecutors()) {
                    return true;
                }
            } catch (Throwable hookFailure) {
                rethrowIfFatal(hookFailure);
                LOGGER.error("Member policy {} threw from ledgerSpansExecutors — treated as per-tree.",
                        member.getClass().getName(), hookFailure);
            }
        }
        return false;
    }
}
