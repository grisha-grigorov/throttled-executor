package io.github.grishagrigorov.executors.throttling;

import java.util.Comparator;
import java.util.Objects;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * An {@link AdmissionPolicy} that forwards <em>everything</em> to a delegate — the base for
 * decorator authors.
 *
 * <p>The interface's {@code default} methods answer for the object they are called on, which
 * inside a wrapper is the wrong object: {@link #inFlightTaskCount()} reports "no ledger" for
 * a delegate that has one, {@link #mayRequireExclusive()} reflects on the wrapper's own class
 * and hides the delegate's isolation, {@link #onScanStart()} swallows the announcement a
 * wrapped policy hangs per-scan refreshes on, and {@link #ledgerSpansExecutors()} rederives
 * the parked-claims scope from the wrapper instead of honouring the delegate's word. A
 * pass-through built directly on the interface therefore degrades the moment the SPI grows —
 * which it has, twice, and both times the missing forward was found in a wrapper written by
 * this library's own authors.</p>
 *
 * <p>Extend this class instead and override only what the decorator changes:</p>
 *
 * <pre>{@code
 * final AdmissionPolicy logged = new ForwardingAdmissionPolicy(inner) {
 *     @Override
 *     public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
 *         LOG.debug("asked about {}", candidate.name);
 *         return super.admit(candidate, snapshot);
 *     }
 * };
 * }</pre>
 *
 * <p>Everything not overridden reaches the delegate untouched, including methods added to
 * the SPI after the wrapper was written — a new {@code AdmissionPolicy} method must land
 * here with an override (a reflective test refuses the build without one; the forwarding
 * behaviour itself is pinned by the behavioural tests beside it), so existing subclasses
 * stay correct without hearing about the change.</p>
 *
 * <p>One answer is <em>derived, not forwarded,</em> the moment its partner is overridden:
 * a subclass declaring its own {@link #requiresExclusive} answers
 * {@link #mayRequireExclusive()} with "may" whatever the delegate says — its isolation
 * must be scanned for, and a needless scan is the cheap failure side. The ledger pair is
 * deliberately not treated that way: {@link #ledgerSpansExecutors()} stays the delegate's
 * explicit word, because re-deriving it from an overridden {@link #inFlightTaskCount()}
 * would widen a per-tree delegate to process-wide — the permissive side. A subclass
 * reporting a ledger of its own overrides that pair together. Overriding the derived
 * member itself always wins — the base only chooses the default.</p>
 */
public abstract class ForwardingAdmissionPolicy implements AdmissionPolicy {

    /**
     * Whether the runtime class declares {@code requiresExclusive} below this base — i.e.
     * the subclass added isolation logic of its own. The forwarded delegate answer to
     * {@link #mayRequireExclusive()} is exact only while {@code requiresExclusive} is
     * still the forwarded one; once the subclass overrides it, "may" has to be re-derived
     * from that fact, exactly as the interface default derives it from an override of the
     * bare interface. The same probe as the interface default's, against a different base.
     */
    private static final ClassValue<Boolean> OWN_ISOLATION =
            ExclusivityOverride.declaresBelow(ForwardingAdmissionPolicy.class, "requiresExclusive", ThrottledTask.class);

    /** The policy every non-overridden call is forwarded to. */
    protected final AdmissionPolicy delegate;

    /**
     * @param delegate the policy to forward to, not {@code null}
     */
    protected ForwardingAdmissionPolicy(AdmissionPolicy delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate");
    }

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        return delegate.admit(candidate, snapshot);
    }

    @Override
    public void onTaskStart(ThrottledTask<?> task) {
        delegate.onTaskStart(task);
    }

    @Override
    public void onTaskEnd(ThrottledTask<?> task) {
        delegate.onTaskEnd(task);
    }

    @Override
    public void onScanStart() {
        delegate.onScanStart();
    }

    @Override
    public Comparator<ThrottledTask<?>> selectionOrder() {
        return delegate.selectionOrder();
    }

    @Override
    public int inFlightTaskCount() {
        return delegate.inFlightTaskCount();
    }

    @Override
    public boolean requiresExclusive(ThrottledTask<?> task) {
        return delegate.requiresExclusive(task);
    }

    /**
     * The delegate's answer — unless the subclass declared a {@code requiresExclusive} of
     * its own, which makes the answer "may" whatever the delegate says: the harness would
     * otherwise skip the per-candidate question entirely and the subclass's isolation
     * would vanish with nothing logged. The needless-scan side is the cheap one, exactly
     * as for the interface's derived default.
     */
    @Override
    public boolean mayRequireExclusive() {
        return OWN_ISOLATION.get(getClass()) || delegate.mayRequireExclusive();
    }

    /**
     * The delegate's explicit word, always — never re-derived from the count. "Spans" is
     * the permissive side (an exemption granted against parkers on other trees), and a
     * delegate that reports a count yet answers per-tree must keep that answer through any
     * wrapper, including one that overrides {@link #inFlightTaskCount()} only to observe
     * it. A subclass that reports a ledger of its own overrides this together with the
     * count; the base does not guess.
     */
    @Override
    public boolean ledgerSpansExecutors() {
        return delegate.ledgerSpansExecutors();
    }
}
