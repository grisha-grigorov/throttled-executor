package io.github.grishagrigorov.executors;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.spi.TaskOutcome;
import io.github.grishagrigorov.executors.spi.TaskStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Cancels the whole subtree on the first task failure.
 *
 * <p>The default is to run every task and report all the failures together, which is right when
 * the tasks are independent. It is wrong when they are not: if the database has gone away there
 * is no reason to build nine hundred more reports and then report nine hundred failures.</p>
 *
 * <pre>{@code
 * var root = new ThrottledExecutor("NightlyReports");
 * root.addListener(new FailFastListener());
 *
 * root.submitThrottled(reports);
 * root.waitForCompletion();     // still throws the aggregate — with far fewer failures in it
 * }</pre>
 *
 * <p>Opt-in only, one instance per root — never a built-in. Two things to know before adding
 * it:</p>
 *
 * <ul>
 *   <li><b>It closes the executor.</b> {@code cancelSubtree()} closes the tree as well as
 *       cancelling it, so the instance cannot be reused for another batch. That is the point,
 *       but it means a long-lived shared executor is the wrong place for this.</li>
 *   <li><b>It must be cheap to re-enter.</b> Cancelling a queued task runs every listener's
 *       {@code onComplete} synchronously on the cancelling thread, so this listener is called
 *       again, once per task it just cancelled. The guard below makes those calls no-ops;
 *       adding this alongside expensive listeners still multiplies their cost by the number of
 *       cancelled tasks.</li>
 * </ul>
 *
 * <p>Only {@link TaskStatus#FAILED} triggers it. A cancelled task is not a reason to cancel —
 * that is how a single failure would otherwise cascade into cancelling unrelated siblings, and
 * it is also what keeps this listener from re-triggering on its own cancellations.</p>
 */
public final class FailFastListener implements TaskExecutionListener<Void> {

    private static final Logger LOGGER = LoggerFactory.getLogger(FailFastListener.class);

    private final AtomicBoolean triggered = new AtomicBoolean();
    private final AtomicLong cancellations = new AtomicLong();

    @Override
    public void onComplete(TaskDescriptor task, Void state, TaskOutcome outcome) {

        if (outcome.status() != TaskStatus.FAILED) {
            return;
        }

        // compareAndSet, not get-then-set: two workers can fail at the same moment, and the
        // second must not start a cancellation while the first is still walking the tree.
        if (!triggered.compareAndSet(false, true)) {
            return;
        }

        cancellations.incrementAndGet();

        LOGGER.warn("executors: {} failed — cancelling {}; the rest of the batch will not run.",
                task.displayName(), task.executorPath(), outcome.failure());

        task.executor().root().cancelSubtree();
    }

    /** @return whether a failure has already tripped this listener */
    public boolean triggered() {
        return triggered.get();
    }

    /**
     * How many times this listener has actually cancelled — at most one.
     *
     * <p>Exposed because "exactly one" is the property worth checking: the cancellation
     * re-enters this listener once per task it takes down.</p>
     *
     * @return the count
     */
    public long cancellations() {
        return cancellations.get();
    }
}
