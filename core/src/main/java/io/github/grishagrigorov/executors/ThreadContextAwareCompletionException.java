package io.github.grishagrigorov.executors;

import java.util.Map;
import java.util.concurrent.CompletionException;
import org.slf4j.MDC;

/**
 * A {@link CompletionException} subclass that captures the SLF4J
 * {@link MDC} context map at construction time and can bundle
 * multiple {@link Throwable}s from parallel tasks into a single
 * exception via {@link #aggregationOfThrowables}.
 *
 * <p>Stack traces are cleared ({@link #EMPTY_STACK_TRACE}) because
 * the interesting traces belong to the wrapped cause(s), not this
 * wrapper itself.</p>
 *
 * @see ThrottledExecutor#waitForCompletion()
 */
public class ThreadContextAwareCompletionException extends CompletionException {

    public static final StackTraceElement[] EMPTY_STACK_TRACE = new StackTraceElement[0];
    public final Map<String, String> mdcConextMapSave;
    public final Throwable[] aggregationOfThrowables;

    public String executorId = null;

    /**
     * Where in the executor tree the failure happened, root first
     * ({@code Root → Leg-3 → FuelCalc-12}), or just the executor id for a root.
     *
     * <p>An id alone says which executor failed but not which branch of a deep tree it
     * belonged to, which is the part that usually identifies the work.</p>
     */
    private String executorPath = null;

    /** @return the root-first path of executor ids leading to the failure */
    public String executorPath() {
        return executorPath != null ? executorPath : executorId;
    }

    /** Package-private: set by the executor once it knows its position in the tree. */
    void executorPath(String executorPath) {
        this.executorPath = executorPath;
    }

    private ThreadContextAwareCompletionException(Throwable cause) {
        super(cause);
        mdcConextMapSave = MDC.getCopyOfContextMap();
        setStackTrace(EMPTY_STACK_TRACE);
        this.aggregationOfThrowables = null;
    }

    public ThreadContextAwareCompletionException(String executorId, Throwable cause) {
        super(cause);
        setStackTrace(EMPTY_STACK_TRACE);
        this.aggregationOfThrowables = new Throwable[]{cause};
        this.mdcConextMapSave = MDC.getCopyOfContextMap();
        this.executorId = executorId;
    }

    private ThreadContextAwareCompletionException(String message) {
        super(message);
        mdcConextMapSave = MDC.getCopyOfContextMap();
        setStackTrace(EMPTY_STACK_TRACE);
        this.aggregationOfThrowables = null;
    }

    private ThreadContextAwareCompletionException() {
        mdcConextMapSave = MDC.getCopyOfContextMap();
        setStackTrace(EMPTY_STACK_TRACE);
        this.aggregationOfThrowables = null;
    }

    /**
     * Bundles several task failures into one exception.
     *
     * <p>The first failure is also set as the {@linkplain #getCause() cause}, so ordinary
     * cause-chain walking — {@code getRootCause()}, AssertJ's {@code rootCause()}, a
     * logger printing the stack trace — still reaches a real underlying failure. Without
     * it an aggregate reports no cause at all and the chain dead-ends here. The complete
     * set is always available via {@link #aggregationOfThrowables}.</p>
     */
    public ThreadContextAwareCompletionException(String executorId, Throwable[] toArray) {
        super(toArray != null && toArray.length > 0 ? toArray[0] : null);
        this.setStackTrace(EMPTY_STACK_TRACE);
        this.aggregationOfThrowables = toArray;
        this.mdcConextMapSave = MDC.getCopyOfContextMap();
        this.executorId = executorId;

        /*
         * Attach every failure beyond the first as a suppressed exception.
         *
         * aggregationOfThrowables was previously never rendered anywhere: a batch of fifty
         * distinct failures printed one cause and silently dropped the rest, so the bundling
         * existed but bought nothing at the point it mattered. Suppressed exceptions are
         * printed by the standard stack-trace machinery, so ordinary logging now shows them
         * all with no caller changes.
         */
        if (toArray != null) {
            for (int i = 1; i < toArray.length; i++) {
                if (toArray[i] != null && toArray[i] != this) {
                    addSuppressed(toArray[i]);
                }
            }
        }
    }



    /**
     * Prefixes the tree path and failure count onto the message.
     *
     * <p>The stack trace is deliberately empty on this wrapper, so without this the
     * rendered exception showed only the cause's own message — the executor id never
     * appeared anywhere a reader would see it, let alone the path or how many tasks
     * actually failed.</p>
     */
    @Override
    public String getMessage() {

        final StringBuilder message = new StringBuilder();

        if (executorPath() != null) {
            message.append(executorPath());
        }

        if (aggregationOfThrowables != null && aggregationOfThrowables.length > 1) {
            message.append(" (").append(aggregationOfThrowables.length).append(" failures)");
        }

        final String inherited = super.getMessage();

        if (inherited != null && !inherited.isEmpty()) {
            if (message.length() > 0) {
                message.append(": ");
            }
            message.append(inherited);
        }

        return message.length() > 0 ? message.toString() : inherited;
    }

    @Override
    public String toString() {
        return super.toString();
    }


    @Override
    public StackTraceElement[] getStackTrace() {
        return super.getStackTrace();
    }


}
