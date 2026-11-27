package io.github.grishagrigorov.executors;


import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.CompositePolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.MemoryBudgetPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * JUnit 5 tests pinning the 2026-08 hardening fixes for the executors
 * library: {@link Supplier} dispatch through the throttled submission path,
 * interrupt-flag hygiene in {@code waitForCompletion}, cross-thread close
 * visibility, and caller-list immutability.
 *
 * <p>Throttle-internal behaviour (atomic accounting, multiplier consistency,
 * interrupt handling while blocked, completion signalling) is covered by
 * {@code ThrottlePolicyTests} against the pluggable policy API.</p>
 *
 * @see ThrottledExecutor
 * @see AdmissionPolicy
 */
public class ThrottledExecutorHardeningTests {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottledExecutorHardeningTests.class);

    @BeforeAll
    public static void runBeforeAll() {
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
        System.setProperty("isThreadContextMapInheritable", "true");
    }

    private static AdmissionPolicy relaxedPolicy() {
        return CompositePolicy.allOf(
                new MemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0),
                new MaxConcurrencyPolicy(10));
    }

    /**
     * A {@link Supplier} job — supported by the plain {@code submit()} overloads —
     * must also be dispatchable through the throttled path instead of blowing up
     * with {@code RuntimeException("Unhandled job")}.
     */
    @Test
    public void supplierJobsShouldRunThroughTheThrottledPath() {

        ThrottledExecutor pr = new ThrottledExecutor("SupplierDispatch");

        Supplier<String> supplier = () -> "supplied-value";

        List<ThrottledTask<Supplier<String>>> jobs = new ArrayList<>();
        jobs.add(new ThrottledTask<>(supplier, 1_000_000L, "SupplierTask"));

        Assertions.assertThatCode(() -> {
            pr.submitThrottled(jobs, relaxedPolicy());
            pr.waitForCompletion();
        }).doesNotThrowAnyException();

        CompletableFuture<?> future = pr.getFutures().toList().getFirst().getCompletableFuture();
        Assertions.assertThat(future.join()).isEqualTo("supplied-value");
    }

    /**
     * A batch that merely FAILS must not leave the calling thread's interrupt
     * flag set — task failures are not interruptions.
     */
    @Test
    public void failedBatchShouldNotInterruptTheCallingThread() {

        try {
            ThrottledExecutor pr = new ThrottledExecutor("InterruptFlag");

            pr.submit((Runnable) () -> {
                throw new RuntimeException("Deliberate failure");
            });

            try {
                pr.waitForCompletion();
            } catch (Throwable expected) {
                LOGGER.info("Expected batch failure: {}", expected.toString());
            }

            Assertions.assertThat(Thread.currentThread().isInterrupted())
                    .as("a failed batch is not an interruption — the caller's interrupt flag must stay clear")
                    .isFalse();
        } finally {
            Thread.interrupted(); // test hygiene: clear the flag for subsequent tests
        }
    }

    /**
     * close() performed on one thread must reject a subsequent submit() from
     * another thread. (Pins the intended behaviour; the volatile visibility
     * itself cannot be asserted deterministically.)
     */
    @Test
    public void closeOnOneThreadShouldRejectSubmitFromAnother() throws Exception {

        ThrottledExecutor pr = new ThrottledExecutor("CloseRace");

        Thread closer = new Thread(pr::close, "closer");
        closer.start();
        closer.join(TimeUnit.SECONDS.toMillis(10));

        CompletableFuture<Void> rejected = pr.submit((Runnable) () ->
                System.out.println("This must never run — the executor is closed."));

        Assertions.assertThat(rejected.isCompletedExceptionally()).isTrue();
    }

    /**
     * submitThrottled must not mutate the caller's list — an immutable
     * List.of(...) must work, and a mutable list must keep its contents
     * after the call.
     */
    @Test
    public void throttledSubmissionShouldNotMutateTheCallersList() {

        ThrottledExecutor pr = new ThrottledExecutor("CallerList");

        // An immutable list must be accepted (a shared-list sort would throw UnsupportedOperationException).
        List<ThrottledTask<Runnable>> immutableJobs = List.of(
                new ThrottledTask<>(() -> { }, 1_000L, "ImmutableTask"));

        Assertions.assertThatCode(() -> {
            pr.submitThrottled(immutableJobs, relaxedPolicy());
            pr.waitForCompletion();
        }).doesNotThrowAnyException();

        // A mutable list must keep its contents (a shared list would be sorted and drained to empty).
        ThrottledExecutor pr2 = new ThrottledExecutor("CallerList2");
        List<ThrottledTask<Runnable>> mutableJobs = new ArrayList<>();
        mutableJobs.add(new ThrottledTask<>(() -> { }, 2_000L, "KeepMe1"));
        mutableJobs.add(new ThrottledTask<>(() -> { }, 1_000L, "KeepMe2"));

        pr2.submitThrottled(mutableJobs, relaxedPolicy());
        pr2.waitForCompletion();

        Assertions.assertThat(mutableJobs)
                .as("the caller's list must not be drained by the executor")
                .hasSize(2);
        Assertions.assertThat(mutableJobs.get(0).name).isEqualTo("KeepMe1");
    }
}
