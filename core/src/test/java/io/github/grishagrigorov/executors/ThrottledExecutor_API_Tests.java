package io.github.grishagrigorov.executors;


import io.github.grishagrigorov.executors.spi.TaskDescriptor;
import io.github.grishagrigorov.executors.spi.TaskExecutionListener;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.CompositePolicy;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import io.github.grishagrigorov.executors.throttling.MemoryBudgetPolicy;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.logging.log4j.LogManager;
import org.assertj.core.api.Assertions;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;


/**
 * The API-facing tests for the executors library — how a programmer is expected to
 * interact with it.
 *
 * <p>Where the other test classes pin individual guarantees (the policy SPI, the listener
 * SPI, the completion state machine, the executor tree), this one exercises the library the
 * way application code does: build an executor, submit plain and memory-aware batches,
 * nest executors inside tasks, wait for completion, deal with the aggregate failure, cancel.
 * If you want to learn the library, read this file top to bottom.</p>
 *
 * <p>It also carries the throughput measurement
 * ({@link #measureMaxSustainedThroughputOfTheThrottledPath()}): the ceiling on tasks per
 * second the throttled path can push through when the tasks themselves cost nothing —
 * i.e. the library's own per-task overhead, reported as a log table.</p>
 *
 * @see ThrottledExecutor
 * @see AdmissionPolicy
 */
public class ThrottledExecutor_API_Tests {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThrottledExecutor_API_Tests.class);


    @BeforeAll
    public static void runBeforeAll() {
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");
        System.setProperty("isThreadContextMapInheritable", "true");
    }

    @Test
    public void justRun() {



        //System.setProperty("log4j2.loggerContextSelector", "org.apache.logging.log4j.core.async.AsyncLoggerContextSelector");
        System.setProperty("log4j2.loggerContextSelector", "org.apache.logging.log4j.core.async.BasicAsyncLoggerContextSelector");

        ThrottledExecutor pr = ThrottledExecutor.build("Level_1", null);

        //List<String> taskNames = List.of("ComputeTask_1111", "ComputeTask_2222", "ComputeTask_3333");
        IntStream intStream = IntStream.range(1, 100);

        List<Runnable> jobs = new ArrayList<>();

        intStream.forEach(number -> {

            pr.submit(() -> {
                MDC.put("taskName", "task" + String.valueOf(number));
                ThrottledExecutor pr2 = ThrottledExecutor.build("Level_2", null);

                pr2.submit((Supplier<Void>) () -> {
                    ThrottledExecutor pr3 = ThrottledExecutor.build("Level_3", null); // the naming can get confusing since this level_3 name applies to more than one instance of this executor.

                    // passing in the parent executor may be the simplest solution; thus explicitly requireing the programmer to define the heirarchy.

                    pr3.submit(new Supplier<Void>() {

                        @Override
                        public Void get() {

                            String taskNameFromMADC = MDC.get("taskName");

                            Map<String, String> ctxMapCopy = MDC.getCopyOfContextMap();

                            LOGGER.info("SimplyPrintThis");

                            if ("ComputeTask_3333".equals(taskNameFromMADC)) {

                            }
                            return null;
                        }
                    });
                    pr3.waitForCompletion();

                    return null;
                });
                pr2.waitForCompletion();
            });
        });

        //  pr.submitRunnables(jobs);
        try {
            pr.waitForCompletion();
        } catch (Throwable e) {

            LOGGER.error("pr.waitForCompletion() error: ", e);
        } finally {
            LogManager.shutdown(); // flush all cached ascync calls before shutting down... shoule really be a shutdown hook that gets added.
        }
    }


    @Test
   public void nestedExecutorsShouldPropagateTheirErrorsToTheirParents() {

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertThatThrownBy(() -> {
                    ThrottledExecutor pr = new ThrottledExecutor("Level_1");

                    List<String> taskNames = List.of("ComputeTask_1111", "ComputeTask_2222", "ComputeTask_3333");

                    List<Runnable> jobs = new ArrayList<>();

                    for (String taskName : taskNames) {
                        jobs.add(() -> {
                            MDC.put("taskName", taskName);
                            ThrottledExecutor pr2 = new ThrottledExecutor("Level_2");

                            pr2.submit((Supplier<Void>) () -> {
                                ThrottledExecutor pr3 = new ThrottledExecutor("Level_3");

                                pr3.submit(new Supplier<Void>() {

                                    @Override
                                    public Void get() {

                                        String taskNameFromMADC = MDC.get("taskName");

                                        Map<String, String> ctxMapCopy = MDC.getCopyOfContextMap();

                                        if ("ComputeTask_3333".equals(taskNameFromMADC)) {
                                            throw new Error("boom!");
                                        }
                                        return null;
                                    }
                                });
                                pr3.waitForCompletion();

                                return null;
                            });
                            pr2.waitForCompletion();
                        });
                    }

                    pr.submitRunnables(jobs);
                    pr.waitForCompletion();
                }).isExactlyInstanceOf(ThreadContextAwareCompletionException.class)
        );
    }

    @Test
    public void nestedExecutorsShouldPropagateTheirExceptionsToTheirParents() {

        Awaitility.await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> Assertions.assertThatThrownBy(() -> {
                    ThrottledExecutor pr = new ThrottledExecutor("Level_1");

                    List<String> taskNames = List.of("ComputeTask_1111", "ComputeTask_2222", "ComputeTask_3333");

                    List<Runnable> jobs = new ArrayList<>();

                    for (String taskName : taskNames) {
                        jobs.add(() -> {
                            MDC.put("taskName", taskName);
                            ThrottledExecutor pr2 = new ThrottledExecutor("Level_2");

                            pr2.submit((Supplier<Void>) () -> {
                                ThrottledExecutor pr3 = new ThrottledExecutor("Level_3");

                                pr3.submit(new Supplier<Void>() {

                                    @Override
                                    public Void get() {

                                        String taskNameFromMADC = MDC.get("taskName");

                                        Map<String, String> ctxMapCopy = MDC.getCopyOfContextMap();

                                        if ("ComputeTask_3333".equals(taskNameFromMADC)) {
                                            throw new RuntimeException("Test Exception!");
                                        }
                                        return null;
                                    }
                                });
                                pr3.waitForCompletion();

                                return null;
                            });
                            pr2.waitForCompletion();
                        });
                    }

                    pr.submitRunnables(jobs);
                    pr.waitForCompletion();
                }).isExactlyInstanceOf(ThreadContextAwareCompletionException.class).rootCause().hasMessage("Test Exception!")
        );
    }

    @Test
    public void shouldExectueAtLeastOneTaskAtATimeEvenIfItExceedsMemoryLimits() {
        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertThatCode(() -> {

                List<String> taskNames = List.of("ComputeTask_1111", "ComputeTask_2222", "ComputeTask_3333");

                List<ThrottledTask<Runnable>> jobs = new ArrayList<>();

                for (String taskName : taskNames) {
                        jobs.add(new ThrottledTask<>(() -> {
                            MDC.put("taskName", taskName);
                            ThrottledExecutor pr2 = new ThrottledExecutor("Level_2");

                            pr2.submit((Supplier<Void>) () -> {
                                ThrottledExecutor pr3 = new ThrottledExecutor("Level_3");

                                pr3.submit(new Supplier<Void>() {

                                    @Override
                                    public Void get() {

                                        String taskNameFromMADC = MDC.get("taskName");

                                        Map<String, String> ctxMapCopy = MDC.getCopyOfContextMap();

                                        return null;
                                    }
                                });
                                pr3.waitForCompletion();

                                return null;
                            });

                            pr2.waitForCompletion();

                            System.out.println(taskName);

                        }, Long.MAX_VALUE, taskName));
                    }

                //Task that the max value is set to some big number.
                Assertions.assertThat(jobs.get(0).size).isEqualTo(Long.MAX_VALUE);

                AdmissionPolicy loadThrottle = CompositePolicy.allOf(
                        new MemoryBudgetPolicy((long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0),
                        new MaxConcurrencyPolicy(2));
                    pr.submitThrottled(jobs, loadThrottle);
                    pr.waitForCompletion();
            }).doesNotThrowAnyException();

            //Ensure that at least one task always runs. The purpose of this pool isn't to block computation. This is a falesafe.
            Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();
        });
    }

    @Test
    public void shouldThrottleDownBasedOnMemEstimates() {
        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        List<String> taskNames = List.of("BigTask", "MediumTask", "SmallTask");

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertThatCode(() -> {

                /*
                 * Records ADMISSION order, which is what the assertion below is about, rather
                 * than the order the bodies happened to reach an append.
                 *
                 * It used to be the latter: a plain StringBuilder appended from inside the task
                 * bodies. BigTask and SmallTask are admitted together and then run concurrently,
                 * so "SmallTask,BigTask,MediumTask," is an equally correct interleaving — and
                 * the StringBuilder was being mutated from several threads at once besides. The
                 * Awaitility retry hid it by re-running the batch until an ordering came out
                 * right, which is why it only failed now and then.
                 *
                 * onSubmit fires on the coordinator thread in the order the harness admits, so
                 * this is deterministic.
                 */
                final StringBuilder builder = new StringBuilder();
                final TaskExecutionListener<Void> admissionOrder = new TaskExecutionListener<Void>() {
                    @Override
                    public Void onSubmit(TaskDescriptor task) {
                        builder.append(task.name()).append(",");
                        return null;
                    }
                };
                pr.addListener(admissionOrder);

                List<ThrottledTask<Runnable>> jobs = new ArrayList<>(100);

                for (String taskName : taskNames) {
                    jobs.add(new ThrottledTask<>(() -> {
                        MDC.put("taskName", taskName);

                        var someLengthyOperation = IntStream.range(0, 999).asDoubleStream().average();


                    }, new Supplier<Long>() {

                        @Override
                        public Long get() {

                            switch (taskName) {
                                case "BigTask":
                                    return 6l;
                                case "MediumTask":
                                    return 5l;
                                case "SmallTask":
                                    return 1l;
                            }

                            return 0L;
                        }
                    }.get(), taskName));
                }

                //Setting the max memory to 200
                AdmissionPolicy loadThrottle = CompositePolicy.allOf(
                        new MemoryBudgetPolicy(10L, 1.0),
                        new MaxConcurrencyPolicy(10));
                pr.submitThrottled(jobs, loadThrottle);
                pr.waitForCompletion();
                pr.removeListener(admissionOrder);

                final String result = builder.toString();

                System.out.println(result);
                /**
                 *   Task that the "SmallTask" was executed in second place. This implies that the throttling works since the MemoryBudgetPolicy will
                 *   sum up the mem usage estimates of BigTask + MediumTask and if those exceed the threshold (which they do) the SmallTask will be
                 *   selected from the back of the sorted que.
                 */
                Assertions.assertThat(result).isEqualTo("BigTask,SmallTask,MediumTask,");
            }).doesNotThrowAnyException();


            //Task that at least one future is completed which means that the execution worked despite the memory exceeding the threshold limit.
            Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();
        });

    }

    @Test
    public void shouldThrottleDownBasedOnMemEstimatesVersionTwo() {
        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        List<String> taskNames = List.of("SmallTask", "MediumTask", "BigTask");

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertThatCode(() -> {

                StringBuilder builder = new StringBuilder();

                List<ThrottledTask<Runnable>> jobs = new ArrayList<>(100);

                for (String taskName : taskNames) {
                    jobs.add(new ThrottledTask<>(() -> {
                        MDC.put("taskName", taskName);

                        var someLengthyOperation = IntStream.range(0, 999).asDoubleStream().average();

                        builder.append(taskName).append(",");

                    }, new Supplier<Long>() {

                        @Override
                        public Long get() {

                            switch (taskName) {
                                case "BigTask":
                                    return 6l;
                                case "MediumTask":
                                    return 5l;
                                case "SmallTask":
                                    return 4l;
                            }

                            return 0L;
                        }
                    }.get(), taskName));
                }

                //Setting the max memory to 200
                AdmissionPolicy loadThrottle = CompositePolicy.allOf(
                        new MemoryBudgetPolicy(10L, 1.0),
                        new MaxConcurrencyPolicy(10));
                pr.submitThrottled(jobs, loadThrottle);
                pr.waitForCompletion();

                final String result = builder.toString();

                System.out.println(result);
                /**
                 *   Assert that everything is executed in order because executing either MediumTask or SmallTask together with BigTask
                 *   exceeds the memory throttling threshold. As a result, the tasks are executed in the order they are declared.
                 */
                Assertions.assertThat(result).isEqualTo("BigTask,MediumTask,SmallTask,");
            }).doesNotThrowAnyException();


            //Task that at least one future is completed which means that the execution worked despite the memory exceeding the threshold limit.
            Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();
        });

    }

    @Test
    public void shouldThrottleDownBasedOnMemEstimatesVersionThree() {
        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        List<String> taskNames = List.of("SmallTask", "MediumTask", "BigTask");

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertThatCode(() -> {

                StringBuilder builder = new StringBuilder();

                List<ThrottledTask<Runnable>> jobs = new ArrayList<>(100);

                for (String taskName : taskNames) {
                    jobs.add(new ThrottledTask<>(() -> {
                        MDC.put("taskName", taskName);

                        var someLengthyOperation = IntStream.range(0, 999).asDoubleStream().average();

                        builder.append(taskName).append(",");

                    }, new Supplier<Long>() {

                        @Override
                        public Long get() {

                            switch (taskName) {
                                case "BigTask":
                                    return 9l;
                                case "MediumTask":
                                    return 9l;
                                case "SmallTask":
                                    return 9l;
                            }

                            return 0L;
                        }
                    }.get(), taskName));
                }


                //Setting the max memory to 200
                AdmissionPolicy loadThrottle = CompositePolicy.allOf(
                        new MemoryBudgetPolicy(10L, 1.0),
                        new MaxConcurrencyPolicy(10));
                pr.submitThrottled(jobs, loadThrottle);
                pr.waitForCompletion();

                final String result = builder.toString();

                System.out.println(result);
                /**
                 *   In this test, all tasks are declared to have the
                 *   same memory consumption and any two tasks can't be executed in parallel. As a result, the tasks are
                 *   executed in the order in which they are declared: SmallTask, MediumTask, BigTask.
                 */
                Assertions.assertThat(result).isEqualTo("SmallTask,MediumTask,BigTask,");
            }).doesNotThrowAnyException();

            //Task that at least one future is completed which means that the execution worked despite the memory exceeding the threshold limit.
            Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();
        });

    }

    @Test
    public void shouldExecutedEvenThoughEveryTaskExceedsTheThreshold() {
        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        List<String> taskNames = List.of("SmallTask", "MediumTask", "BigTask");

        Awaitility.await().atMost(1, TimeUnit.SECONDS).untilAsserted(() -> {
            Assertions.assertThatCode(() -> {

                StringBuilder builder = new StringBuilder();

                List<ThrottledTask<Runnable>> jobs = new ArrayList<>(100);

                for (String taskName : taskNames) {
                    jobs.add(new ThrottledTask<>(() -> {
                        MDC.put("taskName", taskName);

                        var someLengthyOperation = IntStream.range(0, 999).asDoubleStream().average();

                        builder.append(taskName).append(",");

                    }, new Supplier<Long>() {

                        @Override
                        public Long get() {

                            switch (taskName) {
                                case "BigTask":
                                    return 11l;
                                case "MediumTask":
                                    return 11l;
                                case "SmallTask":
                                    return 11l;
                            }

                            return 0L;
                        }
                    }.get(), taskName));
                }

                //Task that the max value is set to some big number.
                // Assertions.assertThat(jobs.get(0).size).isEqualTo(100);

                //Setting the max memory to 200
                AdmissionPolicy loadThrottle = CompositePolicy.allOf(
                        new MemoryBudgetPolicy(10L, 1.0),
                        new MaxConcurrencyPolicy(10));
                pr.submitThrottled(jobs, loadThrottle);
                pr.waitForCompletion();

                final String result = builder.toString();

                System.out.println(result);
                /**
                 *    In this test, all tasks exceed the memory threshold. Regardless,
                 *    All of them should be executed. Or in other words, the executor may throttle down to one thread but never to zero
                 */
                Assertions.assertThat(result).isEqualTo("SmallTask,MediumTask,BigTask,");
            }).doesNotThrowAnyException();

            //Task that at least one future is completed which means that the execution worked despite the memory exceeding the threshold limit.
            Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();
        });

    }

    @Test
    public void shouldBeAbleToCancelAllTasks() {

        ThrottledExecutor pr = new ThrottledExecutor("Level_1");

        List<String> taskNames = IntStream.range(0, 100).mapToObj(i -> "Task" + i).collect(Collectors.toList());

        StringBuilder builder = new StringBuilder();

        List<Runnable> jobs = new ArrayList<>(100);

        for (String taskName : taskNames) {
            jobs.add(() -> {
                MDC.put("taskName", taskName);
                try {
                    var someLengthyOperation = IntStream.range(0, 9_999_999).asDoubleStream().average();
                    var average = someLengthyOperation.getAsDouble();

                    if (taskName.equals("Task2")) {
                        throw new Exception("Test Exception - Lets Start Canceling tasks left and right.");
                    }
                } catch (Throwable throwable) {
                    throwable.printStackTrace();
                    if (true) {  // Imagine I exceeded some failure limit here.
                        if (!pr.isClosed()) {
                            pr.close(); //In throttled mode, we need to start rejecting new tasks.
                            pr.cancelRemainingRunningTasks();
                        }
                    }
                }

            });
        }

        jobs.forEach(job -> pr.submit(job));

        try {
            pr.waitForCompletion();
        } catch (Throwable throwable) {

            throwable.printStackTrace();
        }

        Assertions.assertThat(pr.isClosed()).isTrue();

        Assertions.assertThat(pr.submit(() -> {
            System.out.println("This should now execute because the executor should have been closed. If you see this something has gone wrong...");
        }).isCompletedExceptionally()).isTrue();


        boolean hasAnyFutureBeenCancelled = false;
        for (FuturePair futurePair : pr.getFutures().toList()) {

            if (futurePair.getCompletableFuture().isCancelled()) {
                hasAnyFutureBeenCancelled = true;
                break;
            }
        }

        Assertions.assertThat(hasAnyFutureBeenCancelled).isTrue();

        //Task that at least one future is completed which means that the execution worked despite the memory exceeding the threshold limit.
        Assertions.assertThat(pr.getFutures().toList().getFirst().getCompletableFuture().isDone()).isTrue();

    }

    // ── Throughput ──────────────────────────────────────────────────────────────────────

    /**
     * Tasks per round and the batch size each {@code submitThrottled} call receives. A
     * program submits work in batches, not one task at a time and not all at once, so the
     * measurement does the same.
     */
    private static final int THROUGHPUT_TASKS_PER_ROUND = Integer.getInteger("executors.throughput.tasks", 20_000);
    private static final int THROUGHPUT_BATCH_SIZE = Integer.getInteger("executors.throughput.batch", 1_000);
    private static final int THROUGHPUT_ROUNDS = Integer.getInteger("executors.throughput.rounds", 3);

    /**
     * Generous floor, far below what any reasonable machine produces. It exists to catch a
     * pathological regression — a quadratic admission scan, a lock convoy, a per-task
     * allocation storm — not to benchmark. Read the log table for the real figures.
     */
    private static final double THROUGHPUT_FLOOR_TASKS_PER_SECOND = 5_000;

    /** A no-op body that still proves it ran. */
    private static Runnable countingTask(java.util.concurrent.atomic.AtomicLong ran) {
        return ran::incrementAndGet;
    }

    private static List<ThrottledTask<Runnable>> throughputBatch(
            java.util.concurrent.atomic.AtomicLong ran, int size) {
        List<ThrottledTask<Runnable>> batch = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            batch.add(new ThrottledTask<>(countingTask(ran), 1_024L, "t"));
        }
        return batch;
    }

    /**
     * The floor: a bare {@code ForkJoinPool} of the same size as the library's, plain
     * {@code execute} plus a latch — no futures, no tracking, no listeners, no MDC. Nothing
     * built on a pool can beat this; it is what every other figure is paying on top of.
     */
    private static double measureRawForkJoinPool(int taskCount) throws InterruptedException {
        int poolSize = Math.max(Runtime.getRuntime().availableProcessors() - 2, 2);
        java.util.concurrent.ForkJoinPool raw = new java.util.concurrent.ForkJoinPool(poolSize);
        try {
            java.util.concurrent.atomic.AtomicLong ran = new java.util.concurrent.atomic.AtomicLong();
            java.util.concurrent.CountDownLatch done = new java.util.concurrent.CountDownLatch(taskCount);
            Runnable task = () -> {
                ran.incrementAndGet();
                done.countDown();
            };

            long start = System.nanoTime();
            for (int i = 0; i < taskCount; i++) {
                raw.execute(task);
            }
            Assertions.assertThat(done.await(60, TimeUnit.SECONDS)).isTrue();
            long elapsed = System.nanoTime() - start;

            Assertions.assertThat(ran.get()).isEqualTo(taskCount);
            return taskCount / (elapsed / 1_000_000_000.0);
        } finally {
            raw.shutdownNow();
        }
    }

    /** One timed round through the plain {@code submit} path. Returns tasks per second. */
    private static double measurePlainSubmit(int taskCount) {
        ThrottledExecutor executor = new ThrottledExecutor("Throughput-plain");
        java.util.concurrent.atomic.AtomicLong ran = new java.util.concurrent.atomic.AtomicLong();
        Runnable task = countingTask(ran);

        long start = System.nanoTime();
        for (int i = 0; i < taskCount; i++) {
            executor.submit(task);
        }
        executor.waitForCompletion();
        long elapsed = System.nanoTime() - start;

        Assertions.assertThat(ran.get()).isEqualTo(taskCount);
        return taskCount / (elapsed / 1_000_000_000.0);
    }

    /** One timed round through {@code submitThrottled} under {@code policy}. Returns tasks per second. */
    private static double measureThrottled(int taskCount, int batchSize, AdmissionPolicy policy, String label) {
        ThrottledExecutor executor = new ThrottledExecutor("Throughput-" + label);
        java.util.concurrent.atomic.AtomicLong ran = new java.util.concurrent.atomic.AtomicLong();

        // Build every batch up front so list construction is not part of what is timed.
        List<List<ThrottledTask<Runnable>>> batches = new ArrayList<>();
        for (int submitted = 0; submitted < taskCount; submitted += batchSize) {
            batches.add(throughputBatch(ran, Math.min(batchSize, taskCount - submitted)));
        }

        long start = System.nanoTime();
        for (List<ThrottledTask<Runnable>> batch : batches) {
            executor.submitThrottled(batch, policy);
        }
        executor.waitForCompletion();
        long elapsed = System.nanoTime() - start;

        Assertions.assertThat(ran.get()).isEqualTo(taskCount);
        return taskCount / (elapsed / 1_000_000_000.0);
    }

    /**
     * Measures the sustained throughput ceiling of the throttled path: how many tasks per
     * second {@code submitThrottled} + {@code waitForCompletion} push through when the
     * tasks themselves cost nothing. With no-op bodies what is measured is the
     * <em>library's own overhead per task</em> — admission under the harness lock, the
     * two built-in listeners, future creation and settlement, failure bookkeeping, the
     * coordinator loop — not the work.
     *
     * <p>Three figures are reported, each the best of {@value #THROUGHPUT_ROUNDS} rounds of
     * {@value #THROUGHPUT_TASKS_PER_ROUND} tasks after a warm-up:</p>
     * <ul>
     *   <li><b>plain submit</b> — the baseline the throttled path pays on top of;</li>
     *   <li><b>throttled, always-admit policy</b> — the pure cost of the throttled path with
     *       admission never refusing, i.e. the ceiling;</li>
     *   <li><b>throttled, default policy</b> — what a zero-config caller actually gets,
     *       including the process-wide heap ledger and the CPU-count concurrency cap.</li>
     * </ul>
     *
     * <p>The numbers are machine- and backend-dependent and are NOT asserted beyond a
     * deliberately generous floor. Run with
     * {@code -Dio.github.grishagrigorov.executors.backend=virtual} to compare backends.</p>
     */
    @Test
    public void measureMaxSustainedThroughputOfTheThrottledPath() throws InterruptedException {

        ExecutorBackend backend = ExecutorBackend.defaultBackend();
        AdmissionPolicy alwaysAdmit = CompositePolicy.allOf(
                new MemoryBudgetPolicy(Long.MAX_VALUE / 4, 1.0),
                new MaxConcurrencyPolicy(Integer.MAX_VALUE));
        AdmissionPolicy defaultPolicy = AdmissionPolicy.defaultPolicy(backend);

        // The default policy's two members, each alone, so the report says where its cost goes.
        int cap = backend.defaultMaxConcurrency();
        AdmissionPolicy capOnly = new MaxConcurrencyPolicy(cap);
        AdmissionPolicy ledgerOnly = new io.github.grishagrigorov.executors.throttling.GlobalMemoryBudgetPolicy(
                (long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);

        // Warm-up: JIT, class loading, pool threads, listener paths. Not measured.
        measureRawForkJoinPool(THROUGHPUT_TASKS_PER_ROUND / 4);
        measurePlainSubmit(THROUGHPUT_TASKS_PER_ROUND / 4);
        measureThrottled(THROUGHPUT_TASKS_PER_ROUND / 4, THROUGHPUT_BATCH_SIZE, alwaysAdmit, "warmup");
        measureThrottled(THROUGHPUT_TASKS_PER_ROUND / 4, THROUGHPUT_BATCH_SIZE, defaultPolicy, "warmup");

        double bestRaw = 0, bestPlain = 0, bestThrottled = 0, bestCapOnly = 0, bestLedgerOnly = 0, bestDefault = 0;
        for (int round = 1; round <= THROUGHPUT_ROUNDS; round++) {
            double raw = measureRawForkJoinPool(THROUGHPUT_TASKS_PER_ROUND);
            double plain = measurePlainSubmit(THROUGHPUT_TASKS_PER_ROUND);
            double throttled = measureThrottled(THROUGHPUT_TASKS_PER_ROUND, THROUGHPUT_BATCH_SIZE, alwaysAdmit, "ceiling");
            double byCap = measureThrottled(THROUGHPUT_TASKS_PER_ROUND, THROUGHPUT_BATCH_SIZE, capOnly, "cap");
            double byLedger = measureThrottled(THROUGHPUT_TASKS_PER_ROUND, THROUGHPUT_BATCH_SIZE, ledgerOnly, "ledger");
            double byDefault = measureThrottled(THROUGHPUT_TASKS_PER_ROUND, THROUGHPUT_BATCH_SIZE, defaultPolicy, "default");
            LOGGER.info("throughput round {}: rawFJP={} plain={} throttled(always-admit)={} cap-only={} ledger-only={} default={} tasks/s",
                    round, (long) raw, (long) plain, (long) throttled, (long) byCap, (long) byLedger, (long) byDefault);
            bestRaw = Math.max(bestRaw, raw);
            bestPlain = Math.max(bestPlain, plain);
            bestThrottled = Math.max(bestThrottled, throttled);
            bestCapOnly = Math.max(bestCapOnly, byCap);
            bestLedgerOnly = Math.max(bestLedgerOnly, byLedger);
            bestDefault = Math.max(bestDefault, byDefault);
        }

        String report = String.format(
                "%n"
                + "  ThrottledExecutor throughput — backend=%s, cores=%d, %,d tasks/round x %d rounds, batches of %,d, no-op bodies%n"
                + "  (tasks/s = tasks per second; µs/task = microseconds of overhead per task)%n"
                + "  ----------------------------------------------------------------------------------------------%n"
                + "  raw ForkJoinPool.execute + latch            %,12.0f tasks/s   %6.2f µs/task   (floor: no futures, no tracking)%n"
                + "  plain submit()                              %,12.0f tasks/s   %6.2f µs/task   (%3.0f%% of raw)%n"
                + "  submitThrottled, always-admit               %,12.0f tasks/s   %6.2f µs/task   (%3.0f%% of plain — the throttled-path ceiling)%n"
                + "  submitThrottled, MaxConcurrency(%d) only   %,12.0f tasks/s   %6.2f µs/task   (%3.0f%% of plain — the default's cap, alone)%n"
                + "  submitThrottled, GlobalMemoryBudget only    %,12.0f tasks/s   %6.2f µs/task   (%3.0f%% of plain — the default's ledger, alone)%n"
                + "  submitThrottled, default policy (both)      %,12.0f tasks/s   %6.2f µs/task   (%3.0f%% of plain)%n",
                backend, Runtime.getRuntime().availableProcessors(),
                THROUGHPUT_TASKS_PER_ROUND, THROUGHPUT_ROUNDS, THROUGHPUT_BATCH_SIZE,
                bestRaw, 1_000_000 / bestRaw,
                bestPlain, 1_000_000 / bestPlain, 100 * bestPlain / bestRaw,
                bestThrottled, 1_000_000 / bestThrottled, 100 * bestThrottled / bestPlain,
                cap, bestCapOnly, 1_000_000 / bestCapOnly, 100 * bestCapOnly / bestPlain,
                bestLedgerOnly, 1_000_000 / bestLedgerOnly, 100 * bestLedgerOnly / bestPlain,
                bestDefault, 1_000_000 / bestDefault, 100 * bestDefault / bestPlain);
        LOGGER.info(report);
        //System.out.println(report);

        Assertions.assertThat(bestThrottled)
                .as("throttled-path throughput floor — a pathological regression, not a benchmark miss")
                .isGreaterThan(THROUGHPUT_FLOOR_TASKS_PER_SECOND);
        Assertions.assertThat(bestDefault)
                .as("default-policy throughput floor")
                .isGreaterThan(THROUGHPUT_FLOOR_TASKS_PER_SECOND);
    }

}
