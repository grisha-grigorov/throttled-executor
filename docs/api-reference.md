# API reference

The public surface of the `executors` family: every exported type and the methods a
consumer calls. Package-private members and the `internal` package are omitted. The root
`README.md` is the tutorial; each satellite's `README.md` is that satellite's contract.
When this file and the sources disagree, the source wins and this file is fixed.

## Core: `io.github.grishagrigorov.executors`

### `ThrottledExecutor` (implements `ExecutorService`, `AutoCloseable`)

**Construction and the tree**

| Method | Notes |
|---|---|
| `new ThrottledExecutor(String idPrefix)` | always a root |
| `new ThrottledExecutor(String idPrefix, ThrottledExecutor parent)` | child, or root when parent is null |
| `static build(String idPrefix, ThrottledExecutor parent)` | same as the two-arg constructor |
| `static withBackend(String idPrefix, ExecutorBackend backend)` | root on an explicit backend |
| `static withAmbientDiscovery(String idPrefix)` | adopts the executor whose task runs on this thread, else root |
| `static currentAmbient()` | the ambient executor on this thread, or null |
| `newChild(String idPrefix)` | primary way to make a child; inherits backend and listeners |
| `parent()`, `root()`, `isRoot()`, `executorPath()`, `backend()` | tree inspection |

**Listeners**

| Method | Notes |
|---|---|
| `addListener(TaskExecutionListener<?>)` | returns `this`; the subtree sees it live |
| `addDiscoveredListeners()` | `ServiceLoader` opt-in, per root |
| `removeListener(TaskExecutionListener<?>)` | `false` for an inherited listener |

**Submission**

| Method | Notes |
|---|---|
| `submit(Runnable)`, `submit(Callable<T>)`, `submit(Supplier<T>)` | plain, not throttled; never throws, failures surface via the future |
| `submitRunnables(Runnable[])`, `submitRunnables(List<? extends Runnable>)`, `submitCallables(List<? extends Callable<T>>)` | plain bulk submission |
| `submitThrottled(List<ThrottledTask<V>>)` | default policy |
| `submitThrottled(List, AdmissionPolicy)` | |
| `submitThrottled(List, AdmissionPolicy, boolean clearCompletedFutures)` | |
| `invokeAllThrottled(List<ThrottledTask<Callable<R>>>)` / `(…, AdmissionPolicy)` | blocks and returns the results |
| `submitThrottled(Iterator<ThrottledTask<V>>)` / `(…, AdmissionPolicy)` / `(…, AdmissionPolicy, int window)` | streaming, window-bounded memory |
| `execute(Runnable)` | `Executor` contract |

**Waiting and inspection**

| Method | Notes |
|---|---|
| `waitForCompletion()` | transitive over the subtree; throws the aggregate |
| `waitForCompletion(Duration)` | returns `WaitResult` instead of blocking forever |
| `waitForOwnCompletion()` | own batch only |
| `getFutures()` | `Stream<FuturePair>` |
| `totalJobs()` | |
| `clearFuturesListFromCompletedTasks()` | manual compaction |
| `awaitTermination(long, TimeUnit)`, `isShutdown()`, `isTerminated()` | `ExecutorService` contract |

**Cancellation and shutdown**

| Method | Notes |
|---|---|
| `cancelSubtree()` | depth-first close and cancel |
| `shutdown()`, `shutdownNow()`, `close()`, `isClosed()` | closes this executor, not the shared pools |
| `cancelRemainingRunningTasks()`, `cancelAllRunningFutures()` | cancel own futures |
| `static shutdownThreadExecutors()` | shuts down the shared pools; they resurrect on next use |

### Value and helper types

- `ThrottledTask<T>`: public final fields `job`, `size`, `name`, `tags`; constructors
  `(job, size, name)` and `(job, size, name, tags)`; factories `of(job, size)`,
  `of(job, size, name)`, `of(job, size, name, tags)`; `hasTag(String)`. Identity-based,
  no `equals`/`hashCode`.
- `FuturePair`: `getCompletableFuture()`, `getJob()`, `getDescriptor()`. Handed out, never
  constructed by callers.
- `WaitResult` record: `complete()`, `outstanding()` as a list of `TaskDescriptor`.
- `ExecutorBackend` enum: `FORK_JOIN`, `VIRTUAL_THREAD_PER_TASK`, `CALLER_RUNS`;
  `static defaultBackend()`, `isAvailable()`, `isCreated()`, `defaultMaxConcurrency()`.
- `FailFastListener`: opt-in listener; `triggered()`, `cancellations()`.
- `ThreadContextAwareCompletionException`: `executorPath()`, plus the usual `getMessage()`.
- `exceptions.RootCauseExceptionUtil.getRootCause(Throwable)`.
- `LogMarkers` enum for SLF4J marker filtering.

### `spi` package

- `TaskExecutionListener<C>`: `onSubmit(TaskDescriptor)` returns per-task state `C`;
  `onThrottled(TaskDescriptor, long waitedNanos, boolean floorAdmitted)`;
  `onStart(TaskDescriptor, C)`; `onComplete(TaskDescriptor, C, TaskOutcome)`. All defaulted.
- `TaskDescriptor` record: `executor()`, `sequence()`, `name()`, `sizeEstimate()`,
  `submittedAtNanos()`, `submittedAtEpochMillis()`, plus `executorId()`, `executorPath()`,
  `backend()`, `displayName()`; constant `UNKNOWN_SIZE`.
- `TaskOutcome` record: `status()`, `failure()`, `startedAtNanos()`, `completedAtNanos()`,
  `bodyRan()`, `durationNanos()`.
- `TaskStatus` enum: `COMPLETED`, `FAILED`, `CANCELLED_BEFORE_START`,
  `CANCELLED_AFTER_START`, `REJECTED`; `bodyRan()`.

### `throttling` package

- `AdmissionPolicy` interface: `admit(ThrottledTask, PoolSnapshot)` is the one abstract
  method. Defaults: `onTaskStart`, `onTaskEnd`, `onScanStart`, `selectionOrder()`,
  `inFlightTaskCount()`, `requiresExclusive(task)`, `mayRequireExclusive()`,
  `ledgerSpansExecutors()`. Statics: `defaultPolicy()`, `defaultPolicy(ExecutorBackend)`,
  `perExecutorDefaultPolicy()`.
- `PoolSnapshot`: `runningTasks()`, `trackedSizeBytes()`.
- `ForwardingAdmissionPolicy`: abstract base for decorators, protected constructor taking
  the delegate. Every decorator must extend it.
- `CompositePolicy.allOf(AdmissionPolicy...)`: all must admit.
- `MaxConcurrencyPolicy(int)`.
- `MemoryBudgetPolicy(long ceilingBytes, double multiplier)`: per-instance budget.
- `GlobalMemoryBudgetPolicy(long ceilingBytes, double multiplier)`,
  `static processWideHeapBudget()`, `inFlightBytes()`.
- `RateLimitPolicy(int maxStarts, Duration window, InstantSource clock)`.
- `ResourcePermitPolicy(Map<String,Integer> limits)`, `inUse(String tag)`.
- `HeapPressurePolicy`: `of(double high)`, `defaults()`, full constructor
  `(high, low, samplePeriod, HeapReader, InstantSource)`, `platformReader()`, `high()`,
  `low()`, `lastPressure()`, `isUnderPressure()`, `readPressure()`. `HeapReader` is a
  one-method interface, `occupancyFraction()`.

## Satellites

- **jfr** (`:executors-jfr`): `JfrTaskListener` with `INSTANCE` and a public constructor.
  `TaskEvent` and `ThrottleEvent` are the recorded event types. Discoverable.
- **observability** (`:executors-observability`): `ExecutionRecorder` (`shared()`,
  `new ExecutionRecorder()`, `new ExecutionRecorder(RecorderConfig)`, `snapshot()`,
  `renderText()`, `renderJson()`, `registerMBean(name)`, `unregisterMBean(name)`, and the
  MXBean getters such as `getSubmittedTotal()`, `getFailedTotal()`, `getThrottledTotal()`).
  `RecorderConfig.defaults()`. `RecorderSnapshot` and `ExecutorNodeSnapshot` are read-only
  records with `renderText()` and `renderJson()`. Discoverable.
- **micrometer** (`:executors-micrometer`): `MicrometerTaskListener(MeterRegistry)` and
  `(MeterRegistry, String prefix)`. Not discoverable (needs a registry).
- **adaptive** (`:executors-adaptive`): `AdaptiveSizing` factories `inMemory()`,
  `onFile(Path)`, `onFile(Path, AdaptiveConfig)`, `of(ProfileStore, AdaptiveConfig[, InstantSource])`;
  then `listener()`, `policy(AdmissionPolicy delegate)`, `effectiveSize(name, declared)`,
  `profile(name)`, `freshProfile(name)`, `forget(name)`, `save()`, `close()`.
  `AdaptiveConfig.defaults()` with `withVersion`, `withMaxSampleAge`, `withSelectionKey`.
  Stores: `InMemoryProfileStore(int ringSize)`, `FileProfileStore(Path, int ringSize)`.
  `TaskProfile` exposes `samples()`, `duration()`, `ratio(Estimator)`, `absolute(Estimator)`.
  Not discoverable (needs a store path).

## Example usage

Basic throttled batch with the default policy:

```java
try (var root = new ThrottledExecutor("import")) {
    List<ThrottledTask<Runnable>> tasks = files.stream()
            .map(f -> ThrottledTask.of((Runnable) () -> parse(f), f.length(), "parse"))
            .toList();
    root.submitThrottled(tasks);
    root.waitForCompletion();          // throws the aggregate if any task failed
}
```

Custom composite policy, tags, and results from callables:

```java
AdmissionPolicy policy = CompositePolicy.allOf(
        new MaxConcurrencyPolicy(4),
        new RateLimitPolicy(10, Duration.ofSeconds(1), InstantSource.system()),
        new ResourcePermitPolicy(Map.of("db", 2)));

List<ThrottledTask<Callable<Row>>> jobs = ids.stream()
        .map(id -> ThrottledTask.of((Callable<Row>) () -> load(id), 1_000_000L, "load", Set.of("db")))
        .toList();
List<Row> rows = root.invokeAllThrottled(jobs, policy);
```

Parent and child executors, plus a bounded wait:

```java
var root = new ThrottledExecutor("pipeline");
var stage = root.newChild("stage-a");
stage.submit(() -> doWork());
WaitResult r = root.waitForCompletion(Duration.ofSeconds(30));
if (!r.complete()) {
    r.outstanding().forEach(d -> log.warn("still running: {}", d.displayName()));
    root.cancelSubtree();
}
```

Streaming submission from an iterator with a window:

```java
Iterator<ThrottledTask<Runnable>> it = lines.map(l -> ThrottledTask.of(() -> handle(l), 4096L)).iterator();
root.submitThrottled(it, AdmissionPolicy.defaultPolicy(root.backend()), 256);
root.waitForOwnCompletion();
```

A listener with per-task state:

```java
root.addListener(new TaskExecutionListener<Long>() {
    @Override public Long onSubmit(TaskDescriptor t) { return System.nanoTime(); }
    @Override public void onComplete(TaskDescriptor t, Long submittedAt, TaskOutcome o) {
        if (o.status() == TaskStatus.FAILED) log.error("{} failed", t.displayName(), o.failure());
    }
});
root.addListener(new FailFastListener());   // cancel the subtree on the first failure
root.addDiscoveredListeners();              // picks up JFR and the ExecutionRecorder if present
```

Satellites:

```java
// Micrometer
root.addListener(new MicrometerTaskListener(registry, "executors"));

// Observability
ExecutionRecorder rec = new ExecutionRecorder().registerMBean("io.github.grishagrigorov:type=ExecutionRecorder");
root.addListener(rec);
System.out.println(rec.renderText());

// Adaptive sizing: learns the real memory cost per task name
try (AdaptiveSizing sizing = AdaptiveSizing.onFile(Path.of("profiles.json"))) {
    root.addListener(sizing.listener());
    root.submitThrottled(tasks, sizing.policy(AdmissionPolicy.defaultPolicy()));
    root.waitForCompletion();
}
```

Writing a policy decorator, the required way:

```java
final class LoggingPolicy extends ForwardingAdmissionPolicy {
    LoggingPolicy(AdmissionPolicy delegate) { super(delegate); }
    @Override public boolean admit(ThrottledTask<?> c, PoolSnapshot s) {
        boolean ok = super.admit(c, s);
        if (!ok) log.debug("held {} with {} running", c.name, s.runningTasks());
        return ok;
    }
}
```

`TutorialTests` in core holds longer runnable versions of these recipes.
