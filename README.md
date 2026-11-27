# executors

A standalone Java library for bounded parallel execution. It provides
**pluggable, policy-based throttling** over shared thread pools, an explicit
**parent/child executor tree**, and an optional **virtual-thread backend**:
callers submit tasks with an estimated memory footprint, and an
`AdmissionPolicy` decides whether each task may start now and in what order
queued tasks are offered.

Its purpose is to let a batch of memory-hungry tasks run with maximum
parallelism **without blowing the JVM heap** — report generation and data
loading jobs of wildly different sizes competing for the same process.

The library compiles against the **Java 17 API** (a consumer on a Java 17 JVM
can use everything here), while the virtual-thread backend lights up
automatically on Java 21+ through a runtime probe — there is no compile-time
dependency on any post-17 API.

## Design in one paragraph

Policies are **pure decision logic**; the framework owns all locking and
signalling. Candidate scanning and claiming happen as **one atomic step** under
the framework's lock, so concurrent submitters can never double-claim a budget.
On every task completion the framework wakes waiting submitters
unconditionally, so a buggy policy can never cause a lost wake-up — the worst
it can do is make the submitter re-check and re-sleep. Two further guarantees
are enforced by the executor, outside any policy's reach: the **liveness
floor** (when nothing is in flight, the next candidate is always admitted — the
system never throttles to zero) and a **timed re-check** (a blocked submitter
re-evaluates every ~100 ms, which doubles as the poll for time-based policies
and for budgets shared across executors). A third covers nested work: a
**re-entrant submitter** — one running inside a task this tree has already
claimed — is always allowed to claim, because the only thread that could release
that claim is the one now waiting for admission.

For the class structure, the listener and policy hierarchies, the module graph and the
per-task lifecycle as diagrams, see [`docs/architecture.md`](docs/architecture.md).

## Backends, one choice per executor

Every executor submits to one **shared, lazily-created** pool — or, for tests, to
no pool at all:

| Backend | What it is | Use it for |
|---------|-----------|------------|
| `FORK_JOIN` *(default)* | Fixed work-stealing pool, `availableProcessors - 2` workers | Compute-bound work — it *stops* you running more than the cores can take |
| `VIRTUAL_THREAD_PER_TASK` | One virtual thread per task (Java 21+; falls back to ForkJoin below that) | Blocking work — sockets, DB, file IO — it *lets* many tasks park cheaply |
| `CALLER_RUNS` | No pool: the body runs on the submitting thread | Tests of code that *uses* the executor — no latches, no polling |

```java
// Default — unchanged behaviour, ForkJoinPool:
var compute = new ThrottledExecutor("Reports");

// Opt in per executor:
var io = ThrottledExecutor.withBackend("SpmbbFetch",
                                            ExecutorBackend.VIRTUAL_THREAD_PER_TASK);
```

The JVM-wide default is overridable with
`-Dio.github.grishagrigorov.executors.backend=virtual|forkJoin|callerRuns`.

`CALLER_RUNS` exists so the library does not force your tests to be
concurrent. Everything except the dispatch is unchanged — the completion state
machine, the listener fan-out, failure aggregation and the throttle all still
run — but a batch is finished by the time `submitThrottled` returns:

```java
var executor = ThrottledExecutor.withBackend("Test", ExecutorBackend.CALLER_RUNS);
executor.submitThrottled(batch);
assertThat(results).containsExactly(...);        // already done
```

Two things it cannot tell you: concurrency is 1, so the harness only ever sees
zero or one task in flight and a policy is never exercised at its limits; and
since nothing else can complete, a policy that refuses a lone candidate would
poll forever if the **liveness floor** did not admit it anyway — which turns
that case into serial execution rather than a livelock.

Nested submission is the sharp edge here, and it is handled. Running a body
inline means the coordinator for any batch that body submits *is* the thread
holding the outer task's claim, so a refusal would be unbreakable. The
re-entrancy guarantee covers it: a submitter inside a task of the same tree
always gets in, one task at a time. That holds on the pooled backends too,
where the same deadlock needs every worker to be nesting at once.

Choosing wrong is not symmetric. Blocking work left on `FORK_JOIN` parks one of
a handful of workers, and the pool cannot see plain socket blocking, so it never
compensates — a few such tasks stall the pool. Compute work on the virtual
backend merely loses work-stealing. When unsure, virtual is the cheaper
mistake. But note virtual threads are **not preemptive**: a long computation
holds its carrier to the end, so they are no remedy for CPU-bound work.

Under the virtual backend nothing but your `AdmissionPolicy` bounds
concurrency — `MaxConcurrencyPolicy` stops being decorative and becomes the
only limit there is. The default policy sizes it by expected downstream
capacity (flat 256, tunable via `io.github.grishagrigorov.executors.virtualConcurrency`)
rather than by core count.

## The executor tree

Executors form an explicit parent/child tree. Ownership is deliberate — the
plain constructor **always creates a root**, even when called from inside a
running task, so a field-initialised or injected executor can never be adopted
by whatever happened to be on the stack:

```java
var root  = new ThrottledExecutor("Voyage");        // always a root
var child = root.newChild("Leg-3");                      // primary API

// Escape hatch for framework callbacks where the parent isn't in lexical scope —
// adopts the executor whose task is running on this thread, else becomes a root:
var leaf  = ThrottledExecutor.withAmbientDiscovery("FuelCalc");
```

What the tree gives you:

- **One shared budget per tree.** A child delegates to its root's throttle
  state, so a whole subtree admits against one set of counters — ten nested
  executors can no longer each claim 85% of the heap.
- **Transitive completion.** `waitForCompletion()` waits for this executor's
  batch *and everything its tasks spawned* — including fire-and-forget
  children, and children created while the wait was already in progress.
  `waitForOwnCompletion()` keeps the old own-batch-only behaviour.
- **Subtree cancellation.** `root.cancelSubtree()` closes and cancels
  depth-first; `shutdownNow()` does the same. A task that has already *started*
  is interrupted on the virtual backend only — see Cancellation below.
- **Failure paths.** The aggregate exception's message carries
  `Voyage → Leg-3 → FuelCalc-12`, not just the failing executor's id.
- **Inherited listeners.** A listener added to a root observes every task in
  the subtree — live, including children created before it was added. See
  the next section.
- Children are held weakly and pruned automatically — a long-lived root does
  not accumulate its dead children. `close()` deregisters deterministically.

## Listeners — observing every task

`TaskExecutionListener` is the library's extension seam for cross-cutting
concerns: context propagation (MDC, tracing, security principals), execution
metadata and flight recording, metrics, JFR events. The executor's own MDC
propagation and ambient-parent binding are implemented as listeners of exactly
this shape — it is sufficient for the library's own needs before yours.

```java
import io.github.grishagrigorov.executors.spi.*;

/** Carry a request id from the submitter to the worker. */
final class RequestIdListener implements TaskExecutionListener<String> {
    @Override public String onSubmit(TaskDescriptor task) { return RequestId.current(); }
    @Override public void onStart(TaskDescriptor task, String id) { RequestId.set(id); }
    @Override public void onComplete(TaskDescriptor task, String id, TaskOutcome outcome) {
        if (outcome.bodyRan()) RequestId.clear();      // only restore what onStart installed
    }
}

root.addListener(new RequestIdListener());          // inherited by the whole subtree
```

Four hooks, one invariant each listener can rely on:

| Hook | Thread | Fires |
|------|--------|-------|
| `C onSubmit(TaskDescriptor)` | submitter | once per submission; returns this listener's per-task state |
| `onThrottled(TaskDescriptor, long waitedNanos, boolean floorAdmitted)` | worker | only for **throttled** submissions whose body runs, once, before `onStart` |
| `onStart(TaskDescriptor, C)` | worker | only if the body actually runs |
| `onComplete(TaskDescriptor, C, TaskOutcome)` | worker — or the submitter/canceller for `REJECTED` / `CANCELLED_BEFORE_START` | **exactly once** per `onSubmit`, whatever happened |

`onThrottled` answers the question the other three cannot: *why was this batch
slow?* Queue latency was always derivable from the descriptor's timestamps, but
nothing said whether a task waited because the policy refused it, nor whether it
eventually ran because the policy relented or because the **liveness floor**
overrode it. `floorAdmitted` is that second fact, and it is the difference
between a throttle working and a throttle giving up — a batch full of floor
admissions is running serially with a throttle attached.

`waitedNanos` is that task's own time-to-admission, measured from when its
coordinator first offered it, so every task freed in the same burst reports the
time it actually spent queued. It is not "did the coordinator block": a task
admitted on the first scan reports a small non-zero figure rather than zero.

Like `onStart`, it fires **only for bodies that run**. A task the harness
admitted and charged for, then cancelled while queued or rejected because the
executor closed, reports nothing — so totals built from this hook are totals over
*started* tasks, and a batch cut short by `cancelSubtree()` shows fewer throttle
events than it had admissions.

`TaskDescriptor` identifies the task (executor, sequence, the `ThrottledTask`
name and size for throttled submissions, submission time) and deliberately
holds the executor but **never the job**, so a recorder that keeps descriptors
never pins task bodies. `TaskOutcome` carries the `TaskStatus`, the raw failure
(not wrapped in a `CompletionException`), and start/end timestamps.

Rules, all pinned by tests:

- **Observe, never alter.** Listeners cannot wrap, retry or swallow. A wrapping
  decorator that swallowed a failure would make every recorder after it see a
  success, so wrap-style is deliberately not offered.
- **Ordering.** `onSubmit`/`onStart` run root-first through the tree (inherited
  listeners before the executor's own); `onComplete` runs in reverse — ordinary
  scope semantics, so whoever installed thread state first restores it last.
- **A listener can never fail the task.** Every invocation is guarded
  individually: a throwing listener is logged, the task proceeds, and the other
  listeners still run.
- **Snapshotted at submission.** A listener added later never sees that task.
- **Fail fast, if you want it.** `root.addListener(new FailFastListener())` cancels
  the whole subtree on the first `FAILED` task — there is no reason to build nine
  hundred more reports after the database went away. Opt-in and never a built-in,
  because it *closes* the executor; and note that cancelling a queued task runs
  every listener's `onComplete` on the cancelling thread, so a tree carrying
  expensive listeners pays for them once per cancelled task.
- **Inside `onComplete` the future is not yet complete.** A task's scope is
  closed *before* any waiter or dependent observes completion. Use the
  `TaskOutcome`; never `join()` the task's own future from a hook.
- **Fast and non-blocking.** Hooks run inline on the submitting and worker
  threads. Per-task state goes in `C`, never in listener fields — one instance
  serves every task on every thread.
- **Discovery is opt-in, per root.** `root.addDiscoveredListeners()` attaches
  every listener registered under `META-INF/services/…TaskExecutionListener`
  on the classpath (satellites such as `executors-jfr` ship one). A root that
  does not ask gets nothing — a jar's presence never changes behaviour.
- Children can add listeners, never remove inherited ones. The two built-ins
  are not removable at all.

## One executor for all of your tasks

`ThrottledExecutor` accepts two kinds of submission on the same instance:

- **Throttled, memory-aware batches** — wrap jobs in `ThrottledTask` and call
  `submitThrottled(...)`; the submitting thread drives admission.
- **Regular tasks** — pass `Runnable`s, `Callable`s, or `Supplier`s straight to
  `submit(...)`; they go to the pool immediately, no admission checks.

`submit(...)` **never throws** — a closed executor or a shut-down pool is
reported through the returned future, so there is always a future to consume.

```java
ThrottledExecutor executor = new ThrottledExecutor("AppWide");

// Fire-and-forget housekeeping — nobody waits:
executor.submit(() -> auditLog.flush());

// Memory-aware batch, admission-throttled by policy:
executor.submitThrottled(reportTasks);

// Individual future composition:
CompletableFuture<Report> report = executor.submit((Callable<Report>) () -> buildReport());
report.thenAccept(ui::show);

// ...or block here until everything submitted so far — including work spawned
// by child executors — is done:
executor.waitForCompletion();
```

### Blocking from inside a pool task is safe

A task may create its own nested executor and wait on it.
`CompletableFuture`'s own waiting routes through the ForkJoinPool's
managed-blocking machinery internally, so the shared pool compensates for the
blocked worker with a spare thread; on the virtual backend a parked waiter
costs nothing. The classic every-worker-blocked ForkJoinPool deadlock cannot
occur through this path.

## Getting the library

The library is a small family of modules: a core that does everything described
on this page, and **satellites** that plug cross-cutting concerns in through the
listener SPI without core knowing they exist.

Version 1.0.0 is not on Maven Central yet. Clone the repository and build it
from source (`./gradlew check`), or include it in your own Gradle build:

```groovy
dependencies {
    implementation project(':executors-core')   // the executor, tree, policies, SPIs
    implementation project(':executors-jfr')    // optional: one Flight Recorder event per task
}
```

| Module | Adds | Depends on |
|---|---|---|
| `executors-core` | everything on this page | `slf4j-api` only |
| `executors-jfr` | `JfrTaskListener` — executor tasks in JDK Mission Control, see `jfr/README.md` | core + the JDK's `jdk.jfr` |
| `executors-observability` | `ExecutionRecorder` — a bounded per-executor history tree with permanent aggregates, text/JSON rendering and a JMX view, see `observability/README.md` | core |
| `executors-adaptive` | `AdaptiveSizing` — learns each task's memory cost (allocation, solo heap peak) per task name, remembers it across restarts, and decorates any policy so tasks submitted with no size — or with an input size and a learned multiplier — are throttled on what was measured, see `adaptive/README.md` | core |

Core's only exported dependency is `slf4j-api` — it does not force a logging
backend (or a logging configuration) onto consumers. Satellites may register
their listener under `META-INF/services`; a root picks those up only when it
asks (`root.addDiscoveredListeners()`), never by a jar's mere presence.

## Quick start — no throttling

```java
import io.github.grishagrigorov.executors.ThrottledExecutor;

ThrottledExecutor executor = new ThrottledExecutor("MyBatch");

// A value-returning lambda matches both submit(Callable) and submit(Supplier),
// so it needs a cast to pick one (they behave identically for a pure lambda):
CompletableFuture<Report> report = executor.submit((Callable<Report>) () -> buildReport());
CompletableFuture<Void>   upload = executor.submit(this::uploadYesterday);    // Runnable — unambiguous

executor.waitForCompletion();   // blocks; throws ThreadContextAwareCompletionException on failure
```

Already have threads, pools and futures of your own? *Migrating existing code*,
below, maps each of those shapes onto the library, one recipe per idiom.

## Throttled submission — zero config

Wrap each job in a `ThrottledTask(job, estimatedSizeBytes, name)` and call
`submitThrottled`. With no policy argument the fail-safe default applies: the
**process-wide** heap budget (85% of max heap, shared by every default-policy
user in the JVM — the heap is process-wide, so the default budget is too) AND a
concurrency cap sized for this executor's backend.

```java
import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;

List<ThrottledTask<Runnable>> tasks = List.of(
        new ThrottledTask<>(() -> loadVoyageHistory(), 500_000_000L, "VoyageHistory"),
        new ThrottledTask<>(() -> loadNoonReports(),    50_000_000L, "NoonReports"),
        new ThrottledTask<>(() -> loadFuelCurves(),     20_000_000L, "FuelCurves"));

ThrottledExecutor executor = new ThrottledExecutor("CacheWarmup");
executor.submitThrottled(tasks);              // AdmissionPolicy.defaultPolicy(backend)
executor.waitForCompletion();
```

The task list you pass in is **never mutated** — immutable lists such as
`List.of(...)` are fine. If you want the old isolated-accounting default (each
executor tree budgeted separately), use `AdmissionPolicy.perExecutorDefaultPolicy()`.

`ThrottledTask.of(...)` is the shorter spelling, and a fourth argument attaches
**tags** — free-form labels naming the *resources* a body uses, which a policy
can meter separately from overall concurrency:

```java
ThrottledTask.of(() -> loadFuelCurves(), 20_000_000L, "FuelCurves", Set.of("hsqldb"));
```

Tags are copied at construction and immutable afterwards; an untagged task has
an empty set, never `null`. Note that a task is an **identity, not a value**:
two tasks describing the same work are two pieces of work, charged and released
independently, so `ThrottledTask` deliberately has no `equals`/`hashCode`.

### Getting the results back

`submitThrottled` returns one future per task, **in the order you listed them** —
not the order the policy chose to admit them:

```java
List<CompletableFuture<?>> futures = executor.submitThrottled(tasks);
executor.waitForCompletion();
Object first = futures.get(0).join();          // the future for tasks.get(0)
```

If all you want are the values, `invokeAllThrottled` submits, waits and returns
them, throwing the same aggregate exception `waitForCompletion()` would:

```java
List<ThrottledTask<Callable<Report>>> batch = List.of(
        ThrottledTask.of(() -> buildNoon(day), 40L << 20, "NoonReport"),
        ThrottledTask.of(() -> buildLeg(leg),  90L << 20, "LegReport"));

List<Report> reports = executor.invokeAllThrottled(batch);   // submission order
```

Two details worth knowing. The same task instance listed twice gets two distinct
futures, one per position — the ledgers charge per claim, so that really is two
pieces of work. And if the submitting thread is interrupted mid-batch, the tasks
that were abandoned come back as already-cancelled futures rather than going
missing, so element *i* always describes task *i*.

## Throttled submission — streaming

The `List` overload materialises the whole batch. A producer that discovers work
incrementally — paging a table, reading a feed — would have to build every task
up front, so a hundred thousand of them sit in memory before the first one runs.
Pass an `Iterator` instead and only a bounded **window** is ever held:

```java
try (Stream<Voyage> voyages = repository.streamAll()) {
    executor.submitThrottled(
            voyages.map(v -> ThrottledTask.of(() -> reindex(v), 8L << 20, "Reindex")).iterator(),
            AdmissionPolicy.defaultPolicy(), 64);
}
executor.waitForCompletion();
```

Two things differ, both consequences of not knowing the batch in advance:

- **Ordering applies within the window only.** `selectionOrder()` sorts the
  candidates currently held, so a large task far down the stream cannot be
  pulled forward past a window of small ones. A window at least as large as the
  stream behaves exactly like the `List` overload.
- **Validation is per candidate, as it is pulled.** The `List` overload rejects
  an unsupported job type before anything is claimed; a stream cannot promise
  that, so an unsupported job halfway down throws *after* the tasks before it
  have already run.

It returns nothing — a list of futures for an unbounded stream would reintroduce
exactly the retention streaming exists to avoid. Use a listener, or
`getFutures()`, to observe individual tasks.

## Throttled submission — composing built-in policies

Policies combine with `CompositePolicy.allOf(...)` (logical AND, short-circuits
on the first refusal):

```java
import io.github.grishagrigorov.executors.throttling.*;

AdmissionPolicy policy = CompositePolicy.allOf(
        new MemoryBudgetPolicy(2_000_000_000L, 1.5),                    // ceiling bytes, safety multiplier
        new MaxConcurrencyPolicy(10),                                   // at most 10 in flight
        new RateLimitPolicy(20, Duration.ofSeconds(1), InstantSource.system())); // ≤ 20 starts/second

executor.submitThrottled(tasks, policy);
executor.waitForCompletion();
```

### Built-in policies

| Policy | Admits while… | Scope |
|--------|---------------|-------|
| `MemoryBudgetPolicy(ceilingBytes, multiplier)` | `(trackedSizeBytes + candidate.size) * multiplier < ceiling` | One executor **tree** (reads the per-tree snapshot) |
| `GlobalMemoryBudgetPolicy(ceilingBytes, multiplier)` | same formula, against its **own ledger** | Every executor sharing the instance — including **unrelated roots** |
| `MaxConcurrencyPolicy(max)` | fewer than `max` tasks in flight | One executor tree |
| `RateLimitPolicy(maxStarts, window, clock)` | fewer than `maxStarts` starts in the sliding `window` | Every executor sharing the instance |
| `HeapPressurePolicy.defaults()` | post-GC heap occupancy is under 85% (resumes below 75%) | The **JVM** — it reads the heap, not a ledger |
| `ResourcePermitPolicy(Map.of("hsqldb", 4, …))` | every tag on the candidate is under its limit | Every executor sharing the instance |
| `CompositePolicy.allOf(p1, p2, …)` | **every** member admits | — |

Every policy but one is **predictive**: it sums the sizes you declared (or, with
`executors-adaptive`, the sizes it learned). That makes them blind to a task that
under-declared, a task submitted with no estimate at all, and every byte in the
JVM that is not a task. `HeapPressurePolicy` is the reactive counterpart — it
looks at how full the heap actually is:

```java
AdmissionPolicy policy = CompositePolicy.allOf(
        GlobalMemoryBudgetPolicy.processWideHeapBudget(),   // what the batch says it needs
        HeapPressurePolicy.defaults(),                      // what the heap says it has
        new MaxConcurrencyPolicy(backend.defaultMaxConcurrency()));
```

The signal is post-collection occupancy (`MemoryPoolMXBean.getCollectionUsage()`
summed over the heap pools ÷ `maxMemory()`), so it measures what is *retained*
rather than refusing work on a heap that is merely due a GC. It is
collector-agnostic — plain `java.lang.management`, no pool-name matching — and
has no background thread: the reading is taken during `admit` and memoised for
50ms. Two thresholds rather than one, because a single line makes it chatter.

`ResourcePermitPolicy` answers a different question: `MaxConcurrencyPolicy` bounds
the pool as a whole, which says nothing about the four connections the database
will actually give you. Tag the tasks with the resources they use and meter those
separately:

```java
AdmissionPolicy policy = CompositePolicy.allOf(
        new MaxConcurrencyPolicy(16),                                // the pool
        new ResourcePermitPolicy(Map.of("hsqldb", 4, "spmbb", 2)));  // what is behind it

executor.submitThrottled(List.of(
        ThrottledTask.of(() -> loadVoyages(), 40L << 20, "Voyages", Set.of("hsqldb")),
        ThrottledTask.of(() -> pollSensors(),  4L << 20, "Sensors", Set.of("spmbb"))), policy);
```

A task holding several tags takes them together or not at all — one admitted with
only some of its resources would hold them while waiting for the rest, which is
how deadlocks get built. Untagged tasks, and tags with no configured limit, are
unconstrained: it only ever says no about resources you told it to meter.

Under sustained pressure the liveness floor still admits one task at a time, so
a batch degrades to serial execution rather than deadlocking on a heap only that
batch can drain. It is **not** in `AdmissionPolicy.defaultPolicy(...)`: a
reactive brake changes behaviour for every existing caller, so opt in.

The tree/global distinction matters: sharing a `MemoryBudgetPolicy` instance
across unrelated executors does **nothing**, because each hands it a snapshot
that knows only about that executor's own tree. `GlobalMemoryBudgetPolicy`
ignores the snapshot and counts through its `onTaskStart`/`onTaskEnd` hooks,
so one shared instance genuinely spans subsystems:

```java
// One budget for two unrelated subsystems:
var heapBudget = new GlobalMemoryBudgetPolicy(
        (long) (Runtime.getRuntime().maxMemory() * 0.85), 1.0);

reportExecutor.submitThrottled(reportTasks, heapBudget);   // root 1
importExecutor.submitThrottled(importTasks, heapBudget);   // root 2, same ledger
```

An idle executor's liveness floor defers to the shared ledger (via
`AdmissionPolicy.inFlightTaskCount()`), so it cannot admit past a budget that
another subsystem has already filled.

## Writing your own policy

Implement `AdmissionPolicy`. The contract: `admit(candidate, snapshot)` must be
**fast and thread-safe**, and free of side effects beyond the policy's own
accounting — it runs under the framework's admission lock. Optionally override
`selectionOrder()` to control which queued task is offered first (default:
largest first), and the `onTaskStart`/`onTaskEnd` hooks for internal state. If
your policy tracks work across executors, also override `inFlightTaskCount()`
so the liveness floor keys on the larger of your ledger and the local tree's
count (return `-1` when tracking nothing), and `ledgerSpansExecutors()` to say
whether that ledger reaches past one executor tree — it decides whose parked
claims the re-entrancy exemption counts. Per-scan state belongs on
`onScanStart()`, the lifecycle hook the harness announces before the
exclusivity and admission questions.

Wrapping an existing policy? Extend `ForwardingAdmissionPolicy` and override
only what you change — it forwards the whole SPI by build-enforced contract (a
reflective test rejects any SPI method the base does not declare, so a method
added later cannot ship unforwarded), where a hand-rolled pass-through inherits
interface defaults that answer for the wrapper (`inFlightTaskCount` hides the
delegate's ledger, `mayRequireExclusive` its isolation) and degrades silently
each time the SPI grows. One answer it derives instead of forwarding, so the
coupled pair stays safe: declare your own `requiresExclusive` and it answers
`mayRequireExclusive` with "may". The ledger pair is not guessed at — declare
your own `inFlightTaskCount` and override `ledgerSpansExecutors` with it.

```java
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import io.github.grishagrigorov.executors.throttling.PoolSnapshot;
import java.util.Comparator;

/**
 * Runs small tasks freely, but admits a large task only while total in-flight
 * memory stays under 100MB. Offers the smallest queued task first.
 */
public final class SmallTasksFirstPolicy implements AdmissionPolicy {

    private static final long LARGE = 10L * 1024 * 1024;
    private static final long QUIET = 100L * 1024 * 1024;

    @Override
    public boolean admit(ThrottledTask<?> candidate, PoolSnapshot snapshot) {
        return candidate.size < LARGE
                || snapshot.trackedSizeBytes() + candidate.size < QUIET;
    }

    @Override
    public Comparator<ThrottledTask<?>> selectionOrder() {
        return Comparator.comparingLong(task -> task.size);   // smallest first
    }
}
```

## Guarantees and gotchas

- **Liveness floor.** When nothing is in flight, the next candidate is admitted
  no matter what the policy says. Even an always-refuse policy degrades to
  sequential execution, never to a deadlock.
- **Atomic admission.** Scanning and claiming are one step under the
  framework's lock — coordinators sharing a tree cannot double-claim. Across
  *independent roots* sharing a global policy, admission is not one atomic
  step: at a moment of global idleness, simultaneously-scanning roots can each
  floor-admit one task. Transient, bounded by the number of such roots, and
  only at global zero — a fail-safe budget, not an exact reservation system.
- **No lost wake-ups.** Completion signalling is unconditional and
  framework-owned; the blocking wait is versioned. A completion wakes waiters
  in its own tree immediately; *other* roots watching a shared global policy
  re-check on the ~100 ms poll.
- **Fair admission.** Several coordinators can share one tree's harness (a
  child's batch and its parent's, say). Each holds a place in an admission
  queue for the length of its batch; one refused three scans in a row takes
  priority, and everyone else yields until it has claimed. At rest the queue is
  empty and costs nothing; under contention no coordinator can be passed over
  indefinitely. An abandoned batch releases its place.
- **Exclusive admission.** A policy can answer `requiresExclusive(task)` for a
  task that must run alone. The harness admits it only into an empty pool — and
  then *without* consulting `admit`, since a policy that wants isolation should
  not also have to approve the task on its merits — holds everything else back
  while it runs, and gives its coordinator priority at once, so a steady stream
  of ordinary work cannot keep it waiting for an emptiness that never comes.
  This is the primitive `executors-adaptive`'s first-run quarantine is built on.
- **Sizes can be learned.** If you cannot estimate a task's size — or only know
  its input size — `executors-adaptive` measures the first run, remembers it per
  task name, and resizes later runs before your policy sees them
  (`adaptive.policy(yourPolicy)`). See `adaptive/README.md`.
- **`ThrottledTask.size` is your estimate.** The pool never measures actual heap
  usage; throttling quality is only as good as the estimates.
- **Interruption.** If the submitting thread is interrupted while blocked, the
  interrupt flag is restored and `submitThrottled` returns; tasks not yet
  submitted are abandoned (and logged).
- **Exclusive admission.** A policy's `requiresExclusive(task)` makes the
  framework run that task alone: it is admitted only once nothing is in flight —
  and then *without* consulting `admit`, since a policy that asked for isolation
  should not also have to approve the task on its merits — and while it runs,
  nothing else is admitted from any coordinator sharing the harness. A composite
  is exclusive if **any** member is; isolation is not something a member can
  veto. A coordinator holding a blocked exclusive candidate stops admitting from
  behind it, so its own batch waits too — otherwise it competes with itself and
  the pool never drains. Isolation **yields to progress**: work submitted from
  inside an already-claimed task is exempt, because the pool cannot empty while
  the caller is holding a claim.
- **Fairness.** Coordinators sharing a harness used to race: `awaitStateChange`
  is a bare `signalAll`, so whoever woke first won, every time, and a steady
  stream of work from one coordinator could keep another's candidate out
  indefinitely. Each coordinator now holds a **ticket** for the length of its
  batch. After three consecutive refused scans a ticket takes priority, and
  while any privileged ticket exists only the oldest may claim. A coordinator
  holding a blocked *exclusive* candidate takes priority at once — that is what
  drains the pool for it, and what makes the old adaptive-quarantine starvation
  impossible rather than merely unlikely. Priority is deliberately not immediate:
  strict FIFO would let one coordinator's oversized task block everyone else's
  work behind it. An abandoned batch releases its place, so nothing can wedge the
  harness.
- **One waiter per executor.** `waitForCompletion()` is intended to be called
  from a single thread per executor instance; multiple callers log a warning.
  That holds for the bounded overload too — a deadline does not make two waiters
  safe.
- **Bounded waiting.** `waitForCompletion(Duration)` returns a `WaitResult`
  instead of blocking indefinitely:

  ```java
  WaitResult result = executor.waitForCompletion(Duration.ofMinutes(2));

  if (!result.complete()) {
      LOGGER.warn("still running: {}", result.outstanding());   // TaskDescriptors
      executor.cancelSubtree();
  }
  ```

  A timeout is **not** an error — nothing is cancelled and the tasks keep
  running, so it is reported rather than thrown. Failures still throw: if a task
  has already failed when the budget expires, the aggregate is raised exactly as
  the untimed wait raises it, because a deadline should not swallow a real
  failure. The budget covers the whole wait, not each quiescence pass, so a
  subtree that keeps spawning work cannot renew it. Zero or negative is a single
  non-blocking check.
- **Failure aggregation.** `waitForCompletion()` collects every failure in the
  subtree and throws one `ThreadContextAwareCompletionException`: the first
  failure is the cause (root-cause walking works), the rest are attached as
  suppressed exceptions (standard stack-trace rendering shows them all), and
  the message carries the executor path and failure count. Failures are
  recorded **raw** — the cause chain is `aggregate → per-task wrapper → your
  exception`, with no intermediate `CompletionException` — and a checked
  exception from a `Callable` lands in the future as-is rather than wrapped in
  a `RuntimeException`. `Error`s are aggregated like any other throwable.
  Recording happens *before* a task's future completes, so a waiter can never
  observe a finished batch whose failures are still being collected.
- **Cancellation.** `future.cancel()` on a queued task prevents it from running
  and runs listeners' `onComplete` synchronously on the calling thread; on a
  running task the future is cancelled immediately and
  `onComplete(CANCELLED_AFTER_START)` fires when the body actually returns.
  Whether the body *stops* depends on the backend: under
  `VIRTUAL_THREAD_PER_TASK` the running thread is interrupted, so a body blocked
  on IO or a latch returns promptly; under `FORK_JOIN` and `CALLER_RUNS` it runs
  to its natural end, because the worker outlives the task and an interrupt set
  on it would surface inside somebody else's work. A cancelled batch is reported as a failed batch by
  `waitForCompletion()`. `cancel()` is the only supported way to complete a
  task's future from outside — `complete`/`obtrude*`/`orTimeout` bypass the
  completion machinery.
- **Future retention.** Completed futures (and their job lambdas) are compacted
  from the tracking queue every 128 submissions — recent batches stay
  inspectable via `getFutures()`, and a long-lived executor does not accumulate
  its whole history.
- **Shutdown.** `shutdownThreadExecutors()` stops every pool the library
  created (and only those that were actually created). Pools are lazily held,
  so using the library afterwards simply creates fresh ones; all library
  threads are daemons, so nothing pins the JVM either way.

## Migrating existing code

Most code that needs this library already has *some* concurrency in it — a
`new Thread(...).start()`, an `ExecutorService` per class, a `Future` you
`get()` at the end. This section shows each of those shapes and what it
becomes. Every "after" snippet is lifted from
`core/src/test/java/io/github/grishagrigorov/executors/TutorialTests.java`,
which runs in the ordinary suite, so the guide cannot drift from what the
library does; the test named beside each recipe is the runnable version, with
its assertions.

| You have | You write | You gain |
|---|---|---|
| `new Thread(r).start()` | `executor.submit(r)` | a future, a wait, and the exception that used to vanish |
| `pool.submit(runnable)` + `shutdown()`/`awaitTermination()` | `executor.submit(runnable)` + `waitForCompletion()` + `close()` | no pool of your own; the executor *is* an `ExecutorService` |
| `Future<T> f = pool.submit(callable); f.get()` | `CompletableFuture<T> f = executor.submit(callable); f.join()` | `thenApply`/`thenAccept` composition; checked exceptions arrive as-is |
| `pool.invokeAll(callables)` / a loop of `submit` then `get` | `executor.invokeAllThrottled(tasks[, policy])` | values in submission order; admission bounded by memory and concurrency |
| a thread per element of a stream | `executor.submitThrottled(iterator, policy, window)` | a bounded window over an unbounded source |
| `f.get(55, SECONDS)` / `awaitTermination(timeout)` | `executor.waitForCompletion(Duration)` → `WaitResult` | *which* tasks are still outstanding, no `TimeoutException` |
| `thread.interrupt()` / `shutdownNow()` + a cancel loop | `future.cancel(true)` / `root.cancelSubtree()` | the whole tree, depth-first, one call |
| a `Runnable` that is also a `Future` (`FutureTask`, JavaFX `Task`) | `executor.submit(task); task.get()` | unchanged — it still works |

One thing to know before the recipes: `submit` is overloaded for `Runnable`,
`Callable` and `Supplier`, and a lambda that *returns a value* — or a block
body that only throws — matches more than one of them. The compiler tells you
so; a cast settles it (`(Callable<Report>) () -> buildReport()`,
`(Runnable) () -> { throw ...; }`), and the three behave identically for a
pure lambda.

### A. `new Thread(...).start()` — `TutorialTests.FireAndForget`

Before:

```java
new Thread(() -> auditLog.flush()).start();
```

Nobody holds the thread, nobody learns whether it finished, and an exception
inside it goes to the default uncaught-exception handler — usually stderr,
usually unread.

After:

```java
final ThrottledExecutor executor = new ThrottledExecutor("Housekeeping");

executor.submit(() -> flushed.set(true));   // the thread you used to start

executor.waitForCompletion();               // the join you never had
executor.close();
```

The exception the thread used to swallow now surfaces from `waitForCompletion()`
as a `ThreadContextAwareCompletionException`, with your exception reachable
through `RootCauseExceptionUtil.getRootCause(aggregate)`:

```java
// A block body that only throws matches Callable and Supplier too, so say Runnable.
executor.submit((Runnable) () -> {
    throw new IllegalStateException("audit log is read-only");
});

executor.waitForCompletion();   // throws: root cause is the IllegalStateException
```

### B. `ExecutorService` + `Runnable` — `TutorialTests.RunnableOnAPool`

Before:

```java
ExecutorService pool = Executors.newFixedThreadPool(4);
pool.submit(() -> reindex());
pool.shutdown();
pool.awaitTermination(1, TimeUnit.MINUTES);
```

A pool per call site, hand-rolled shutdown bookkeeping, and a `Future<?>`
whose failure you only see if you remember to `get()` it.

After:

```java
final ThrottledExecutor executor = new ThrottledExecutor("Reindex");

final CompletableFuture<Void> done = executor.submit((Runnable) reindexed::incrementAndGet);

executor.waitForCompletion();   // replaces shutdown() + awaitTermination(...)
executor.close();               // no pool of your own to tear down
```

The pools are the library's, shared and lazily created (see *Backends*), so
there is nothing per call site to size, name or shut down. And because
`ThrottledExecutor` extends `AbstractExecutorService`, code you are not
ready to touch keeps working — hand it the executor where it asks for a pool:

```java
/** A method you do not want to touch yet: it only knows {@link ExecutorService}. */
private void legacyMethodThatTakesAPool(ExecutorService pool, Runnable work) {
    pool.execute(work);
}

legacyMethodThatTakesAPool(executor, () -> ran.set(true));
executor.waitForCompletion();
```

### C. `Callable` + `Future` — `TutorialTests.CallableWithAFuture`

Before:

```java
Future<Integer> f = pool.submit(() -> countRows());
Integer rows = f.get();     // blocks; wraps everything in ExecutionException
```

A `java.util.concurrent.Future` can only be polled or blocked on — there is no
"and then".

After:

```java
final ThrottledExecutor executor = new ThrottledExecutor("RowCount");

// A value-returning lambda matches both submit(Callable) and submit(Supplier),
// so a cast picks one; they behave identically for a pure lambda.
final CompletableFuture<Integer> rows = executor.submit((Callable<Integer>) () -> 21);
final CompletableFuture<Integer> doubled = rows.thenApply(n -> n * 2);

doubled.join();      // 42 — join(): no checked ExecutionException
executor.close();
```

A checked exception thrown by the `Callable` lands in the future **as-is** —
`get()` reports it as the `ExecutionException`'s cause, not wrapped in a
`RuntimeException` — and it is in the batch report too, so a caller who never
looks at the individual future still learns about it from
`waitForCompletion()`:

```java
final CompletableFuture<Integer> rows = executor.submit((Callable<Integer>) () -> {
    throw new IOException("connection reset");
});
// rows fails with cause IOException; executor.waitForCompletion() throws the aggregate.
```

### D. Fan-out: `invokeAll` or a loop of submits — `TutorialTests.FanOut`

Before:

```java
List<Future<Report>> futures = pool.invokeAll(callables);
for (Future<Report> f : futures) { reports.add(f.get()); }
```

or the hand-rolled version: a loop of `submit()`, then a loop of `get()`.
Either way every task starts as soon as a worker is free, whatever it is
about to allocate.

After — wrap each job in a `ThrottledTask` with its estimated peak heap and a
name, and let the executor decide when each may start:

```java
final ThrottledExecutor executor = new ThrottledExecutor("Reports");
final List<ThrottledTask<Callable<String>>> reports = List.of(
        ThrottledTask.of(() -> "voyage", 50_000_000L, "VoyageReport"),
        ThrottledTask.of(() -> "leg", 5_000_000L, "LegReport"),
        ThrottledTask.of(() -> "noon", 500_000L, "NoonReport"));

final List<String> values = executor.invokeAllThrottled(reports);   // ["voyage", "leg", "noon"]
executor.close();
```

The values come back in **submission order** whatever order the policy admitted
them in, and `invokeAllThrottled` waits for the whole batch and throws the
aggregate if any task failed — the loop of `get()`s, folded in. With no policy
argument the default applies (85% of heap, process-wide, plus a concurrency cap
for the backend). Pass one to bound the batch explicitly — the thing a loop of
submits could never do:

```java
final List<Integer> ids = executor.invokeAllThrottled(reports, new MaxConcurrencyPolicy(2));
// at most two report bodies ever ran at once; ids is [0, 1, ..., 11]
```

### E. A thread per element of a stream — `TutorialTests.Streaming`

Before:

```java
while (cursor.hasNext()) {
    Row row = cursor.next();
    new Thread(() -> process(row)).start();
}
```

As many threads as there are rows, all at once, and nothing to wait on
afterwards.

After — hand over the iterator and a window; the executor pulls from it only
as fast as admission allows, holding at most `window` un-admitted tasks:

```java
final ThrottledExecutor executor = new ThrottledExecutor("Rows");

final Iterator<ThrottledTask<Runnable>> rows = IntStream.range(0, 1_000)
        .mapToObj(row -> ThrottledTask.of((Runnable) processed::incrementAndGet, 1_000L, "Row-" + row))
        .iterator();

executor.submitThrottled(rows, new MaxConcurrencyPolicy(4), 16);   // at most 16 pulled ahead

executor.waitForCompletion();
executor.close();
```

The source is never drained into memory, which is the point when it is a
database cursor or a file. `submitThrottled(Iterator)` returns nothing on
purpose — a list of futures would grow with the stream; use
`waitForCompletion()` or a listener to observe the results.

### F. A timed wait — `TutorialTests.TimedWait`

Before:

```java
Result r = task.get(55, TimeUnit.SECONDS);    // TimeoutException, and now what?
pool.awaitTermination(55, TimeUnit.SECONDS);  // true/false, and which one hung?
```

After:

```java
final WaitResult sofar = executor.waitForCompletion(Duration.ofMillis(200));

sofar.complete();       // false: something is still running
sofar.outstanding();    // [SlowUpload] — the descriptors of what is
```

A timeout is a *report*, not an error: nothing is cancelled, the tasks keep
running, and you decide — log the stragglers, wait again, or
`cancelSubtree()`. Failures still throw, so a deadline cannot hide one. Wait
again once the cause clears:

```java
networkIsBack.countDown();
executor.waitForCompletion(Duration.ofSeconds(10)).complete();   // true
```

### G. Cancellation — `TutorialTests.Cancellation`

Before:

```java
thread.interrupt();                 // and hope the body checks the flag
future.cancel(true);
for (Runnable r : pool.shutdownNow()) { ((Future<?>) r).cancel(true); }
```

After, for one task:

```java
final CompletableFuture<Void> sync = executor.submit(() -> {
    started.countDown();
    try {
        release.await();
    } catch (InterruptedException e) {
        Thread.currentThread().interrupt();     // the virtual backend interrupts; pooled ones do not
    }
});

sync.cancel(true);
sync.isCancelled();     // true, immediately, whatever the body does next
```

The future settles at once; the listener's `onComplete` reports
`CANCELLED_AFTER_START` when the body actually ends. Whether the body is
*interrupted* depends on the backend — under `VIRTUAL_THREAD_PER_TASK` the
thread is the task's own and gets the interrupt; under `FORK_JOIN` and
`CALLER_RUNS` the worker outlives the task, so the body runs to its natural
end (see *Guarantees and gotchas*). Write bodies that check the flag, exactly
as before.

For everything at once, `cancelSubtree()` replaces `shutdownNow()` plus the
loop over what it returned — it closes and cancels the executor and every
live descendant, depth-first:

```java
final ThrottledExecutor root = new ThrottledExecutor("Sync");
final ThrottledExecutor child = root.newChild("Pages");

final CompletableFuture<Void> page = child.submit(() -> { /* ... */ });

root.cancelSubtree();                            // closes and cancels root, child, everything queued

page.isCancelled();     // true
root.isClosed();        // true; child.isClosed() too
root.submit(() -> { }); // a closed executor rejects through the future — never throws
```

### H. A `Runnable` that is also a `Future` — `TutorialTests.RunnableFutures`

`FutureTask`, and `javafx.concurrent.Task` with it, is the `Runnable` *and* the
`Future` — the object you submit is the object you read. That idiom survives
unchanged:

```java
final ThrottledExecutor executor = new ThrottledExecutor("UserPool");
final FutureTask<String> task = new FutureTask<>(() -> "cargo pairs");

executor.submit(task);                           // submit(Runnable)

task.get(10, TimeUnit.SECONDS);                  // "cargo pairs"
executor.waitForCompletion();
executor.close();
```

Note that the *executor's* future for such a task completes when `run()`
returns, and `run()` of a `FutureTask` never throws — the failure is inside
`task.get()`. Read the task's own future for the value and the failure, as you
always did; use the executor's for waiting and cancellation.

### If your code uses `ThreadManager` / `ThreadPool`

The ship and office apps funnel everything through
`ThreadManager.runAt(Destination, task)` onto four static pools in
`ThreadPool`, with JavaFX `Task` subclasses as the unit of work. Mapped onto
the recipes above:

| Today | Tomorrow |
|---|---|
| `ThreadManager.runAt(Destination.USER_THREAD_POOL, task)` | `userExecutor.submit(task)` — recipe H; keep one long-lived executor per former pool |
| `ThreadManager.runAt(Destination.SYSTEM_THREAD_POOL, task)` | `systemExecutor.submit(task)` — same; give it `new MaxConcurrencyPolicy(1)` on throttled batches if the serialisation was load-bearing |
| `ThreadManager.runAt(Destination.APPLICATION_THREAD, r)` | **unchanged** — dispatching onto the FX thread is not this library's job |
| `ThreadManager.runAt(Destination.ON_THE_CURRENT_THREAD, r)` | `ThrottledExecutor.withBackend("X", ExecutorBackend.CALLER_RUNS)` when you want the same instrumentation on inline work; otherwise just call `r.run()` |
| `ThreadPool.X.submit(task); task.get(55, SECONDS)` | recipe H for the value; `waitForCompletion(Duration)` (recipe F) when it is the *batch* you are waiting on |
| `ThreadPool.TCP_THREAD_POOL_*` (unbounded cached pools for socket work) | `withBackend("Tcp", ExecutorBackend.VIRTUAL_THREAD_PER_TASK)` — blocking IO is what that backend is for |
| `ThreadPool.shutdownNow()` + the `instanceof FutureTask / Task / Service` cancel loop | `root.cancelSubtree()` — recipe G |

Two behaviours of `runAt` do **not** carry over, deliberately. It runs the
task *inline* when it believes it is already on the target pool's thread
(decided by reference-equality on the thread *name*), so a call site never
knows whether it is about to block; the library never does that — `submit`
always goes to the backend, and `CALLER_RUNS` is the explicit, per-executor
way to ask for inline execution. And `runAt` discards the pool's `Future`, so
a failing task is invisible; here every submission returns a future and every
failure reaches `waitForCompletion()`.

## Building and testing

```bash
./gradlew :executors-core:build          # compile + jar + both test suites
./gradlew :executors-core:test           # main suite
./gradlew :executors-core:shutdownTest   # shutdown tests — isolated JVM, they kill the shared pools

# Run the entire suite on the virtual-thread backend:
./gradlew :executors-core:test -Dio.github.grishagrigorov.executors.backend=virtual
```

Main sources are pinned to the Java 17 API (`options.release = 17`) and the
emitted bytecode is guarded by a test; test sources build on the full Java 25
toolchain so they can assert on virtual threads directly. Tests are JUnit 5
with AssertJ and Awaitility — `throttling/ThrottlePolicyTests`,
`ExecutorTreeTests` and `TaskExecutionListenerTests` serve as executable
documentation of the policy, tree and listener guarantees respectively;
`TaskRunTests` drives the completion state machine deterministically;
`TutorialTests` is the *Migrating existing code* section, runnable.

## License

Licensed under the [Apache License, Version 2.0](LICENSE). Copyright 2026 Grigor Grigorov.
