# Architecture in six diagrams

A structural map of the `executors` family. Each diagram was drawn from the sources it
links to; when the two disagree, the source wins and the diagram needs fixing. The root
[README](../README.md) is the tutorial, [CLAUDE.md](../CLAUDE.md) holds the rules, and
`ThrottleHarness`'s class Javadoc is the reference for admission mechanics.

Conventions: `<<package-private>>` and `<<not exported>>` mark types you cannot reach from
outside the module; everything else in a diagram is public API. Method lists are the ones
worth knowing, not the complete surface.

1. [The executor tree and its futures](#1-the-executor-tree-and-its-futures)
2. [Admission policies](#2-admission-policies)
3. [Listeners and the satellites](#3-listeners-and-the-satellites)
4. [Life of a throttled task](#4-life-of-a-throttled-task)
5. [Java modules](#5-java-modules)
6. [The per-task state machine](#6-the-per-task-state-machine)

## 1. The executor tree and its futures

`ThrottledExecutor` is the entry point and the only large class. A child shares its
parent's `ThrottleHarness`, so "one harness per tree" is a structural fact, not a
convention. Each submission becomes a `TaskRun` that owns the `TaskFuture` handed back to
the caller; the run, not the future, is the state machine (diagram 6).

Sources: [`ThrottledExecutor.java`](../core/src/main/java/io/github/grishagrigorov/executors/ThrottledExecutor.java),
[`ThrottledTask.java`](../core/src/main/java/io/github/grishagrigorov/executors/ThrottledTask.java),
[`FuturePair.java`](../core/src/main/java/io/github/grishagrigorov/executors/FuturePair.java),
[`WaitResult.java`](../core/src/main/java/io/github/grishagrigorov/executors/WaitResult.java),
[`internal/ThrottleHarness.java`](../core/src/main/java/io/github/grishagrigorov/executors/internal/ThrottleHarness.java).

```mermaid
classDiagram
    direction TB

    class AbstractExecutorService {
        <<java.util.concurrent>>
    }

    class ThrottledExecutor {
        +String executorId
        +build(idPrefix, parent)$ ThrottledExecutor
        +withBackend(idPrefix, backend)$ ThrottledExecutor
        +withAmbientDiscovery(idPrefix)$ ThrottledExecutor
        +currentAmbient()$ ThrottledExecutor
        +newChild(idPrefix) ThrottledExecutor
        +parent() ThrottledExecutor
        +root() ThrottledExecutor
        +executorPath() String
        +backend() ExecutorBackend
        +addListener(listener) ThrottledExecutor
        +addDiscoveredListeners() ThrottledExecutor
        +removeListener(listener) boolean
        +submit(Runnable or Callable or Supplier) CompletableFuture
        +submitThrottled(List tasks, policy, clearCompleted) List~CompletableFuture~
        +submitThrottled(Iterator tasks, policy, window) void
        +invokeAllThrottled(List tasks, policy) List~R~
        +waitForCompletion() boolean
        +waitForCompletion(Duration) WaitResult
        +waitForOwnCompletion() boolean
        +getFutures() Stream~FuturePair~
        +cancelSubtree() void
        +shutdownNow() List~Runnable~
        +close() void
        +shutdownThreadExecutors()$ void
    }

    class ExecutorBackend {
        <<enumeration>>
        FORK_JOIN
        VIRTUAL_THREAD_PER_TASK
        CALLER_RUNS
        +defaultBackend()$ ExecutorBackend
        +defaultMaxConcurrency() int
        +isAvailable() boolean
    }

    class ThrottledTask~T~ {
        +T job
        +long size
        +String name
        +Set~String~ tags
        +of(job, size, name, tags)$ ThrottledTask
        +hasTag(tag) boolean
    }

    class TaskRun~T~ {
        <<package-private>>
        -AtomicInteger state
        ~execute() void
        ~cancelRequested(mayInterrupt) boolean
    }

    class TaskFuture~T~ {
        <<package-private>>
        +cancel(mayInterrupt) boolean
    }

    class CompletableFuture~T~ {
        <<java.util.concurrent>>
    }

    class FuturePair {
        ~FuturePair(future, job, descriptor)
        +getCompletableFuture() CompletableFuture
        +getJob() Object
        +getDescriptor() TaskDescriptor
    }

    class WaitResult {
        <<record>>
        +boolean complete
        +List~TaskDescriptor~ outstanding
    }

    class ThreadContextAwareCompletionException {
        +executorPath() String
        +getCause() Throwable
    }

    class ThrottleHarness {
        <<not exported>>
        +newTicket() Ticket
        +snapshot() PoolSnapshot
        +stateVersion() long
        +admitFirst(candidates, policy, ticket, reentrant) ThrottledTask
        +onEnd(task, policy) void
        +awaitStateChange(version, timeoutMillis) void
        +holdingAClaim(body)$ Callable
    }

    class Ticket {
        <<AutoCloseable>>
    }

    AbstractExecutorService <|-- ThrottledExecutor
    ThrottledExecutor "1" --> "0..1" ThrottledExecutor : parent
    ThrottledExecutor "1" --> "*" ThrottledExecutor : children (weak refs)
    ThrottledExecutor --> ThrottleHarness : one per tree, shared by children
    ThrottledExecutor --> ExecutorBackend : choice, pools are static
    ThrottledExecutor "1" --> "*" FuturePair : futures, compacted every 128
    ThrottledExecutor ..> ThrottledTask : accepts
    ThrottledExecutor ..> TaskRun : one per submission
    ThrottledExecutor ..> WaitResult : waitForCompletion(Duration)
    ThrottledExecutor ..> ThreadContextAwareCompletionException : waitForCompletion() throws
    TaskRun --> TaskFuture : settles last
    TaskRun --> FuturePair
    CompletableFuture <|-- TaskFuture
    ThrottleHarness *-- Ticket : inner class
```

## 2. Admission policies

`AdmissionPolicy` is a pure, advisory SPI: `admit` is the only abstract method and the
harness consults it before the liveness floor. Every built-in is `final` and implements the
interface directly. Decorators must extend `ForwardingAdmissionPolicy`, which is why
`AdaptiveSizingPolicy` from the adaptive satellite appears here rather than as a peer of
the built-ins. `defaultPolicy(backend)` is `CompositePolicy(GlobalMemoryBudgetPolicy
singleton, MaxConcurrencyPolicy)`.

Sources: [`throttling/`](../core/src/main/java/io/github/grishagrigorov/executors/throttling),
[`adaptive/AdaptiveSizingPolicy.java`](../adaptive/src/main/java/io/github/grishagrigorov/executors/adaptive/AdaptiveSizingPolicy.java),
[`adaptive/AdaptiveSizing.java`](../adaptive/src/main/java/io/github/grishagrigorov/executors/adaptive/AdaptiveSizing.java).

```mermaid
classDiagram
    direction TB

    class AdmissionPolicy {
        <<interface>>
        +admit(candidate, snapshot)* boolean
        +onTaskStart(task) void
        +onTaskEnd(task) void
        +selectionOrder() Comparator
        +inFlightTaskCount() int
        +requiresExclusive(task) boolean
        +mayRequireExclusive() boolean
        +onScanStart() void
        +ledgerSpansExecutors() boolean
        +defaultPolicy(backend)$ AdmissionPolicy
        +perExecutorDefaultPolicy()$ AdmissionPolicy
    }

    class PoolSnapshot {
        <<interface>>
        +runningTasks() int
        +trackedSizeBytes() long
    }

    class HeapReader {
        <<interface>>
        +occupancyFraction() double
    }

    class ForwardingAdmissionPolicy {
        <<abstract>>
        #AdmissionPolicy delegate
        forwards every method
    }

    class ExclusivityOverride {
        <<package-private>>
        detects overridden requiresExclusive
    }

    class CompositePolicy {
        AND of members, order from the first
    }
    class MaxConcurrencyPolicy {
        in-flight cap
    }
    class MemoryBudgetPolicy {
        budget from the tree snapshot
    }
    class GlobalMemoryBudgetPolicy {
        own ledger, shareable across roots
        +processWideHeapBudget()$ GlobalMemoryBudgetPolicy
        +inFlightBytes() long
    }
    class RateLimitPolicy {
        N starts per sliding window
    }
    class ResourcePermitPolicy {
        per-tag concurrency caps
    }
    class HeapPressurePolicy {
        refuse above high, resume below low
    }

    class AdaptiveSizingPolicy {
        <<executors-adaptive>>
        resizes tasks, quarantines unknown names
    }
    class AdaptiveSizing {
        <<executors-adaptive>>
        +inMemory()$ AdaptiveSizing
        +onFile(path)$ AdaptiveSizing
        +listener() AdaptiveSizingListener
        +policy(delegate) AdaptiveSizingPolicy
        +effectiveSize(name, declared) long
    }
    class ProfileStore {
        <<interface>>
    }
    class InMemoryProfileStore
    class FileProfileStore

    AdmissionPolicy <|.. CompositePolicy
    AdmissionPolicy <|.. MaxConcurrencyPolicy
    AdmissionPolicy <|.. MemoryBudgetPolicy
    AdmissionPolicy <|.. GlobalMemoryBudgetPolicy
    AdmissionPolicy <|.. RateLimitPolicy
    AdmissionPolicy <|.. ResourcePermitPolicy
    AdmissionPolicy <|.. HeapPressurePolicy
    AdmissionPolicy <|.. ForwardingAdmissionPolicy
    ForwardingAdmissionPolicy <|-- AdaptiveSizingPolicy
    ForwardingAdmissionPolicy --> AdmissionPolicy : delegate
    CompositePolicy "1" --> "*" AdmissionPolicy : members
    HeapPressurePolicy --> HeapReader
    AdmissionPolicy ..> PoolSnapshot : reads
    AdmissionPolicy ..> ExclusivityOverride : mayRequireExclusive default
    AdaptiveSizingPolicy --> AdaptiveSizing
    AdaptiveSizing --> ProfileStore
    ProfileStore <|.. InMemoryProfileStore
    ProfileStore <|.. FileProfileStore
```

## 3. Listeners and the satellites

`TaskExecutionListener<C>` is the single cross-cutting seam: four hooks, all with default
bodies, and a typed per-task state `C` that `onSubmit` returns and the later hooks receive.
Two built-ins in core are a static, non-removable prefix of every executor's chain. All four
satellites plug in here, using nothing but the public SPI. Only the JFR and observability
listeners are `ServiceLoader`-discoverable; the other two need a constructor argument.

Sources: [`spi/`](../core/src/main/java/io/github/grishagrigorov/executors/spi),
[`FailFastListener.java`](../core/src/main/java/io/github/grishagrigorov/executors/FailFastListener.java),
[`jfr/`](../jfr/src/main/java/io/github/grishagrigorov/executors/jfr),
[`observability/`](../observability/src/main/java/io/github/grishagrigorov/executors/observability),
[`micrometer/`](../micrometer/src/main/java/io/github/grishagrigorov/executors/micrometer),
[`adaptive/`](../adaptive/src/main/java/io/github/grishagrigorov/executors/adaptive).

```mermaid
classDiagram
    direction TB

    class TaskExecutionListener~C~ {
        <<interface>>
        +onSubmit(task) C
        +onThrottled(task, waitedNanos, floorAdmitted) void
        +onStart(task, state) void
        +onComplete(task, state, outcome) void
    }

    class TaskDescriptor {
        <<record>>
        +ThrottledExecutor executor
        +long sequence
        +String name
        +long sizeEstimate
        +long submittedAtNanos
        +executorId() String
        +executorPath() String
    }

    class TaskOutcome {
        <<record>>
        +TaskStatus status
        +Throwable failure
        +long startedAtNanos
        +long completedAtNanos
        +bodyRan() boolean
        +durationNanos() long
    }

    class TaskStatus {
        <<enumeration>>
        COMPLETED
        FAILED
        CANCELLED_BEFORE_START
        CANCELLED_AFTER_START
        REJECTED
    }

    class AmbientExecutorListener {
        <<package-private, built-in>>
        binds the ambient executor
    }
    class MdcPropagationListener {
        <<package-private, built-in>>
        carries submitter MDC to the worker
    }
    class FailFastListener {
        opt-in: cancelSubtree on first FAILED
    }

    class JfrTaskListener {
        <<executors-jfr, discoverable>>
        one TaskEvent per task
    }
    class TaskEvent {
        <<jdk.jfr.Event>>
    }
    class ThrottleEvent {
        <<jdk.jfr.Event>>
    }

    class ExecutionRecorder {
        <<executors-observability>>
        +shared()$ ExecutionRecorder
        +snapshot() RecorderSnapshot
        +renderText() String
        +renderJson() String
        +registerMBean(name) ExecutionRecorder
    }
    class ExecutionRecorderMXBean {
        <<interface, JMX>>
    }
    class DiscoveredExecutionRecorder {
        <<discoverable>>
        delegates to ExecutionRecorder.shared()
    }
    class ExecutorNode {
        <<package-private>>
        bounded history tree, weak ref to executor
    }

    class MicrometerTaskListener {
        <<executors-micrometer>>
        counts and timers tagged by executor id
    }
    class MeterRegistry {
        <<io.micrometer>>
    }

    class AdaptiveSizingListener {
        <<executors-adaptive>>
        measures allocation, heap peak, duration
    }
    class ProfileStore {
        <<interface>>
    }

    TaskExecutionListener ..> TaskDescriptor : receives
    TaskExecutionListener ..> TaskOutcome : receives
    TaskOutcome --> TaskStatus

    TaskExecutionListener <|.. AmbientExecutorListener
    TaskExecutionListener <|.. MdcPropagationListener
    TaskExecutionListener <|.. FailFastListener
    TaskExecutionListener <|.. JfrTaskListener
    TaskExecutionListener <|.. ExecutionRecorder
    TaskExecutionListener <|.. DiscoveredExecutionRecorder
    TaskExecutionListener <|.. MicrometerTaskListener
    TaskExecutionListener <|.. AdaptiveSizingListener

    JfrTaskListener ..> TaskEvent : C = TaskEvent
    JfrTaskListener ..> ThrottleEvent
    ExecutionRecorderMXBean <|.. ExecutionRecorder
    ExecutionRecorder "1" --> "*" ExecutorNode
    DiscoveredExecutionRecorder --> ExecutionRecorder : shared()
    MicrometerTaskListener --> MeterRegistry
    AdaptiveSizingListener --> ProfileStore
```

## 4. Life of a throttled task

The ordering rules that matter most live here, not in any one class. `onSubmit` runs on
the submitter; the admission scan and the claim happen together under the harness lock;
`onThrottled` and `onStart` run on the worker just before the body; and on every completion
path the order is record the failure, fire `onComplete` in reverse listener order, then
settle the future. A cancel that lands after the body started settles the future
immediately on the canceller, and the worker later reports `CANCELLED_AFTER_START`.

Sources: `submitThrottled(List, policy, clearCompleted)` and `TaskRun` in
[`ThrottledExecutor.java`](../core/src/main/java/io/github/grishagrigorov/executors/ThrottledExecutor.java),
`admitFirst` in [`ThrottleHarness.java`](../core/src/main/java/io/github/grishagrigorov/executors/internal/ThrottleHarness.java).

```mermaid
sequenceDiagram
    autonumber
    participant Caller
    participant Exec as ThrottledExecutor
    participant H as ThrottleHarness
    participant P as AdmissionPolicy
    participant L as Listeners (built-ins, ancestors, own)
    participant W as Worker thread
    participant F as TaskFuture

    Caller->>Exec: submitThrottled(tasks, policy)
    Exec->>H: newTicket()
    loop until every task is admitted
        Exec->>H: admitFirst(candidates, policy, ticket)
        activate H
        Note over H: harness lock held: scan and claim are one step
        H->>P: onScanStart(), selectionOrder()
        H->>P: admit(candidate, snapshot)
        alt policy admits, or in-flight is 0 (liveness floor), or re-entrancy exemption
            H->>P: onTaskStart(task)
            H-->>Exec: admitted task (+ waitedNanos, floorAdmitted)
        else nothing admitted
            H-->>Exec: null
            Exec->>H: awaitStateChange(version, 100 ms)
        end
        deactivate H
        Exec->>L: onSubmit(descriptor) -> per-listener state
        Exec->>F: new TaskRun + TaskFuture
        Exec->>W: pool.execute(run::execute)
    end
    Exec-->>Caller: List<CompletableFuture>

    W->>W: CAS SUBMITTED -> STARTED
    W->>L: onThrottled(descriptor, waitedNanos, floorAdmitted)
    W->>L: onStart(descriptor, state)
    W->>W: body runs
    alt body returns or throws
        W->>W: CAS STARTED -> DONE
        W->>Exec: recordFailure(t) if it threw
        W->>L: onComplete(descriptor, state, outcome) in reverse order
        W->>F: settle(value) or settleExceptionally(t)
    else future.cancel() landed while running
        Note over F: canceller already did: recordFailure, settleCancelled, CAS STARTED -> CANCEL_REQUESTED
        W->>L: onComplete(CANCELLED_AFTER_START) in reverse order
    end
    W->>H: ticket close -> onEnd(task, policy) -> P.onTaskEnd, signal waiters
```

## 5. Java modules

Five named modules. Core exports four packages and withholds `internal`; each satellite
exports exactly its own package. An edge marked `transitive` means the dependency's types
appear in the exporter's public signatures. The two `provides` edges are what
`addDiscoveredListeners()` finds through `ServiceLoader`.

Sources: the five [`module-info.java`](../core/src/main/java/module-info.java) files.

```mermaid
flowchart BT
    subgraph core["io.github.grishagrigorov.executors  (:executors-core)"]
        direction TB
        cexp["exports: executors, spi, throttling, exceptions<br/>withheld: internal<br/>uses TaskExecutionListener"]
    end

    slf4j["org.slf4j"]
    jfrmod["jdk.jfr"]
    jmx["java.management"]
    jdkm["jdk.management"]
    mm["micrometer.core<br/>(automatic module)"]

    jfr["…executors.jfr<br/>JfrTaskListener"]
    obs["…executors.observability<br/>ExecutionRecorder"]
    mic["…executors.micrometer<br/>MicrometerTaskListener"]
    ada["…executors.adaptive<br/>AdaptiveSizing"]

    core -- "requires transitive" --> slf4j
    core -- requires --> jmx

    jfr -- "requires transitive" --> core
    jfr -- "requires transitive" --> jfrmod
    jfr -. "provides TaskExecutionListener" .-> core

    obs -- "requires transitive" --> core
    obs -- requires --> jmx
    obs -. "provides TaskExecutionListener" .-> core

    mic -- "requires transitive" --> core
    mic -- "requires transitive" --> mm

    ada -- "requires transitive" --> core
    ada -- requires --> jmx
    ada -- requires --> jdkm
```

## 6. The per-task state machine

`TaskRun` holds one `AtomicInteger`; every transition is a single compare-and-set, so a
worker and a canceller can never both believe they own the completion. A cancel before the
body starts completes everything on the canceller's thread. A cancel after the body
started records the failure and settles the future at once, but `onComplete` waits for the
body to return, because listener thread-state lives on the worker.

Source: `TaskRun` in
[`ThrottledExecutor.java`](../core/src/main/java/io/github/grishagrigorov/executors/ThrottledExecutor.java).

```mermaid
stateDiagram-v2
    [*] --> SUBMITTED : submit, onSubmit fired

    SUBMITTED --> STARTED : execute() on the worker
    SUBMITTED --> DONE : cancel() or pool rejection<br/>record, onComplete(CANCELLED_BEFORE_START or REJECTED), settle — on the canceller

    STARTED --> DONE : body returns<br/>record if failed, onComplete(COMPLETED or FAILED), settle — on the worker
    STARTED --> CANCEL_REQUESTED : cancel()<br/>record, settleCancelled — on the canceller

    CANCEL_REQUESTED --> [*] : body returns<br/>onComplete(CANCELLED_AFTER_START) — on the worker, future untouched, state stays

    DONE --> [*]
```
