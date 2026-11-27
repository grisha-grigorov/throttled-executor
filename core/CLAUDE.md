# core — `:executors-core`

Loaded when a file under `core/` is touched. The family-wide rules are in the root
`CLAUDE.md`; the full public surface with examples is in `docs/api-reference.md`. This file
holds the invariants and mechanics an agent cannot derive from the code.

## Java version and the multi-release jar
- The convention plugin pins the **toolchain** to Liberica 25 and `options.release = 17`:
  the build runs on 25 and compiles for 17. `JavaVersionFloorTests` pins the bytecode
  version and fails if the `release` override is removed.
- `executors-core.jar` is a multi-release jar (JEP 238). Exactly one class is versioned:
  `VirtualThreadSupport` — base (`src/main/java`, reflective probe) and
  `src/main/java21` (packed under `META-INF/versions/21`, direct call). Both variants must
  present the identical API. Keep the versioned set to this one class.
- **Selection only applies to jar-loaded classes.** The ordinary `test` task and IDE runs
  use the base variant even on a 25 JVM, which is why the base keeps a working probe rather
  than a stub, and why `jarTest` re-runs the suite against the jar.
- **`jarTest17`** runs a small smoke suite (`src/jarTest17/java`) compiled at release 17 on
  a foojay-provisioned Liberica 17 JVM — the only place the pre-21 branches execute on the
  JVM they exist for. First run downloads the JDK. It declares `slf4j-api` explicitly
  because a `files(jar)` dependency carries no transitive metadata.
- Javadoc in exported types refers to internal ones with `{@code}`, never `{@link}`.

## Source files (`src/main/java/io/github/grishagrigorov/executors/`)

| File | Role |
|------|------|
| `ThrottledTask.java` | Unit of throttled work: `job`, `size` (`-1` = none), `name` (the sizing's learning key), `tags`. **Identity, not value** — no `equals`/`hashCode`. |
| `FuturePair.java` | Future + job + `TaskDescriptor`. Final, package-private constructor: handed out, never accepted. |
| `spi/TaskExecutionListener.java` | **The extension seam.** `onSubmit`/`onThrottled`/`onStart`/`onComplete` with typed per-task state. Hook-style: observe, never alter. Full contract in its javadoc. |
| `FailFastListener.java` | Opt-in: `cancelSubtree()` on the first `FAILED`. Never a built-in — it closes the executor. |
| `MdcPropagationListener.java`, `AmbientExecutorListener.java` | Built-ins: a static, non-removable prefix, not entries in any executor's list. |
| `throttling/AdmissionPolicy.java` | Policy SPI. `admit()` is consulted **before** the liveness floor so `onThrottled` can report why a task started; it is advisory, contractually pure, and a throwing one reads as a refusal. |
| `throttling/ForwardingAdmissionPolicy.java` | Public abstract base for decorators; forwards every method. |
| `throttling/GlobalMemoryBudgetPolicy.java` | Budget from its own hook-driven ledger — shareable across roots. Hosts the process-wide 85%-heap singleton used by `defaultPolicy`. |
| `throttling/MaxConcurrencyPolicy.java` | In-flight cap. Decorative under FORK_JOIN; the only bound under virtual. |
| `throttling/RateLimitPolicy.java` | ≤ N starts per sliding window, `InstantSource`-injected. |
| `throttling/ResourcePermitPolicy.java` | Per-tag concurrency caps on `ThrottledTask.tags`, all-or-nothing across a task's tags. Two trees sharing one instance can overshoot by one (documented). |
| `throttling/HeapPressurePolicy.java` + `HeapReader.java` | Reactive: refuses above `high` post-GC occupancy, resumes below `low`. Sampled in `admit`, no thread. `inFlightTaskCount()` stays −1 on purpose so the floor degrades to serial instead of deadlocking. Not in `defaultPolicy`. |
| `internal/ThrottleHarness.java` | Not exported. One per tree. Admission mechanics, fairness tickets, the re-entrancy exemption, rate-limited hook-failure logging. Its class javadoc is the reference. |

## Behavioural contracts not visible from signatures

### Ownership
- `new ThrottledExecutor("X")` is always a root — never adopts, even inside a running task.
- Only `withAmbientDiscovery` reads the ambient parent; every executor binds itself as
  ambient while its tasks run (built-in `AmbientExecutorListener`).
- `newChild` is the primary API: inherits backend + live listeners.

### Submission and waiting
- `submit(...)` is plain, non-throttled, and **never throws** — closed executor and
  shut-down pool surface through the future.
- `submitThrottled(Iterator, ...)` is streaming: pulls a window at a time, memory bounded by
  the window; ordering applies within the window only.
- `waitForCompletion()` is **transitive**: fixpoint over the subtree, capped at 1000 passes;
  throws the aggregate. `waitForCompletion(Duration)` returns `WaitResult` instead.
- `cancelSubtree()` / `shutdownNow()`: depth-first close + cancel; a running body is
  interrupted **only on the virtual backend**.
- `getFutures()` is compacted every 128 submissions (on submit, not on wait).

### Listeners
- Hook order: `onSubmit` (submitter) → `onThrottled` (throttled submissions whose body runs)
  → `onStart` (worker) → `onComplete` exactly once. Submit/start walk built-ins →
  ancestors (root-first) → own; complete walks the **reverse**.
- Snapshotted at submit; guarded per invocation (a throwing listener is logged and the
  others still run; `VirtualMachineError` is rethrown).
- For `REJECTED` / `CANCELLED_BEFORE_START`, `onComplete` runs on the submitter/canceller.
  Restore thread state only if `outcome.bodyRan()`.
- Inside `onComplete` the future is not yet done: use the `TaskOutcome`, never `join()`.
- `onThrottled(desc, waitedNanos, floorAdmitted)`: `waitedNanos` is per-task
  time-to-admission; `floorAdmitted` means the policy refused and the liveness floor
  started it anyway.
- `removeListener` returns `false` for an inherited listener.

### The completion state machine (`TaskRun` / `TaskFuture`)
```
SUBMITTED ─execute()─▶ STARTED ─body returns─▶ DONE
    │ cancel()            │ cancel()
    ▼                     ▼
  DONE              CANCEL_REQUESTED ─body returns─▶ onComplete(CANCELLED_AFTER_START); future untouched
```
- One CAS per transition, never `isDone()`-then-flag.
- Order on every path: **record failure → onComplete (reverse) → settle the future**, so
  "future done ⇒ failure recorded" holds structurally.
- Cancellation is recorded at cancel time; a cancelled-while-queued body never runs.
- `TaskFuture.cancel()` is the one supported external transition. `complete`, `obtrude*`,
  `orTimeout` bypass the machine (WARN when the run later settles).

### Backends
- Two shared static lazy pools; per-instance is the *choice*, not the pool. Default
  `FORK_JOIN` (`availableProcessors - 2`), overridable with
  `-Dio.github.grishagrigorov.executors.backend=virtual|forkJoin`.
- `shutdownThreadExecutors()` shuts down whichever pools exist; pools resurrect on next use.
  All threads are daemons.

### Throttling
1. `ThrottleHarness.admitFirst` scans candidates in `selectionOrder()` and claims the first
   admitted one **atomically under the harness lock** — scan and claim are never separable.
2. **Liveness floor**: when the effective in-flight count is 0 the next candidate is
   admitted unconditionally. The count is the larger of the policy's `inFlightTaskCount()`
   and the harness's own count.
3. **Re-entrancy exemption**: a coordinator whose thread holds a throttle claim may claim
   past the policy, one task per scan, only while everything the policy counts in flight is
   itself parked waiting to nest. Without it nested `submitThrottled` deadlocks. Pinned by
   `NestedSubmissionTests` and `ReentrancyExemptionTests`.
4. **Exclusivity**: a `requiresExclusive` candidate is admitted only into an empty pool and
   blocks the scan for everyone until it runs. This is how adaptive isolates unknown tasks.
5. Nothing admitted → versioned timed wait (100 ms), which doubles as the poll for
   time-based policies and cross-root policy state.

`defaultPolicy(backend)` = the process-wide 85%-heap `GlobalMemoryBudgetPolicy` singleton
AND `MaxConcurrencyPolicy(backend.defaultMaxConcurrency())` (cores for ForkJoin; 256 for
virtual, tunable via `io.github.grishagrigorov.executors.virtualConcurrency`).

### Exceptions
- Failures land in a per-executor queue, recorded raw before the future settles, and
  **drained** at reporting time so a second batch reports only its own.
- The chain is `aggregate → ThreadContextAwareCompletionException(task) → cause`, no
  intermediate `CompletionException`; a nested child's aggregate is not double-wrapped.
- `Error`s are captured like any other throwable. Only a `VirtualMachineError` thrown by a
  listener or policy hook is rethrown, and the task's future is settled first.

## Known issues / gotchas

| # | Issue |
|---|-------|
| 1 | Cross-root admission is not atomic: at global zero, simultaneously-scanning roots can each floor-admit one task. Bounded and documented on `GlobalMemoryBudgetPolicy`. |
| 2 | A completion signals only its own tree's harness; other roots sharing a global policy re-check on the 100 ms poll. |
| 3 | Between the 128-submission compactions, completed futures and job lambdas are retained. |
| 4 | Runaway recursive spawning trips the 1000-pass `waitForCompletion` cap with a WARN. |
| 5 | `defaultPolicy(backend)` shares JVM-global mutable state (the heap singleton). |
| 6 | `future.cancel()` on a not-yet-started task runs every listener's `onComplete` synchronously on the caller. |
| 7 | `onComplete(CANCELLED_AFTER_START)` fires when the body returns, later than the future is observed cancelled; how much later is backend-dependent. |
| 8 | `ScopedValue` cannot replace `AmbientExecutorListener` under this SPI shape — it binds by wrapping, which start/complete hooks cannot express. |
| 9 | Roots sharing one adaptive `policy(...)` share a ledger but not a fairness queue, so a busy sibling root can hold an unknown task out indefinitely — give each root its own call. |
