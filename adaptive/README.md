# executors-adaptive

Learn each task's memory cost, throttle the next run on it. A task may be
submitted **without a size** (`-1`) or with a rough **input size** (bytes to
read); a listener measures what the run actually cost, a store remembers it per
task name — across restarts — and a policy decorator **resizes the task on the
fly** before the budget policy you already use sees it. Tasks nothing is known
about are quarantined so they run alone, exactly once.

```groovy
dependencies {
    implementation project(':executors-adaptive')   // brings executors-core transitively
}
```

## Attaching

```java
try (AdaptiveSizing adaptive = AdaptiveSizing.onFile(Path.of("task-profiles.tsv"))) {
    root.addListener(adaptive.listener());                 // the measuring half — one instance, every root that learns

    AdmissionPolicy policy = adaptive.policy(              // the deciding half — a decorator
            CompositePolicy.allOf(GlobalMemoryBudgetPolicy.processWideHeapBudget(),
                                  new MaxConcurrencyPolicy(8)));

    root.submitThrottled(List.of(
            new ThrottledTask<>(() -> buildVoyageReport(), -1,          "VoyageReport"),   // unknown: learned on the first run
            new ThrottledTask<>(() -> loadNoonReports(),   bytesOnDisk, "NoonReports")),   // sized: the multiplier is learned
            policy);
    root.waitForCompletion();
}                                                          // close() stops the sampler and saves the profiles
```

`AdaptiveSizing.inMemory()` learns for this JVM only; `onFile(path, config)` and
`of(store, config)` take an `AdaptiveConfig` (ring size, headroom, estimator, the
reserve shown for an unknown task, the heap-sample period, and the two
[freshness](#when-a-measurement-stops-counting) knobs — the application
version and the maximum sample age). There is no `META-INF/services` entry on
purpose — something that needs a store path is attached explicitly.

## Run 1, run 2 — how learning comes back

Nothing extra to call: the file is read in the constructor behind `onFile`, so
the moment it returns, `policy(...)` already resizes from the previous run.

```java
// Run 1 — nothing known yet
try (var adaptive = AdaptiveSizing.onFile(Path.of("task-profiles.tsv"))) {   // file missing → empty store
    root.addListener(adaptive.listener());
    var policy = adaptive.policy(CompositePolicy.allOf(budget, new MaxConcurrencyPolicy(8)));
    root.submitThrottled(tasks, policy);     // an unknown name runs solo — the batch waits for it
    root.waitForCompletion();
}                                            // close() → save(): the TSV is written

// Run 2 — a fresh JVM, the same code
try (var adaptive = AdaptiveSizing.onFile(Path.of("task-profiles.tsv"))) {   // file exists → loaded HERE
    root.addListener(adaptive.listener());
    var policy = adaptive.policy(CompositePolicy.allOf(budget, new MaxConcurrencyPolicy(8)));
    root.submitThrottled(tasks, policy);     // same names → already sized: no quarantine, parallel from the first task
    root.waitForCompletion();
}                                            // close() → save() only if something new was learned
```

Who runs alone, and whether you have to arrange it:

| Task | Backend | Solo run needed for learning? |
|---|---|---|
| declared `-1`, never seen | ForkJoin | no — the quarantine does it automatically |
| declared `-1`, never seen | virtual | the quarantine arranges it, but only against work that goes through the same policy instance: a plain `submit()` on the root, or a root without the listener, overlapping the run means no memory is measured — a duration-only sample is stored, one WARN per name, and it is quarantined again next batch |
| declared size | ForkJoin | no — allocation is measured per thread, overlap does not matter |
| declared size | virtual | yes — allocation is `-1` there; calibrate once under `adaptive.policy(new MaxConcurrencyPolicy(1))`. Every run still stores a duration-only sample |
| unnamed (`name == null`) | either | never learned: invisible to the decorator, shown to the delegate exactly as declared; nothing is stored |

Any later run that happens to be alone (the tail of a batch, a quiet period)
adds a fresh heap-peak sample too; the ring keeps filling on both measurements.

To look at what came back rather than just let the policy use it:

```java
TaskProfile p = adaptive.profile("VoyageReport");     // null if never seen — the raw ring, stale samples included
p.samples();                                          // oldest first; each says when and under which version it was measured
p.lastMeasuredAt();                                   // Optional<Instant> — the newest sample's completion
p.absolute(Estimator.ALLOCATION);                     // OptionalLong — largest bytes learned
p.ratio(Estimator.ALLOCATION);                        // OptionalDouble — largest measured / declared
adaptive.freshProfile("VoyageReport");                // only the samples that still count — what the sizing reads
adaptive.effectiveSize("VoyageReport", -1);           // what the delegate policy will be shown (× headroom)
adaptive.store().snapshot();                          // Map<String, TaskProfile> — everything loaded
```

Two variations: to apply learned sizes to a policy you do not want wrapped,
size the tasks yourself — `new ThrottledTask<>(job, adaptive.effectiveSize("VoyageReport", -1), "VoyageReport")`
(no quarantine for still-unknown names; `effectiveSize` is `-1` for those). To
reload with different settings, build the store yourself:
`AdaptiveSizing.of(new FileProfileStore(path, 8), new AdaptiveConfig(8, 1.5, Estimator.SOLO_HEAP_PEAK, reserve, Duration.ofMillis(10)))`
— a smaller ring on load keeps the newest samples.

## The two models, chosen per sample

| The submission declared | What is learned | What the budget policy is shown |
|---|---|---|
| a size `> 0` (e.g. bytes to read) | the **ratio** measured ÷ declared | `ceil(declared × maxRatio × headroom)` |
| `-1` (nothing) or `0` | the **absolute** measurement | `ceil(maxAbsolute × headroom)` |

Both are **maxima over the last N samples** (default 16), never averages:
under-throttling costs an `OutOfMemoryError`, over-throttling costs a little
parallelism. Headroom (default 1.25) applies to learned values only. A declared
size nothing was learned about passes through **untouched** — an executor whose
tasks are all declared and never seen behaves exactly as if undecorated (pinned
by a 500-task identity test).

## The two measurements

| | How | Where it works |
|---|---|---|
| **Allocation** (default estimator) | the worker's `ThreadMXBean` allocated-bytes counter, end − start | HotSpot platform-thread workers. `-1` on **virtual threads** (the JDK itself answers `-1`) and non-HotSpot JVMs |
| **Solo heap peak** | the largest rise of heap-in-use above the lowest reading seen during the run — a sampler thread reads every `heapSamplePeriod` (default 10 ms; `ZERO` = start/end readings only) | only when the task ran **solo**: nothing in flight when it started, nothing started before it ended |

Allocation counts everything the body churned, not only what it retained; the
heap peak is closer to "peak retained" but is only attributable when the task
ran alone and is noisier. Each estimator reads its own measurement and falls
back to the other when its own was never recorded for the name — a
virtual-thread workload learns through heap peaks without touching any setting.

## The first run of an unknown task

A task with no declared size and no profile cannot be throttled, so it is
**quarantined**: the decorator declares it `requiresExclusive`, and core's
harness admits it only into an empty pool and admits nothing beside it while it
runs. The delegate's ledger sees `unknownReserveBytes` (default: the whole max
heap — "this may take everything"). It therefore runs alone, its heap peak is
attributable, and from the second batch on it is throttled like everything else.
The policy offers unknowns first in `selectionOrder()`, so the solo slot is used
as soon as it exists.

Isolation being the framework's job rather than this decorator's is what removed
the old starvation hazard. The quarantine used to be a private census that
refused every other candidate and bent the liveness floor into letting the
unknown through — which meant the unknown could only start at a moment of
emptiness, and a steady stream of known tasks from another coordinator sharing
the decorator never produced one. The harness now holds the waiting task's place
in the admission queue, so the pool is allowed to drain for it.

## When a measurement stops counting

A learned size describes the code that was measured. After a release — or just
after a long time — it may describe nothing that still runs. A known task keeps
being measured where allocation is available, so a cost that *grew* shows up on
its next run; but a cost that *shrank* keeps its old maximum until the ring
turns over, and on virtual-thread workers (heap peaks only, solo runs only) a
known task that is never alone again is never re-measured at all. Every sample
therefore records **when** it was taken (`epochMillis`) and **under which
application version** (`version`), and the sizing reads only the samples that
are still *fresh*:

```java
AdaptiveConfig config = AdaptiveConfig.defaults()
        .withVersion(Main.class.getPackage().getImplementationVersion())   // or a build id, a git sha, a date
        .withMaxSampleAge(Duration.ofDays(90));

try (var adaptive = AdaptiveSizing.onFile(Path.of("task-profiles.tsv"), config)) { … }
```

| Knob | Default | Effect |
|---|---|---|
| `version` | `null` — unversioned | new samples are stamped with it; only samples with **exactly** this version count. An unversioned sample never matches a named version, so the first run with a version set re-measures everything |
| `maxSampleAge` | `null` — no limit | a sample older than this, measured from the facade's clock **at admission time**, no longer counts — a long-running JVM sees profiles go stale without a restart |

A task whose samples have all gone stale is treated as never seen: a declared
size passes through untouched, an undeclared one is `UNKNOWN` — quarantined,
run solo, measured again. No call is needed; the old samples stay in the ring
(visible through `profile()`, absent from `freshProfile()`) until fresh ones
push them out. When the whole ring is fresh the check is two comparisons
against values precomputed per profile; only a profile that has actually gone
partly stale pays for a filtered copy.

That memo lives on the immutable `TaskProfile` (`fresh(version, notBefore)`
keeps its last filtered view together with the exact cutoff range that view
answers), not in the policy. A scan-keyed memo in `AdaptiveSizingPolicy` was
tried first and never hit once: only `admit` has a snapshot to key on, while
`requiresExclusive` and the ordering comparator pass `null` and run first,
invalidating it before every read. Keyed on the profile it is a memo, never
stale, and `append`/`forget` return a different instance so recording
invalidates it for free.

For "this *part* of the application changed" there is the manual lever:
`adaptive.forget("VoyageReport")` drops the name — it is unknown again, and the
drop is written by the next `save()`. Nothing is ever forgotten automatically.

## The store

`ProfileStore` is pluggable; two implementations ship:

- `InMemoryProfileStore` — gone with the JVM.
- `FileProfileStore(path, ringSize)` — loaded once on construction, held in
  memory, written by `save()` / `close()` **atomically** (temp file + move) and
  **only when something changed**. `record()` never touches the disk: it runs on
  the worker inside `onComplete`, where a syscall would break core's fast-hooks
  contract. Call `save()` at a quiet moment; `close()` does.

The file is TSV, one line per sample, names sorted, a name's samples oldest
first:

```
#executors-adaptive v3
name<TAB>declaredSize<TAB>allocatedBytes<TAB>soloHeapPeakBytes<TAB>epochMillis<TAB>version<TAB>durationNanos
```

Names and versions escape `\ TAB LF CR` as `\\ \t \n \r` and a leading `#` as
`\#`; an empty version column is "unversioned"; `-1` in a measurement column is
"not measured". The column count decides the version on load, not the header:
five columns is v1 (unversioned, untimed), six is v2 (untimed), seven is v3.
Older files still load and are rewritten as v3 on the next save. Empty and `#`
lines are ignored; a malformed line is logged and skipped, never fatal —
losing one learned sample is cheaper than refusing to start.

**Footprint.** Every completed run of a *named* task stores a sample — on the
virtual backend, or under any overlap, a duration-only one — so the store
holds up to `ringSize` samples per distinct name, with no cap on names. The
file is rewritten in full on a dirty `save()` and loaded in full at startup.
Name the *kind* of work (`"LegReport"`), not the instance (`"Reindex-" +
voyageId`): an unbounded name space is an unbounded file.

## Caveats

- **Ratio learning assumes memory ∝ declared input.** Declare something that
  really scales with the cost (bytes read, rows to process), or declare `-1` and
  learn the absolute.
- **Heap peaks are noisy.** A collection that reclaims more than the task built,
  with nothing built afterwards, reads as a peak of `0`; generational
  collectors make such readings common for short tasks. The max over the ring
  absorbs them — calibrate a task type a few times before trusting the heap
  path alone. (Measuring the rise above the *running low* rather than above the
  start reading already handles the common early-collection case.)
- **Virtual-thread workers learn memory through heap peaks only** — allocation is
  `-1` there. Combined with the point above, calibrate such workloads solo
  (e.g. one batch under `MaxConcurrencyPolicy(1)`). Durations are learned on
  every run regardless: a run whose memory could not be measured records a
  duration-only sample, which teaches the sizing nothing. Memory measurements
  own half the ring (at least one — the solo-run sample is never evicted by a
  timing); timings take the other half and rotate there, so
  `SelectionKey.DURATION` keeps learning after the quarantine run, and keeps
  learning for a profile first written under ForkJoin.
- **Quarantine serialises the whole window, not just the unknowns.** This is the
  caveat to read twice. An unknown declares `requiresExclusive`, and the harness
  answers a blocked exclusive candidate *anywhere in the window* by refusing the
  entire scan — the coordinator admits nothing at all until the pool has drained
  and the unknown has run alone. `selectionOrder()` also sorts unknowns first, so
  an unknown is candidate #0 on every scan while it waits. A window carrying one
  unknown and ten thousand known tasks therefore admits nothing until the pool
  empties, runs the unknown solo, and only then resumes: throughput drops to
  serial for one round per unknown name.

  That is deliberate — an exclusive task can only start at a moment of emptiness,
  and a coordinator that kept admitting from behind it would compete with itself
  and never produce one — but it is a stronger effect than the old private census,
  which only blocked while an unknown was actually *in flight* and let known work
  behind a queued one keep flowing.

  It costs one round on a first batch, so name tasks by *type*
  (`"VoyageReport"`), not by instance (`"VoyageReport-4711"`) — eight never-seen
  types cost eight rounds, eight thousand instance names cost eight thousand.
  **Watch the interaction with `maxSampleAge`**: names go stale in normal
  operation and become unknown again, so on a long-running system this is a
  recurring cost, not a start-up one. Size `maxSampleAge` against how often you
  can afford a serial round, or leave it unset and use `version()`/`forget(name)`
  to invalidate deliberately.

  In exchange the harness holds the quarantined task's place in the admission
  queue, so within one root tree a busy neighbour cannot keep it waiting
  indefinitely — the starvation caveat the private census carried is gone
  *there*. Across roots it is not: two roots sharing one `policy(...)` share a
  ledger (the unknown waits for the whole of it to drain) but not a fairness
  queue, so a steady stream of known work on a busy sibling root can hold an
  unknown on an idle root indefinitely. Give each root its own `policy(...)`
  call if that matters.
- **Quarantine holds the pool per executor tree.** An unknown waits for the
  policy's whole ledger to drain before it starts — "alone" is measured against
  `inFlightTaskCount()`, which spans every root sharing the instance — but the
  hold while it runs is the harness's, and there is one harness per root, so
  another root sharing a single `policy(...)` can still start known work beside
  it. A memory-budget delegate still covers that (the unknown reserves
  `unknownReserveBytes`, which by default saturates the budget); a delegate that
  only counts tasks does not. Give each root its own `policy(...)` call, or keep
  a memory budget in the delegate.
- **Wrapping `policy(...)` in a decorator of your own?** Extend core's
  `ForwardingAdmissionPolicy` and override only what you change — it forwards
  the whole SPI by build-enforced contract (a reflective test rejects any SPI
  method the base does not declare). `AdaptiveSizingPolicy` itself extends it. A
  hand-rolled pass-through must forward `requiresExclusive`,
  `mayRequireExclusive`, `onScanStart`, `ledgerSpansExecutors` and
  `inFlightTaskCount`, not just the three task hooks: the harness asks the
  outermost policy, and only if it says it may isolate, so a wrapper that
  forwards `admit`/`onTaskStart`/`onTaskEnd` alone hides the quarantine
  completely — `admit` refuses the unknown, the liveness floor admits it
  anyway, known work starts beside it, and nothing is logged. And forward
  `onScanStart` *unconditionally*: it is the announcement the adaptive policy
  refreshes its delegate's answer on — swallow it and that answer freezes at
  its construction seed. An announcement, unlike a query, cannot be
  short-circuited.
- **Cross-root admission is not atomic at a global zero** — the same transient
  window `GlobalMemoryBudgetPolicy` documents.
- **Each `policy()` call has its own ledger.** Share one decorator instance
  between the submissions that should see each other, exactly like a
  `GlobalMemoryBudgetPolicy`; wrap the composite (`adaptive.policy(allOf(…))`),
  not a member — inside a composite that is not first, the decorator's
  `selectionOrder()` is ignored. While the ledger is idle the decorator reports
  the delegate's own in-flight count (`-1` for a snapshot-based one), so the
  executor's liveness floor keeps keying on the tree exactly as if undecorated.
- **The wrapped delegate inherits the decorator's scope.** A `MemoryBudgetPolicy`
  inside `adaptive.policy(…)` budgets the decorator's ledger of *effective*
  bytes (learned × headroom), not the tree's declared-bytes snapshot that
  core's `PoolSnapshot.trackedSizeBytes()` describes: one `policy()` per root
  keeps it per tree, a shared decorator makes it behave like a global budget.
- **The delegate's `selectionOrder()` is replaced**, not consulted: unknowns
  first, then effective size descending. A delegate with its own ordering (a
  deadline or FIFO policy) loses it under the decorator.
- **Freshness is all-or-nothing per sample, and exact on the version.** A
  build id that changes on every CI run re-measures everything on every
  deployment; a version that never changes re-measures nothing. Pick what
  actually invalidates measurements (a release, a schema change), and keep
  `maxSampleAge` as the backstop for drift you did not version.
- **Stale samples are kept, not deleted.** `profile()` and the file still show
  them until the ring pushes them out; a name that never runs again keeps its
  old samples forever. `forget()` is the only deletion.
- **Duplicate instances are charged per claim.** Core admits the same
  `ThrottledTask` once per occurrence in a batch (and again when a later batch
  re-submits it while the earlier run is still going); the decorator keeps one
  ledger entry per claim, so every start reaches the delegate and every end
  releases one.

## Building and testing

```bash
./gradlew :executors-adaptive:test
./gradlew :executors-adaptive:test -Dio.github.grishagrigorov.executors.backend=virtual   # heap-peak path only
```

207 tests: `TaskProfileTests` and `HeapRiseTests` (the arithmetic, the freshness
view), `HeapSamplerTests` (sessions), `EffectiveSizeTests` (every branch of the
resolution rule, staleness by age and by version on an injected clock),
`ProfileStoreTests` (ring, concurrency, TSV v1/v2 round-trips, atomic
dirty-only saves, `forget`), `AdaptiveSizingPolicyTests` (resizing, ledger,
quarantine, wrapped snapshot, selection order, the identity test),
`AdaptiveSizingListenerTests` (all five `TaskStatus` paths, solo detection, the
sampler, the direct-driven ordering contracts, the version/clock stamp) and
`AdaptiveEndToEndTests` (learning loop, ratio loop, restart survival, a new
version measures again) — green on both backends.
