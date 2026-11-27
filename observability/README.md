# executors-observability

A bounded execution-history tree — the family's flight recorder. Keeps **metadata
about every task** (what ran, where in the executor tree, when, for how long, how
it ended) while the tasks themselves are garbage collected. Query it in process,
render it as text or JSON, or point `jconsole`/JMC at it over JMX and use it as
the production troubleshooting console.

```groovy
dependencies {
    implementation project(':executors-observability')   // brings executors-core transitively
}
```

## Attaching

```java
var recorder = new ExecutionRecorder();                  // or new ExecutionRecorder(new RecorderConfig(...))
root.addListener(recorder);                              // explicit — inherited by the subtree

// or the process-wide instance, which ServiceLoader discovery feeds:
root.addDiscoveredListeners();                           // feeds ExecutionRecorder.shared()
var recorder = ExecutionRecorder.shared();
```

## Querying

```java
RecorderSnapshot snap = recorder.snapshot();             // immutable forest, one tree per root
System.out.println(snap.renderText());                   // indented tree for a console/log
String json = snap.renderJson();                         // zero-dependency JSON

recorder.registerMBean("app");                           // JMX:
// io.github.grishagrigorov.executors:type=ExecutionRecorder,name="app"
// attributes: SubmittedTotal, RecordedTotal, FailedTotal, ExecutorNodeCount
// operations: renderText(), renderJson()
```

## What is kept, and what is let go

Per executor **node** (identity = the executor's id string; the node *outlives*
the executor — the only back-reference is a `WeakReference`):

- a **ring** of the last N `TaskRecord`s (default 256) — recent detail;
- **permanent aggregates** — per-status counts, submitted count (so
  `submitted − recorded` is a live in-flight view), duration min/max/mean, a
  9-bucket decade histogram (`<1µs … ≥10s`), and failure counts keyed by
  **root-cause class**, capped with an `"(other)"` overflow — all O(1) forever.

Per parent: live children unlimited; **collected** children keep the last M full
nodes (default 64), older ones are **folded** into the parent's aggregates with an
`evictedChildren` count — compressed, not lost.

A `TaskRecord` is ~200 bytes of strings and numbers. It never holds the job, the
descriptor, the executor, or a `Throwable` (a retained exception pins its stack
frames and captured state — exactly what a recorder must let go of). Failures are
rendered to `Class: message (root cause Class: message)` at completion time.

## Caveats

- A snapshot taken immediately after `waitForCompletion()` can miss
  `CANCELLED_AFTER_START` records — that hook fires when the cancelled body
  actually returns, later than the future settles.
- Snapshots are per-node consistent; cross-node totals are approximate while
  work is in flight.
- Calling `addDiscoveredListeners()` twice on one root double-feeds every
  discovered listener (family-wide discovery property).
- `shared()` is per-classloader, and discovery resolves through core's loader —
  in layered-classloader containers attach explicitly.
- A node folds only when its executor has been **garbage collected** — never on
  `close()`, because a closed executor still delivers events (late bodies,
  rejected submissions). History mirrors reachability.
