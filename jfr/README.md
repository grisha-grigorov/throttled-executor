# executors-jfr

One Java Flight Recorder event per task, so executor activity shows up in **JDK
Mission Control** alongside the JVM's own events — GC pauses, lock waits, socket
reads — on the worker thread's timeline. Production troubleshooting with zero
custom tooling: JFR is in the JDK (11+), designed to stay on in production at
~1–2% overhead, and this module adds no dependency beyond `executors-core`.

```groovy
dependencies {
    implementation project(':executors-jfr')   // brings executors-core transitively
}
```

## Attaching

```java
var root = new ThrottledExecutor("App");

root.addListener(JfrTaskListener.INSTANCE);    // explicit — inherited by the whole subtree
// or
root.addDiscoveredListeners();                 // opt-in ServiceLoader discovery; this jar registers itself
```

Nothing happens by a jar's mere presence — a root has to ask, either way.

## Recording

```bash
java -XX:StartFlightRecording:filename=app.jfr,settings=profile ...
# or attach later:  jcmd <pid> JFR.start filename=app.jfr
```

Open `app.jfr` in Mission Control → *Event Browser* → **Executors**. Or from code
or tests: `new Recording()`, `enable("io.github.grishagrigorov.executors.Task")`,
`start()`, … , `dump(path)`, `RecordingFile.readAllEvents(path)`.

## The event — `io.github.grishagrigorov.executors.Task`

| Field | Meaning |
|---|---|
| `executorId`, `executorPath` | which executor, and where in the tree (`Voyage → Leg-3 → FuelCalc`) |
| `taskName` | the `ThrottledTask` name for throttled submissions, else `task#<sequence>` |
| `sequence` | per-executor submission number |
| `sizeEstimate` | the `ThrottledTask` size estimate, `-1` if none |
| `backend` | `FORK_JOIN` / `VIRTUAL_THREAD_PER_TASK` |
| `status` | `COMPLETED`, `FAILED`, `CANCELLED_BEFORE_START`, `CANCELLED_AFTER_START`, `REJECTED` |
| `bodyRan` | whether the body actually executed |
| `queuedNanos` | submission → body start |
| `failure` | class + message for non-completed outcomes — never the `Throwable` |
| *duration* | `begin()` at body start, `end()` at body end — the body's wall time |

Tasks that never ran (rejected, cancelled while queued) are emitted too, with no
duration; in a production recording those are often the interesting ones. Stack
traces are off by default (`@StackTrace(false)`) — the submitting stack is rarely
useful and costs per task; enable in a custom `.jfc` if you need it.

## The throttle event — `io.github.grishagrigorov.executors.Throttle`

Emitted **only for throttled submissions**, once per task, when it is admitted:

| Field | Meaning |
|---|---|
| `executorId`, `executorPath` | which executor, and where in the tree |
| `taskName`, `sequence` | as above |
| `waitedNanos` | how long this task was queued before admission, from when its coordinator first offered it — not "how long the policy refused it", and never quite `0` |
| `floorAdmitted` | the policy **refused** this task and the liveness floor started it anyway |

A separate event rather than fields on `Task`, because the two answer different
questions and are wanted at different times: `Task` covers every submission and
is about the task; this is about the *throttle*. A recording chasing "why was
this batch slow?" can enable this one alone and pay nothing for the rest.

`floorAdmitted` is the field to filter on. A handful is the floor doing its job.
A batch full of them means the policy is refusing everything and the floor is the
only thing making progress — a serial batch wearing a parallel batch's clothes.

It is instantaneous, not timed, so the wait is an explicit `@Timespan` field:
by the time the event is committed the waiting has already happened. Enable it
with `withThreshold(Duration.ZERO)`, or a non-zero threshold will drop every one.

## Cost when not recording

One small allocation per task and an `isEnabled()` check. `onSubmit` hands back no
state, so `onStart`/`onComplete` are no-ops. The throttle event is checked
independently, so enabling one event does not cost you the other.
