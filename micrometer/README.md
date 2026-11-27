# executors-micrometer

A `TaskExecutionListener` that publishes executor activity to a Micrometer
`MeterRegistry`, so the library's numbers land wherever the application's other
metrics already go — Prometheus, a Spring Boot actuator endpoint, JMX.

`executors-observability` answers *"what happened in this JVM, right now"*
through its own MBean and text/JSON renderings. This answers *"what is happening
over time"*, through infrastructure the consumer already runs. Neither replaces
the other, and they can be attached together.

## Attaching

```java
var root = new ThrottledExecutor("NightlyReports");
root.addListener(new MicrometerTaskListener(meterRegistry));
```

There is deliberately **no** `META-INF/services` entry: the listener needs a
registry, and a `ServiceLoader` cannot supply one. Attach it explicitly, exactly
as with `executors-adaptive`.

The meter prefix defaults to `executors` and is configurable:

```java
new MicrometerTaskListener(meterRegistry, "ship.reports");
```

## The meters

| Meter | Type | Meaning |
|---|---|---|
| `executors.tasks` | counter | completed tasks, tagged `status` and `executor` |
| `executors.task.duration` | timer | body wall time, for tasks that ran |
| `executors.task.queued` | timer | submission → body start |
| `executors.throttle.wait` | timer | how long a task was queued before admission, from when its coordinator first offered it |
| `executors.throttle.floorAdmitted` | counter | tasks the policy **refused** and the liveness floor started anyway |

`executors.throttle.floorAdmitted` is the one to alarm on. A few are the floor
doing its job. A rate approaching the task rate means the policy is refusing
everything and the floor is the only thing making progress — the batch is running
serially, which is the failure the throttle exists to prevent.

## Tagging

Meters are tagged by **executor id**, never by task name. Task names are chosen
by callers and can be unbounded — one per voyage, one per report — and an
unbounded tag value is the classic way to melt a metrics backend. Executor ids
come from a small fixed set the application controls.

If you want per-task-name metrics, that is a decision to make with a
known-bounded name set, and a good reason to write your own listener: the SPI is
three default methods and this module is under 150 lines.

## Cost

A registry lookup and a counter or timer record per task. Meters are resolved
through the registry rather than cached in fields — Micrometer's own lookup is
cheap, and it keeps the listener correct if a registry is reconfigured underneath
it. Tasks that never ran (rejected, cancelled while queued) are counted but not
timed: recording a zero duration would drag the averages towards a lie.

## Building and testing

```bash
./gradlew :executors-micrometer:test
```

Eight tests, driven through a real executor into a real `SimpleMeterRegistry` and
read back out of it — no mocking, so what is asserted is what a scrape would see.
