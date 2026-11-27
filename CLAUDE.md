# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

# executors — the library family

## Purpose
A standalone Java library for bounded parallel execution: pluggable, policy-based
throttling over shared thread pools, an explicit parent/child executor tree, an optional
virtual-thread backend, and a listener SPI through which everything cross-cutting plugs
in. It has **no consumers yet**, but is built for reuse by external projects, some pinned
to Java 17, so treat the public API as a real contract.

This file holds only what an agent cannot derive from the code and must not get wrong.
Rationale belongs in Javadoc or a README, not here. Directory-scoped guidance lives in
`core/CLAUDE.md`, `core/src/test/CLAUDE.md`, `docs/CLAUDE.md` and one per satellite;
Claude Code loads each when a file under that directory is first touched.

Reference documents (read on demand, do not duplicate here):
- `README.md` — user-facing design document and tutorial.
- `docs/api-reference.md` — every exported type and method, with usage examples.
- `docs/architecture.md` — six Mermaid diagrams drawn from the sources.
- `<satellite>/README.md` — that satellite's contract.

## Rules that must not be broken
- **No post-17 APIs in `src/main`.** Main sources compile with `--release 17`, so
  `List.getFirst()`, `Thread.threadId()`, `Future.exceptionNow()`, `ScopedValue` etc. fail
  the build. Tests compile at 25 and may use them.
- **Virtual threads are reached only through `VirtualThreadSupport`.** The base copy uses a
  reflective probe; the `src/main/java21` copy makes the direct call. Never add a
  compile-time virtual-thread reference anywhere else.
- **Satellites use the public SPI only** (`TaskExecutionListener`, `TaskDescriptor`,
  `TaskOutcome`, the public executor API). If a satellite cannot do something, the SPI is
  missing a hook: add it to core, do not reach into internals.
- **Policy decorators extend `ForwardingAdmissionPolicy`.** A pass-through built directly
  on `AdmissionPolicy` silently degrades every time the SPI grows a default method; it has
  happened twice.
- **Exported packages contain only extension points or final value types.** Every
  non-final public class in an exported package is an implicit subclassing promise.
- **Dependency coordinates live only in `gradle/libs.versions.toml`.** `slf4j-api` is the
  single `api` dependency of core; the log4j2 binding is test-scoped and `log4j2.xml` lives
  in `src/test/resources` (a library must not ship a root logger config).
- **Failure recording is hardwired** in the completion path; `waitForCompletion` depends on
  it. Do not make it pluggable or removable. Observing failures is what listeners are for.
- **Keep `MdcPropagationTests` and the ambient tests in `ExecutorTreeTests` green
  unchanged.** They are the proof that the built-in listeners work through the SPI.
- **Discovery is opt-in per root** (`addDiscoveredListeners()`), never automatic.

## Layout — core + satellites
The repository root holds the family docs and build scaffolding (`settings.gradle`, the
Gradle wrapper, `gradle/libs.versions.toml`); it is **not** itself a Java project. Every
member applies one convention plugin,
`buildSrc/src/main/groovy/com.grex.executors-library.gradle`: java-library, the
`io.github.grishagrigorov` group, one shared version (`1.0.0`), the Liberica 25
toolchain, the Java 17 floor, and test-JVM property forwarding. **Publishing is not
configured yet** — no `maven-publish`, no release procedure; do not invent one.

| Dir | Project | Role |
|---|---|---|
| `core/` | `:executors-core` | Executor, tree, backends, policy + listener SPIs. Only dependency: `slf4j-api`. Multi-release jar. |
| `jfr/` | `:executors-jfr` | One Flight Recorder event per task. Discoverable. |
| `observability/` | `:executors-observability` | Bounded history tree with aggregates, text/JSON, JMX. Discoverable. |
| `micrometer/` | `:executors-micrometer` | Task and admission meters to a `MeterRegistry`. Needs a registry, so not discoverable. |
| `adaptive/` | `:executors-adaptive` | Learns per-task memory cost and duration; quarantines unknown names. Needs a store path, so not discoverable. |

Adding a satellite: `mkdir <name>`, a `build.gradle` with `id "com.grex.executors-library"`
+ `api project(":executors-core")` + the test deps, `include ":executors-<name>"` + projectDir
in `settings.gradle`, a `module-info.java`, a floor test, a README, a `CLAUDE.md` that
imports it, a row above.

## Java modules (JPMS)
Every project ships a `module-info.java`. Module name = the root package it owns: core is
`io.github.grishagrigorov.executors`, each satellite adds its suffix.
- **Exports are the API line.** Core exports `executors`, `spi`, `throttling`, `exceptions`
  and withholds `internal`.
- **`requires transitive` only when the dependency's types appear in public signatures**:
  satellites → core; core → `org.slf4j`; jfr → `jdk.jfr`; micrometer → `micrometer.core`.
  `java.management` / `jdk.management` stay plain.
- **Discovery is declared twice**: core `uses TaskExecutionListener`; jfr and observability
  `provides ... with ...` AND keep the `META-INF/services` entry (named modules read the
  descriptor, classpath consumers read the file).
- **Tests stay on the classpath** (no test `module-info`) so white-box tests reach
  package-private seams.
- **TODO**: the `provides` path was verified by hand with a named-module consumer, but
  nothing in the build pins it.

## Build & Test
```bash
./gradlew check                          # the whole family: every module's test + core's shutdownTest, jarTest, jarTest17
./gradlew :executors-core:test           # core main suite (MRJAR tests skip here by design)
./gradlew :executors-core:shutdownTest   # @Tag("shutdown") tests — own JVM; they kill the shared pools
./gradlew :executors-core:jarTest        # main suite against the built jar (MRJAR selection)
./gradlew :executors-core:jarTest17      # Java 17 smoke suite on a real 17 JVM
./gradlew :executors-<sat>:test          # jfr / observability / micrometer / adaptive
./gradlew check -Dio.github.grishagrigorov.executors.backend=virtual   # everything on virtual threads
./gradlew :executors-core:test --tests '*TaskRunTests'                  # one class; '*Class.method' for one test
```
- JDK: `mise install` provisions Liberica 25 and sets `JAVA_HOME`; without mise the foojay
  resolver in `settings.gradle` downloads the toolchain on first build. Gradle comes only
  from the wrapper.
- Run `./gradlew check` before calling a change done.
- The backend, `virtualConcurrency` and `executors.throughput.*` system properties are
  forwarded from the Gradle CLI into every module's forked test JVM by the convention plugin.

## Workflow
- `master` is the mainline (ignore git's `main` hint). Never edit or commit on it.
- Feature and fix work starts on a new branch from `master`, created **before the first
  edit**: `feat/<slug>` or `fix/<slug>`, kebab-case, e.g. `feat/executors-rate-limit-burst`.
- A PreToolUse hook (`.claude/hooks/require-branch.sh`) blocks Edit/Write on `master`;
  if it fires, create the branch and retry the edit. (`docs/<slug>` is also in use for
  documentation-only branches; the hook only cares that you are off `master`.)
- A Stop hook (`.claude/hooks/check-tests.sh`) runs `./gradlew check` before a turn may end
  whenever the tree has uncommitted `.java`/`.gradle`/`.toml`/`.properties`/`.xml` changes,
  and blocks the stop on failure. It lets the second stop through so a red build cannot
  loop, so a passed-through failure must still be reported, never hidden.
