# core tests

Loaded when a file under `core/src/test/` is touched. Tests compile at Java 25 and may use
post-17 APIs; `src/main` may not.

- JUnit 5 + AssertJ + Awaitility. Tests stay on the classpath (no test `module-info`) so
  white-box tests reach package-private seams. `jarTest` and `jarTest17` are classpath runs too.
- `TaskRunTests` drives the completion state machine deterministically through the
  package-private `newRun(...)`.
- `TaskExecutionListenerTests` pins the SPI contract.
- `TutorialTests` are the README's migration recipes as runnable code — keep them in step
  with the README.
- **Keep `MdcPropagationTests` and the ambient tests in `ExecutorTreeTests` green
  unchanged.** They are the proof that the built-in listeners work through the SPI.
- `GlobalMemoryBudgetPolicyTests` asserts the process-wide heap singleton stays drained:
  construct your own `GlobalMemoryBudgetPolicy` in tests rather than using `defaultPolicy`.
- `MultiReleaseJarTests` are assumption-guarded to run only under `jarTest`; their skip
  under `test` is expected. `JavaVersionFloorTests` pins the bytecode version.
- `@Tag("shutdown")` tests run only under `shutdownTest`, in their own JVM, because they
  kill the shared pools.
- The Java 17 smoke suite lives in `src/jarTest17/java`, compiled at release 17.
- Throughput knobs: `./gradlew :executors-core:test --tests '*measureMax*' -Dexecutors.throughput.batch=20000`.
