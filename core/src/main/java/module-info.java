/*
 * executors-core as a Java module. Named after the root package it owns, so each satellite's
 * module name equals its single package. Four packages are API; `internal` (the
 * ThrottleHarness, framework mechanics that were public only because the executor lives in
 * another package) is deliberately absent from the exports.
 *
 * The descriptor lives in the base layer of the multi-release jar; the java21 variant only
 * re-supplies a class in an already-exported package, which JEP 238 permits. The reflective
 * virtual-thread probe calls public java.base methods, so nothing needs opening.
 *
 * SLF4J is on the compile surface (LogMarkers exposes Marker, the aggregate exception
 * captures an MDC map), hence `requires transitive`. java.management backs the heap
 * budget and heap-pressure policies. `uses` is the module-system half of
 * addDiscoveredListeners(); satellites keep their META-INF/services entries too, for
 * classpath consumers.
 */
module io.github.grishagrigorov.executors {
    requires transitive org.slf4j;
    requires java.management;

    exports io.github.grishagrigorov.executors;
    exports io.github.grishagrigorov.executors.spi;
    exports io.github.grishagrigorov.executors.throttling;
    exports io.github.grishagrigorov.executors.exceptions;

    uses io.github.grishagrigorov.executors.spi.TaskExecutionListener;
}
