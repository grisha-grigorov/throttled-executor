/*
 * executors-observability as a Java module. java.management backs the JMX MBean. The
 * `provides` clause mirrors the META-INF/services entry (kept for classpath consumers):
 * the discovered listener feeds ExecutionRecorder.shared().
 */
module io.github.grishagrigorov.executors.observability {
    requires transitive io.github.grishagrigorov.executors;
    requires java.management;

    exports io.github.grishagrigorov.executors.observability;

    provides io.github.grishagrigorov.executors.spi.TaskExecutionListener
        with io.github.grishagrigorov.executors.observability.DiscoveredExecutionRecorder;
}
