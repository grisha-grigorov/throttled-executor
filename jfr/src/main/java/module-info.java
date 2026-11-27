/*
 * executors-jfr as a Java module. Core and jdk.jfr are `requires transitive` because the listener's
 * signature exposes core types and the public event classes extend jdk.jfr.Event. The `provides` clause is what ServiceLoader consults for a
 * named module; the META-INF/services entry stays for classpath consumers.
 */
module io.github.grishagrigorov.executors.jfr {
    requires transitive io.github.grishagrigorov.executors;
    requires transitive jdk.jfr;

    exports io.github.grishagrigorov.executors.jfr;

    provides io.github.grishagrigorov.executors.spi.TaskExecutionListener
        with io.github.grishagrigorov.executors.jfr.JfrTaskListener;
}
