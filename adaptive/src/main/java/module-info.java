/*
 * executors-adaptive as a Java module. java.management backs the heap-peak measurement;
 * the per-thread allocation meter needs com.sun.management.ThreadMXBean, which lives in
 * jdk.management (the descriptor is what surfaced that dependency: on the classpath it
 * was invisible). No `provides`: the policy needs a profile store, so it is never
 * discovered.
 */
module io.github.grishagrigorov.executors.adaptive {
    requires transitive io.github.grishagrigorov.executors;
    requires java.management;
    requires jdk.management;

    exports io.github.grishagrigorov.executors.adaptive;
}
