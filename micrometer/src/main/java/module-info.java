/*
 * executors-micrometer as a Java module. micrometer-core ships only an
 * Automatic-Module-Name (`micrometer.core`), so this is a dependency on an automatic
 * module: legal, but it keeps this satellite out of jlink images until Micrometer
 * publishes a real descriptor. `requires transitive` because MeterRegistry is a
 * constructor parameter. No `provides`: the listener needs a registry, so it is never
 * discovered.
 */
module io.github.grishagrigorov.executors.micrometer {
    requires transitive io.github.grishagrigorov.executors;
    requires transitive micrometer.core;

    exports io.github.grishagrigorov.executors.micrometer;
}
