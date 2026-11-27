package io.github.grishagrigorov.executors;

import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

/**
 * SLF4J {@link Marker} constants used by the executors library
 * for log filtering. Provides markers for pool shutdown events,
 * default logging, and console-only output.
 *
 * @see ThrottledExecutor
 */
public enum LogMarkers {
    
    ;
    
    public static final Marker POOL_SHUTDOWN = MarkerFactory.getMarker("POOL_SHUTDOWN");
    public static final Marker DEFAULT = MarkerFactory.getMarker("DEFAULT");
    public static final Marker CONSOLE_ONLY = MarkerFactory.getMarker("CONSOLE_ONLY");
}