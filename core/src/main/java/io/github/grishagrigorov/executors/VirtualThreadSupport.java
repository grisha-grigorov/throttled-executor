package io.github.grishagrigorov.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates the virtual-thread-per-task executor, where the runtime can provide one.
 *
 * <p><b>This is the multi-release seam of the core jar</b> — the one class with two
 * compiled variants:</p>
 * <ul>
 *   <li><b>This file</b> (base, Java 17 bytecode): a reflective probe, because the module
 *       compiles with {@code --release 17} and a direct call to a Java 21 API would not
 *       compile. Selected on 17–20 JVMs — and whenever core is on the classpath as a
 *       classes <em>directory</em> (builds, ordinary unit tests), since multi-release
 *       selection only applies to jar-loaded classes.</li>
 *   <li>{@code src/main/java21} (packed under {@code META-INF/versions/21}, Java 21
 *       bytecode): the direct, compile-checked call. Selected automatically when the core
 *       <em>jar</em> runs on a 21+ JVM.</li>
 * </ul>
 *
 * <p>Both variants present the identical signature (the multi-release same-API rule) and
 * identical behaviour on any given JVM; the 21 variant merely trades reflection for a
 * call the compiler can verify. Keep the two files in lock-step.</p>
 */
final class VirtualThreadSupport {

    private static final Logger LOGGER = LoggerFactory.getLogger(VirtualThreadSupport.class);

    private VirtualThreadSupport() {
    }

    /**
     * @return a virtual-thread-per-task executor, or {@code null} when this runtime
     *         cannot provide one (JVMs below 21)
     */
    static ExecutorService createOrNull() {
        try {
            return (ExecutorService) Executors.class
                    .getMethod("newVirtualThreadPerTaskExecutor")
                    .invoke(null);

        } catch (ReflectiveOperationException | UnsupportedOperationException pre21) {
            /*
             * ReflectiveOperationException covers both NoSuchMethodException (Java < 21)
             * and InvocationTargetException (the factory itself refused).
             */
            LOGGER.info("Virtual-thread backend unavailable ({}); falling back to the ForkJoinPool.",
                    pre21.toString());
            return null;
        }
    }
}
