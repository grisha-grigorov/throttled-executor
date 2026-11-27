package io.github.grishagrigorov.executors;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * The {@code META-INF/versions/21} variant of {@link VirtualThreadSupport}: the direct,
 * compile-checked call the base variant cannot make at the module's {@code --release 17}
 * floor. Selected automatically whenever the core <em>jar</em> runs on a 21+ JVM;
 * behaviourally identical to the base probe succeeding.
 *
 * <p>Same signature as the base file — the multi-release same-API rule. Keep the two in
 * lock-step; {@code MultiReleaseJarTests} pins the packaging and selection.</p>
 */
final class VirtualThreadSupport {

    private VirtualThreadSupport() {
    }

    /**
     * @return a virtual-thread-per-task executor — on this variant's JVMs (21+), always
     *         available; never {@code null}
     */
    static ExecutorService createOrNull() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
