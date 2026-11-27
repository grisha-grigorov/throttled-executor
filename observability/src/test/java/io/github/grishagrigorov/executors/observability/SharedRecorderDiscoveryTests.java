package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the discovery path. {@code ServiceLoader} instantiates fresh objects per load,
 * which would orphan a recorder nobody can query — so the services entry names a stateless
 * facade delegating to {@link ExecutionRecorder#shared()}, and everything discovered lands
 * where {@code ExecutionRecorder.shared()} (or JMX) can reach it.
 *
 * <p>{@code shared()} is JVM-global across the whole test suite, so these tests assert
 * <b>deltas</b>, never absolute totals.</p>
 */
class SharedRecorderDiscoveryTests {

    @Test
    @DisplayName("shared() always returns the same instance")
    void sharedReturnsTheSameInstanceAlways() {
        assertThat(ExecutionRecorder.shared()).isSameAs(ExecutionRecorder.shared());
    }

    @Test
    @DisplayName("addDiscoveredListeners feeds the shared recorder; a plain root does not")
    void addDiscoveredListenersFeedsTheSharedRecorder() {
        final long before = ExecutionRecorder.shared().getRecordedTotal();

        final ThrottledExecutor plain = new ThrottledExecutor("DiscPlain");
        plain.submit((Runnable) () -> { }).join();
        plain.waitForCompletion();
        assertThat(ExecutionRecorder.shared().getRecordedTotal())
                .as("a root that never opted in feeds nothing")
                .isEqualTo(before);

        final ThrottledExecutor discovering =
                new ThrottledExecutor("DiscOptIn").addDiscoveredListeners();
        discovering.submit((Runnable) () -> { }).join();
        discovering.waitForCompletion();
        assertThat(ExecutionRecorder.shared().getRecordedTotal()).isEqualTo(before + 1);

        final ThrottledExecutor child = discovering.newChild("DiscChild");
        child.submit((Runnable) () -> { }).join();
        child.waitForCompletion();
        assertThat(ExecutionRecorder.shared().getRecordedTotal())
                .as("discovery is inherited by the subtree like any listener")
                .isEqualTo(before + 2);
    }

    @Test
    @DisplayName("the facade is stateless and ServiceLoader-instantiable")
    void theFacadeIsStatelessAndHasAPublicNoArgConstructor() throws Exception {
        assertThat(DiscoveredExecutionRecorder.class.getConstructor()).isNotNull();
        assertThat(DiscoveredExecutionRecorder.class.getDeclaredFields())
                .as("all state lives in shared(); the facade must have none")
                .isEmpty();
    }
}
