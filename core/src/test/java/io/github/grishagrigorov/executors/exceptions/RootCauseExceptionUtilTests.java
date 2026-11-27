package io.github.grishagrigorov.executors.exceptions;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the cause-chain walk against cyclic chains.
 *
 * <p>{@code initCause} only forbids self-causes, so a cycle of length two or more is
 * constructible — and an uncapped walk loops forever. That is not an academic concern
 * here: {@code getRootCause} runs on core's failure-recording path, so an uncapped walk
 * wedges the worker thread that ran the failing task, silently, before any listener sees
 * the completion.</p>
 */
class RootCauseExceptionUtilTests {

    @Test
    @DisplayName("a cyclic cause chain terminates instead of wedging the caller")
    void aCyclicCauseChainTerminates() {
        final RuntimeException a = new RuntimeException("cycle-a");
        final RuntimeException b = new RuntimeException("cycle-b", a);
        a.initCause(b);   // a -> b -> a

        assertThat(org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(
                Duration.ofSeconds(5), () -> RootCauseExceptionUtil.getRootCause(a)))
                .as("some element of the cycle — which one is unspecified; returning at all is the contract")
                .isIn(a, b);
    }

    @Test
    @DisplayName("an ordinary chain still returns the deepest cause")
    void anOrdinaryChainReturnsTheDeepestCause() {
        final IllegalStateException root = new IllegalStateException("root");
        final RuntimeException top = new RuntimeException("wrap", new RuntimeException("middle", root));

        assertThat(RootCauseExceptionUtil.getRootCause(top)).isSameAs(root);
    }

    @Test
    @DisplayName("a throwable without a cause is its own root")
    void aCauselessThrowableIsItsOwnRoot() {
        final RuntimeException alone = new RuntimeException("alone");
        assertThat(RootCauseExceptionUtil.getRootCause(alone)).isSameAs(alone);
    }
}
