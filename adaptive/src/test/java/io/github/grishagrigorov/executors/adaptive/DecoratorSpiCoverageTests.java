package io.github.grishagrigorov.executors.adaptive;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;

import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * The adaptive decorator must take a position on every {@code AdmissionPolicy} method — an
 * inherited interface default answers for the wrapper, not the delegate, and the SPI has
 * grown twice with the missing forward found in exactly this class both times. It now
 * extends core's {@code ForwardingAdmissionPolicy}, whose own sweep (in core) refuses a
 * future SPI method until the base declares it; this sweep pins that the decorator keeps
 * riding on that guarantee rather than drifting back to a hand-rolled {@code implements}.
 */
@DisplayName("the adaptive decorator covers the SPI")
class DecoratorSpiCoverageTests {

    @Test
    @DisplayName("every SPI method is declared below the interface — by this class or by the base it extends")
    void adaptiveDecoratorTakesAPosition() throws NoSuchMethodException {

        for (Method spi : AdmissionPolicy.class.getMethods()) {
            if (Modifier.isStatic(spi.getModifiers())) {
                continue;
            }
            final Method impl = AdaptiveSizingPolicy.class.getMethod(spi.getName(), spi.getParameterTypes());
            assertThat(impl.getDeclaringClass())
                    .as("AdaptiveSizingPolicy must handle %s itself or through ForwardingAdmissionPolicy "
                            + "— an inherited interface default answers for the wrapper, not the delegate",
                            spi.getName())
                    .isNotEqualTo(AdmissionPolicy.class);
        }
    }
}
