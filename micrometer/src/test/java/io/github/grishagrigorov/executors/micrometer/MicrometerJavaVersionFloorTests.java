package io.github.grishagrigorov.executors.micrometer;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Pins this satellite's bytecode floor at Java 17 — proof that the family convention
 * plugin's {@code options.release = 17} applies to every member, not just core.
 */
class MicrometerJavaVersionFloorTests {

    private static final int JAVA_17 = 61;

    private static int majorVersionOf(Class<?> type) throws IOException {
        final String resource = type.getName().replace('.', '/') + ".class";
        try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).isNotNull();
            final DataInputStream data = new DataInputStream(in);
            assertThat(data.readInt()).isEqualTo(0xCAFEBABE);
            data.readUnsignedShort();
            return data.readUnsignedShort();
        }
    }

    @Test
    @DisplayName("the satellite's classes are compiled to Java 17 bytecode")
    void satelliteClassesShouldBeCompiledToJava17Bytecode() throws IOException {
        assertThat(majorVersionOf(MicrometerTaskListener.class)).isEqualTo(JAVA_17);
    }
}
