package io.github.grishagrigorov.executors.adaptive;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins this satellite's bytecode floor at Java 17 — the family convention plugin's
 * {@code options.release = 17} applies to every member. Also the first test of the module:
 * it names the public surface (facade, profile, store, policy, listener) before any of it
 * exists, so "compiles" is already a statement about the API.
 */
class AdaptiveJavaVersionFloorTests {

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
    void moduleClassesShouldBeCompiledToJava17Bytecode() throws IOException {
        assertThat(majorVersionOf(AdaptiveSizing.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(AdaptiveConfig.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(TaskProfile.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(Sample.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(ProfileStore.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(InMemoryProfileStore.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(FileProfileStore.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(AdaptiveSizingPolicy.class)).isEqualTo(JAVA_17);
        assertThat(majorVersionOf(AdaptiveSizingListener.class)).isEqualTo(JAVA_17);
    }
}
