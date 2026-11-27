package io.github.grishagrigorov.executors;

import io.github.grishagrigorov.executors.internal.ThrottleHarness;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins this module's Java API/bytecode floor at 17.
 *
 * <p>The library is intended for reuse by projects that will not move off Java 17 for
 * years. Virtual threads (Java 21+) are therefore reached through a reflective probe and
 * must never appear as a compile-time reference. That rule is enforced mechanically by
 * {@code options.release = 17} in the family's {@code com.grex.executors-library}
 * convention plugin (buildSrc) rather than by review, because
 * {@code --release} resolves main sources against the Java 17 API signatures in
 * {@code ct.sym}.</p>
 *
 * <p>This test guards the build setting itself: if someone removes the {@code release}
 * override, main sources silently start emitting Java 25 bytecode and the floor is gone
 * with no other visible symptom until a consumer on 17 gets an
 * {@link UnsupportedClassVersionError} at runtime.</p>
 *
 * <p>Note this asserts on {@code main} classes only. Test sources deliberately compile at
 * the toolchain level so they may call {@code Thread.isVirtual()} directly.</p>
 */
class JavaVersionFloorTests {

    /** Class-file major version for Java 17. Java 21 is 65, Java 25 is 69. */
    private static final int JAVA_17_CLASS_FILE_MAJOR_VERSION = 61;

    private static final int CLASS_FILE_MAGIC = 0xCAFEBABE;

    /**
     * Reads the class-file major version straight out of the compiled artifact.
     *
     * <p>Structure per JVMS §4.1: {@code u4 magic}, {@code u2 minor_version},
     * {@code u2 major_version}.</p>
     */
    private static int majorVersionOf(Class<?> type) throws IOException {
        final String resource = type.getName().replace('.', '/') + ".class";

        try (InputStream in = type.getClassLoader().getResourceAsStream(resource)) {
            assertThat(in).as("compiled class file for %s", type.getName()).isNotNull();

            final DataInputStream data = new DataInputStream(in);
            assertThat(data.readInt()).as("class file magic").isEqualTo(CLASS_FILE_MAGIC);
            data.readUnsignedShort(); // minor_version, unused
            return data.readUnsignedShort();
        }
    }

    @Test
    @DisplayName("main classes are compiled to Java 17 bytecode")
    void mainClassesShouldBeCompiledToJava17Bytecode() throws IOException {
        assertThat(majorVersionOf(ThrottledExecutor.class))
                .as("ThrottledExecutor class-file major version "
                        + "(61 = Java 17; a higher number means options.release was lost)")
                .isEqualTo(JAVA_17_CLASS_FILE_MAJOR_VERSION);
    }

    @Test
    @DisplayName("the floor covers every package in the module, not just the root one")
    void throttlingPackageClassesShouldAlsoBeCompiledToJava17Bytecode() throws IOException {
        assertThat(majorVersionOf(io.github.grishagrigorov.executors.internal.ThrottleHarness.class))
                .as("ThrottleHarness class-file major version")
                .isEqualTo(JAVA_17_CLASS_FILE_MAJOR_VERSION);
    }
}
