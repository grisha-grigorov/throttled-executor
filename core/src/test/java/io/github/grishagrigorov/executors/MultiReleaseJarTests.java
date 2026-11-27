package io.github.grishagrigorov.executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.CodeSource;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Pins the multi-release structure of the core jar.
 *
 * <p>Every test is assumption-guarded on core being loaded <em>from a jar</em>: version
 * selection ({@code META-INF/versions/N}) only happens for jar-loaded classes, so under
 * the ordinary {@code test} task (classes directory) these tests skip — and that skip is
 * itself the documentation of why the {@code jarTest} task exists. Under {@code jarTest}
 * they run against the artifact that would actually ship.</p>
 */
class MultiReleaseJarTests {

    private static final String SUPPORT_CLASS_ENTRY =
            "io/github/grishagrigorov/executors/VirtualThreadSupport.class";
    private static final int JAVA_17_MAJOR = 61;
    private static final int JAVA_21_MAJOR = 65;

    /** The jar core was loaded from; assumption-fails under the classes-dir test task. */
    private static Path coreJar() {
        final CodeSource source = ExecutorBackend.class.getProtectionDomain().getCodeSource();
        assumeTrue(source != null && source.getLocation().getPath().endsWith(".jar"),
                "core is on the classpath as a classes directory — jar structure not testable here");
        return Paths.get(java.net.URI.create(source.getLocation().toString()));
    }

    private static int majorVersionOf(InputStream in) throws IOException {
        final DataInputStream data = new DataInputStream(in);
        assertThat(data.readInt()).isEqualTo(0xCAFEBABE);
        data.readUnsignedShort();
        return data.readUnsignedShort();
    }

    @Test
    @DisplayName("the jar is marked Multi-Release")
    void theJarIsMarkedMultiRelease() throws IOException {
        try (JarFile jar = new JarFile(coreJar().toFile())) {
            assertThat(jar.getManifest().getMainAttributes().getValue("Multi-Release"))
                    .isEqualTo("true");
        }
    }

    @Test
    @DisplayName("the base entry of the versioned class stays at the Java 17 floor")
    void theBaseEntryStaysAtTheJava17Floor() throws IOException {
        // Plain JarFile.getJarEntry with the unversioned path bypasses version selection.
        try (JarFile jar = new JarFile(coreJar().toFile())) {
            final JarEntry base = jar.getJarEntry(SUPPORT_CLASS_ENTRY);
            assertThat(base).as("base entry %s", SUPPORT_CLASS_ENTRY).isNotNull();
            try (InputStream in = jar.getInputStream(base)) {
                assertThat(majorVersionOf(in))
                        .as("the base variant is what a 17-20 JVM runs")
                        .isEqualTo(JAVA_17_MAJOR);
            }
        }
    }

    @Test
    @DisplayName("the versions/21 entry exists at major 65")
    void theVersions21EntryExistsAtMajor65() throws IOException {
        try (JarFile jar = new JarFile(coreJar().toFile())) {
            final JarEntry versioned = jar.getJarEntry("META-INF/versions/21/" + SUPPORT_CLASS_ENTRY);
            assertThat(versioned).as("the versions/21 variant").isNotNull();
            try (InputStream in = jar.getInputStream(versioned)) {
                assertThat(majorVersionOf(in)).isEqualTo(JAVA_21_MAJOR);
            }
        }
    }

    @Test
    @DisplayName("on this JVM the selected class is the versioned one")
    void theSelectedClassOnThisJvmIsTheVersionedOne() throws IOException {
        coreJar();   // assumption: jar-loaded
        assumeTrue(Runtime.version().feature() >= 21, "selection picks the base variant below 21");

        // getResourceAsStream is selection-aware: on 21+ it returns the versions/21 bytes.
        try (InputStream in = ExecutorBackend.class.getClassLoader()
                .getResourceAsStream(SUPPORT_CLASS_ENTRY)) {
            assertThat(in).isNotNull();
            assertThat(majorVersionOf(in))
                    .as("a 21+ JVM must run the direct-call variant, not the probe")
                    .isEqualTo(JAVA_21_MAJOR);
        }
    }
}
