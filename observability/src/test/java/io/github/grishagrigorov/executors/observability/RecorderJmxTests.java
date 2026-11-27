package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import java.lang.management.ManagementFactory;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the JMX face. {@code jconsole}/JMC against a running ship application is the
 * intended production console, so what is asserted here goes through the platform MBean
 * server for real — attributes read and operations invoked by name, exactly as a remote
 * client would.
 */
class RecorderJmxTests {

    private static final MBeanServer SERVER = ManagementFactory.getPlatformMBeanServer();

    private static ObjectName expectedName(String name) throws Exception {
        return new ObjectName(ExecutionRecorder.JMX_DOMAIN
                + ":type=ExecutionRecorder,name=" + ObjectName.quote(name));
    }

    @Test
    @DisplayName("register exposes attributes and operations under the expected ObjectName")
    void registerExposesAttributesAndOperationsUnderTheExpectedObjectName() throws Exception {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor executor = new ThrottledExecutor("JmxBasics");
        executor.addListener(recorder);
        executor.submit((Runnable) () -> { }).join();
        executor.waitForCompletion();

        recorder.registerMBean("jmx-basics");
        try {
            final ObjectName name = expectedName("jmx-basics");
            assertThat(SERVER.isRegistered(name)).isTrue();
            assertThat((long) SERVER.getAttribute(name, "SubmittedTotal")).isEqualTo(1L);
            assertThat((long) SERVER.getAttribute(name, "RecordedTotal")).isEqualTo(1L);
            assertThat((long) SERVER.getAttribute(name, "FailedTotal")).isZero();
            assertThat((int) SERVER.getAttribute(name, "ExecutorNodeCount")).isEqualTo(1);

            final String json = (String) SERVER.invoke(name, "renderJson", new Object[0], new String[0]);
            assertThat(json).isEqualTo(recorder.renderJson());
            new RenderTests.MiniJson(json).parseDocument();
        } finally {
            recorder.unregisterMBean("jmx-basics");
        }
    }

    @Test
    @DisplayName("registering the same name twice on one recorder is idempotent")
    void registerTwiceWithTheSameNameIsIdempotent() throws Exception {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        recorder.registerMBean("jmx-idem");
        try {
            assertThatCode(() -> recorder.registerMBean("jmx-idem")).doesNotThrowAnyException();
            assertThat(SERVER.isRegistered(expectedName("jmx-idem"))).isTrue();
        } finally {
            recorder.unregisterMBean("jmx-idem");
        }
    }

    @Test
    @DisplayName("unregister without register is a no-op; unregister removes the bean")
    void unregisterSemantics() throws Exception {
        final ExecutionRecorder recorder = new ExecutionRecorder();

        assertThatCode(() -> recorder.unregisterMBean("jmx-never-registered")).doesNotThrowAnyException();

        recorder.registerMBean("jmx-remove");
        assertThat(SERVER.isRegistered(expectedName("jmx-remove"))).isTrue();
        recorder.unregisterMBean("jmx-remove");
        assertThat(SERVER.isRegistered(expectedName("jmx-remove"))).isFalse();
    }

    @Test
    @DisplayName("names containing ObjectName metacharacters are quoted and work")
    void namesWithObjectNameMetacharactersAreQuoted() throws Exception {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final String hostile = "a,b=c:*?\"";

        recorder.registerMBean(hostile);
        try {
            assertThat(SERVER.isRegistered(expectedName(hostile))).isTrue();
        } finally {
            recorder.unregisterMBean(hostile);
        }
        assertThat(SERVER.isRegistered(expectedName(hostile))).isFalse();
    }
}
