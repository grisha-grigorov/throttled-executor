package io.github.grishagrigorov.executors.throttling;

import java.util.Map;
import java.util.Set;

import io.github.grishagrigorov.executors.ThrottledTask;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Per-resource permits. {@code MaxConcurrencyPolicy} bounds the pool as a whole, which says
 * nothing about the four connections the database will actually give you.
 */
@DisplayName("ResourcePermitPolicy")
class ResourcePermitPolicyTests {

    @Nested
    @DisplayName("metering")
    class Metering {

        @Test
        @DisplayName("admits up to the tag's limit, then refuses until one finishes")
        void countsPerTag() {

            final ResourcePermitPolicy policy = new ResourcePermitPolicy(Map.of("hsqldb", 2));

            final ThrottledTask<Runnable> a = tagged("A", "hsqldb");
            final ThrottledTask<Runnable> b = tagged("B", "hsqldb");
            final ThrottledTask<Runnable> c = tagged("C", "hsqldb");

            assertThat(policy.admit(a, snapshot())).isTrue();
            policy.onTaskStart(a);
            assertThat(policy.admit(b, snapshot())).isTrue();
            policy.onTaskStart(b);

            assertThat(policy.admit(c, snapshot())).as("both permits are out").isFalse();

            policy.onTaskEnd(a);
            assertThat(policy.admit(c, snapshot())).isTrue();
        }

        @Test
        @DisplayName("tags are independent — a full database does not stop work on the sensor link")
        void tagsAreIndependent() {

            final ResourcePermitPolicy policy = new ResourcePermitPolicy(Map.of("hsqldb", 1, "spmbb", 1));

            final ThrottledTask<Runnable> db = tagged("Db", "hsqldb");
            policy.onTaskStart(db);

            assertThat(policy.admit(tagged("Db2", "hsqldb"), snapshot())).isFalse();
            assertThat(policy.admit(tagged("Sensor", "spmbb"), snapshot())).isTrue();

            policy.onTaskEnd(db);
        }

        @Test
        @DisplayName("a task needing several resources waits for ALL of them")
        void everyTagMustHaveRoom() {

            final ResourcePermitPolicy policy = new ResourcePermitPolicy(Map.of("hsqldb", 2, "spmbb", 1));

            final ThrottledTask<Runnable> sensor = tagged("Sensor", "spmbb");
            policy.onTaskStart(sensor);

            final ThrottledTask<Runnable> both = tagged("Sync", "hsqldb", "spmbb");
            assertThat(policy.admit(both, snapshot()))
                    .as("hsqldb has room, spmbb does not")
                    .isFalse();

            policy.onTaskEnd(sensor);
            assertThat(policy.admit(both, snapshot())).isTrue();
        }

        @Test
        @DisplayName("an untagged task, and a tag with no limit, are unconstrained")
        void unmeteredTasksPassThrough() {

            final ResourcePermitPolicy policy = new ResourcePermitPolicy(Map.of("hsqldb", 1));

            final ThrottledTask<Runnable> db = tagged("Db", "hsqldb");
            policy.onTaskStart(db);

            assertThat(policy.admit(ThrottledTask.<Runnable>of(() -> { }, 10L, "Plain"), snapshot())).isTrue();
            assertThat(policy.admit(tagged("Other", "kafka"), snapshot()))
                    .as("no limit configured for that tag")
                    .isTrue();

            policy.onTaskEnd(db);
        }
    }

    @Nested
    @DisplayName("bookkeeping")
    class Bookkeeping {

        @Test
        @DisplayName("permits return exactly, so a long-lived policy does not leak them")
        void permitsReturn() {

            final ResourcePermitPolicy policy = new ResourcePermitPolicy(Map.of("hsqldb", 1));

            for (int i = 0; i < 100; i++) {
                final ThrottledTask<Runnable> task = tagged("T" + i, "hsqldb");
                assertThat(policy.admit(task, snapshot())).isTrue();
                policy.onTaskStart(task);
                policy.onTaskEnd(task);
            }

            assertThat(policy.inUse("hsqldb")).isZero();
        }

        @Test
        @DisplayName("it never claims an in-flight count — a resource cap is not a reason to disable the liveness floor")
        void doesNotHijackTheFloor() {
            assertThat(new ResourcePermitPolicy(Map.of("hsqldb", 1)).inFlightTaskCount()).isEqualTo(-1);
        }
    }

    @Nested
    @DisplayName("construction")
    class Construction {

        @Test
        @DisplayName("a limit below one is rejected — it would mean 'never', which is a configuration mistake")
        void rejectsNonPositiveLimits() {
            assertThatThrownBy(() -> new ResourcePermitPolicy(Map.of("hsqldb", 0)))
                    .isInstanceOf(IllegalArgumentException.class);
        }

        @Test
        @DisplayName("the caller's map is copied")
        void copiesTheMap() {

            final Map<String, Integer> mutable = new java.util.HashMap<>(Map.of("hsqldb", 1));
            final ResourcePermitPolicy policy = new ResourcePermitPolicy(mutable);

            mutable.put("hsqldb", 99);

            final ThrottledTask<Runnable> db = tagged("Db", "hsqldb");
            policy.onTaskStart(db);
            assertThat(policy.admit(tagged("Db2", "hsqldb"), snapshot())).isFalse();
            policy.onTaskEnd(db);
        }
    }

    private static ThrottledTask<Runnable> tagged(String name, String... tags) {
        return ThrottledTask.of(() -> { }, 10L, name, Set.of(tags));
    }

    private static PoolSnapshot snapshot() {
        return new PoolSnapshot() {
            @Override
            public int runningTasks() {
                return 1;
            }

            @Override
            public long trackedSizeBytes() {
                return 0;
            }
        };
    }
}
