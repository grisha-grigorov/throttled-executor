package io.github.grishagrigorov.executors.adaptive;

import io.github.grishagrigorov.executors.throttling.AdmissionPolicy;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.nio.file.Path;
import java.time.Duration;
import java.time.InstantSource;
import java.util.Objects;
import java.util.OptionalDouble;
import java.util.OptionalLong;

/**
 * The facade: one measuring {@linkplain #listener() listener} to attach to each executor
 * root, {@linkplain #policy(AdmissionPolicy) policy decorators} that resize tasks on what
 * was learned, and the {@linkplain #effectiveSize(String, long) resolution rule} that joins
 * them.
 *
 * <pre>{@code
 * try (AdaptiveSizing adaptive = AdaptiveSizing.onFile(Path.of("task-profiles.tsv"))) {
 *     root.addListener(adaptive.listener());
 *     AdmissionPolicy policy = adaptive.policy(
 *             CompositePolicy.allOf(GlobalMemoryBudgetPolicy.processWideHeapBudget(), new MaxConcurrencyPolicy(8)));
 *     root.submitThrottled(tasks, policy);      // sizes may be -1: learned on the first run
 *     root.waitForCompletion();
 * }                                             // close() saves the profiles
 * }</pre>
 *
 * <p>What was learned is trusted only while it is <em>fresh</em>: measured under the
 * configured {@link AdaptiveConfig#version() version} and within
 * {@link AdaptiveConfig#maxSampleAge() maxSampleAge} of the {@linkplain #clock() clock}. A
 * task whose samples have all gone stale is treated as never seen — quarantined and
 * measured again — and {@link #freshProfile(String)} shows exactly what the sizing reads.</p>
 */
public final class AdaptiveSizing implements AutoCloseable {

    /** The effective size of a task nothing is known about: neither declared nor learned. */
    public static final long UNKNOWN = -1L;

    /**
     * Upper clamp of every learned estimate, so that a ledger summing a few of them cannot
     * overflow {@code long}.
     */
    public static final long MAX_EFFECTIVE_SIZE = Long.MAX_VALUE / 4;

    private final ProfileStore store;
    private final AdaptiveConfig config;
    private final InstantSource clock;
    private volatile AdaptiveSizingListener listener;    // created on first listener()

    private AdaptiveSizing(ProfileStore store, AdaptiveConfig config, InstantSource clock) {
        this.store = store;
        this.config = config;
        this.clock = clock;
    }

    /**
     * Learning that lasts for this JVM only, with {@link AdaptiveConfig#defaults()}.
     *
     * @return a new instance over an {@link InMemoryProfileStore}
     */
    public static AdaptiveSizing inMemory() {
        final AdaptiveConfig config = AdaptiveConfig.defaults();
        return of(new InMemoryProfileStore(config.ringSize()), config);
    }

    /**
     * Learning that survives restarts, with {@link AdaptiveConfig#defaults()}.
     *
     * @param file the profile file; loaded now if it exists, written by {@link #save()}
     * @return a new instance over a {@link FileProfileStore}
     */
    public static AdaptiveSizing onFile(Path file) {
        return onFile(file, AdaptiveConfig.defaults());
    }

    /**
     * Learning that survives restarts, with the given configuration — typically
     * {@code AdaptiveConfig.defaults().withVersion(…)} so that profiles from an earlier
     * release are measured afresh.
     *
     * @param file   the profile file; loaded now if it exists, written by {@link #save()}
     * @param config the tuning knobs; its ring size is the store's
     * @return a new instance over a {@link FileProfileStore}
     */
    public static AdaptiveSizing onFile(Path file, AdaptiveConfig config) {
        Objects.requireNonNull(config, "config");
        return of(new FileProfileStore(Objects.requireNonNull(file, "file"), config.ringSize()), config);
    }

    /**
     * @param store  where profiles live
     * @param config the tuning knobs
     * @return a new instance on the system clock
     */
    public static AdaptiveSizing of(ProfileStore store, AdaptiveConfig config) {
        return of(store, config, InstantSource.system());
    }

    /**
     * @param store  where profiles live
     * @param config the tuning knobs
     * @param clock  what stamps new samples and what {@link AdaptiveConfig#maxSampleAge()}
     *               is measured against — injectable for tests
     * @return a new instance
     */
    public static AdaptiveSizing of(ProfileStore store, AdaptiveConfig config, InstantSource clock) {
        return new AdaptiveSizing(Objects.requireNonNull(store, "store"), Objects.requireNonNull(config, "config"),
                Objects.requireNonNull(clock, "clock"));
    }

    /** @return the one measuring listener — attach it to every executor root that learns */
    public AdaptiveSizingListener listener() {
        AdaptiveSizingListener l = listener;
        if (l == null) {
            synchronized (this) {
                l = listener;
                if (l == null) {
                    /*
                     * Resolve the bean once. ManagementFactory.getMemoryMXBean() is not a field
                     * read: it walks the platform components through two stream pipelines and a
                     * static synchronized helper on every call — on every task start and end,
                     * and every sampler tick, for a JVM singleton that never changes.
                     */
                    final MemoryMXBean memory = ManagementFactory.getMemoryMXBean();
                    l = new AdaptiveSizingListener(store, config, AllocationMeter.platform(),
                            () -> memory.getHeapMemoryUsage().getUsed(), clock);
                    listener = l;
                }
            }
        }
        return l;
    }

    /**
     * A new decorator around {@code delegate}, resizing each candidate on what was learned
     * before the delegate sees it. Each call creates a policy with its own ledger — share
     * one instance between the submissions that should see each other.
     *
     * @param delegate the policy that decides on the resized tasks
     * @return the decorator
     */
    public AdaptiveSizingPolicy policy(AdmissionPolicy delegate) {
        return new AdaptiveSizingPolicy(this, Objects.requireNonNull(delegate, "delegate"));
    }

    /**
     * The resolution rule — what the delegate policy is shown for a task:
     * <pre>
     *   no profile          : declared &gt;= 0 -&gt; declared (unchanged, no headroom)   declared &lt; 0 -&gt; UNKNOWN
     *   declared &gt; 0, ratio : ceil(declared * maxRatio * headroom)                 no ratio -&gt; declared
     *   declared == 0       : ceil(absolute * headroom)                             no absolute -&gt; 0
     *   declared &lt; 0        : ceil(absolute * headroom)                             no absolute -&gt; UNKNOWN
     * </pre>
     * Learned values are clamped to {@code [0, MAX_EFFECTIVE_SIZE]}; a declared size nothing
     * was learned about passes through untouched, so an executor whose tasks are all
     * declared and never seen behaves exactly as if undecorated. "No profile" includes a
     * profile with no {@linkplain #freshProfile(String) fresh} sample.
     *
     * @param name     the task name; {@code null} (an unnamed task) resolves to {@code declared}
     *                 — for {@code -1} that is the same value as {@link #UNKNOWN}, which is why
     *                 {@link AdaptiveSizingPolicy} never asks for an unnamed task: nothing can
     *                 be learned about it, so it is shown to the delegate exactly as declared
     * @param declared the declared size, {@code -1} for none
     * @return the size the delegate policy will be shown, or {@link #UNKNOWN}
     */
    public long effectiveSize(String name, long declared) {
        final TaskProfile profile = freshProfile(name);
        if (profile == null) {
            return canBeUnknown(declared) ? UNKNOWN : declared;
        }
        if (declared > 0) {
            final OptionalDouble ratio = profile.ratio(config.estimator());
            return ratio.isPresent() ? withHeadroom((double) declared * ratio.getAsDouble()) : declared;
        }
        final OptionalLong absolute = profile.absolute(config.estimator());
        if (absolute.isPresent()) {
            return withHeadroom(absolute.getAsLong());
        }
        return canBeUnknown(declared) ? UNKNOWN : 0;
    }

    /**
     * Whether a task with this declared size could ever resolve to {@link #UNKNOWN}.
     *
     * <p>The one abbreviation of {@link #effectiveSize} the fast paths may take, kept beside
     * it so the rule cannot drift: a declared size {@code >= 0} is returned as declared (or
     * scaled, or {@code 0}) whatever the store holds, so only a negative one can come back
     * {@code UNKNOWN}.</p>
     *
     * @param declared the declared size, negative for "none"
     * @return whether {@code effectiveSize} could answer {@link #UNKNOWN} for it
     */
    static boolean canBeUnknown(long declared) {
        return declared < 0;
    }


    private long withHeadroom(double learned) {
        final double scaled = Math.ceil(learned * config.headroom());
        if (scaled >= (double) MAX_EFFECTIVE_SIZE) {
            return MAX_EFFECTIVE_SIZE;
        }
        return scaled <= 0 ? 0 : (long) scaled;
    }

    /**
     * @param name the task name
     * @return everything the store holds for it — stale samples included — or {@code null}
     * @see #freshProfile(String)
     */
    public TaskProfile profile(String name) {
        return store.find(name);
    }

    /**
     * What {@link #effectiveSize} actually reads: the profile restricted to the samples
     * measured under the configured version and within {@code maxSampleAge} of the clock
     * — see {@link TaskProfile#fresh(String, long)}. With neither knob set this is
     * {@link #profile(String)} itself.
     *
     * @param name the task name
     * @return the fresh view (possibly with no samples), or {@code null} when the store
     *         holds nothing for the name
     */
    public TaskProfile freshProfile(String name) {
        final TaskProfile profile = name == null ? null : store.find(name);
        if (profile == null) {
            return null;
        }
        final Duration maxAge = config.maxSampleAge();
        final long notBefore = maxAge == null ? Long.MIN_VALUE : clock.millis() - maxAge.toMillis();
        return profile.fresh(config.version(), notBefore);
    }

    /**
     * Drops everything learned about a name — its next run is quarantined and measured
     * again. The manual lever for "this part of the application changed"; the automatic
     * ones are {@link AdaptiveConfig#version()} and {@link AdaptiveConfig#maxSampleAge()}.
     *
     * @param name the task name
     * @return whether there was anything to drop
     * @see ProfileStore#forget(String)
     */
    public boolean forget(String name) {
        return store.forget(name);
    }

    /** @return the clock that stamps samples and ages them */
    public InstantSource clock() {
        return clock;
    }

    /** @return the store */
    public ProfileStore store() {
        return store;
    }

    /** @return the configuration */
    public AdaptiveConfig config() {
        return config;
    }

    /**
     * Persists the profiles through the store.
     *
     * @throws IOException when the store cannot persist
     */
    public void save() throws IOException {
        store.save();
    }

    /**
     * Stops the heap sampler, if one was started, then saves the profiles.
     *
     * @throws IOException when the store cannot persist
     */
    @Override
    public void close() throws IOException {
        final AdaptiveSizingListener l = listener;
        if (l != null) {
            l.close();
        }
        store.save();
    }
}
