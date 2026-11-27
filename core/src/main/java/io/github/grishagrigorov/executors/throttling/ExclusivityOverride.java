package io.github.grishagrigorov.executors.throttling;

import io.github.grishagrigorov.executors.ThrottledTask;

/**
 * Whether a policy class overrides {@link AdmissionPolicy#requiresExclusive}, decided once per
 * class.
 *
 * <p>Backs the default {@link AdmissionPolicy#mayRequireExclusive()}. The default has to be
 * <em>derived</em> rather than a plain {@code false}: a policy that overrides
 * {@code requiresExclusive} and never hears of the newer method must still get the full
 * per-candidate scan, or its isolation silently disappears. Reading the override off the class
 * is what makes "forgot to opt in" impossible.</p>
 *
 * <p>A {@link ClassValue}, so the reflection runs once per class for the life of the JVM and
 * the per-scan cost is a cache read. Anything reflection cannot answer is read as "may" — the
 * side that costs a scan rather than an isolation.</p>
 */
final class ExclusivityOverride {

    private static final ClassValue<Boolean> DECLARES =
            declaresBelow(AdmissionPolicy.class, "requiresExclusive", ThrottledTask.class);

    private ExclusivityOverride() {
    }

    /**
     * The one override probe in the family: whether a runtime class declares {@code method}
     * anywhere below {@code base}. {@link ForwardingAdmissionPolicy} asks the same question
     * against itself as the base — it declares {@code requiresExclusive} to forward it, so
     * "below the interface" would be true of every subclass — and the two must not drift
     * (bridge handling, fallback direction), or a policy written on the interface and the
     * same policy rewritten on the base would answer {@code mayRequireExclusive} differently.
     *
     * @param base           the class an override must be declared strictly below
     * @param method         the method name
     * @param parameterTypes its parameter types
     * @return a per-class cache of the answer; anything reflection cannot tell is {@code true}
     */
    static ClassValue<Boolean> declaresBelow(Class<?> base, String method, Class<?>... parameterTypes) {
        return new ClassValue<Boolean>() {
            @Override
            protected Boolean computeValue(Class<?> type) {
                try {
                    return type.getMethod(method, parameterTypes).getDeclaringClass() != base;
                } catch (NoSuchMethodException | SecurityException cannotTell) {
                    return true;
                }
            }
        };
    }

    /**
     * @param policyClass the policy's runtime class
     * @return whether it declares its own {@code requiresExclusive}
     */
    static boolean declaredBy(Class<?> policyClass) {
        return DECLARES.get(policyClass);
    }
}
