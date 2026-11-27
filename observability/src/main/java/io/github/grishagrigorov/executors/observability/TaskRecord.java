package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.spi.TaskStatus;

/**
 * The retained metadata of one task — roughly 200 bytes of strings and numbers.
 *
 * <p>Deliberately holds <b>no object references into the application</b>: no job, no
 * descriptor, no executor, and no {@code Throwable} (a retained exception drags its stack
 * frames and captured state along — a disguised strong reference to exactly what the
 * recorder exists to let go of). The failure is rendered to a string at completion time.</p>
 *
 * @param sequence               per-executor submission sequence number
 * @param name                   the {@code ThrottledTask} name, or {@code task#<sequence>}
 * @param sizeEstimate           bytes, {@code -1} when the submission carried none
 * @param backend                {@code FORK_JOIN} / {@code VIRTUAL_THREAD_PER_TASK}
 * @param submittedAtEpochMillis wall-clock submission time
 * @param queuedNanos            submission → body start; {@code -1} if the body never ran
 * @param durationNanos          body wall time; {@code -1} if the body never ran
 * @param status                 how the task ended
 * @param completionThread       the thread {@code onComplete} ran on — the worker for
 *                               bodies that ran, the submitter/canceller otherwise
 * @param failure                {@code Class: message (root cause Class: message)}, or
 *                               {@code null} for {@code COMPLETED}
 */
public record TaskRecord(
        long sequence,
        String name,
        long sizeEstimate,
        String backend,
        long submittedAtEpochMillis,
        long queuedNanos,
        long durationNanos,
        TaskStatus status,
        String completionThread,
        String failure) {
}
