package io.github.grishagrigorov.executors.observability;

import io.github.grishagrigorov.executors.spi.TaskStatus;
import java.util.List;
import java.util.Map;

/**
 * An immutable snapshot of the whole recorded forest (one tree per root executor), with
 * the two renderings a troubleshooter actually wants: an indented text tree for a console
 * or log, and JSON for anything programmatic. Both are dependency-free.
 */
public record RecorderSnapshot(List<ExecutorNodeSnapshot> roots) {

    public RecorderSnapshot {
        roots = List.copyOf(roots);
    }

    /** @return an indented, root-first text rendering of the forest */
    public String renderText() {
        final StringBuilder out = new StringBuilder();
        for (ExecutorNodeSnapshot root : roots) {
            renderNodeText(out, root, 0);
        }
        return out.toString();
    }

    private static void renderNodeText(StringBuilder out, ExecutorNodeSnapshot node, int depth) {
        out.append("  ".repeat(depth))
                .append(node.executorId())
                .append(' ').append(node.liveness())
                .append("  submitted=").append(node.submitted())
                .append(" recorded=").append(node.totalRecorded());

        for (Map.Entry<TaskStatus, Long> count : node.statusCounts().entrySet()) {
            out.append(' ').append(count.getKey().name().toLowerCase()).append('=').append(count.getValue());
        }
        if (node.durationCount() > 0) {
            out.append("  mean=").append(formatNanos((long) node.meanDurationNanos()))
                    .append(" max=").append(formatNanos(node.durationMaxNanos()));
        }
        if (node.evictedChildren() > 0) {
            out.append("  evictedChildren=").append(node.evictedChildren());
        }
        if (!node.failureCounts().isEmpty()) {
            out.append("  failures=").append(node.failureCounts());
        }
        out.append('\n');

        for (ExecutorNodeSnapshot child : node.children()) {
            renderNodeText(out, child, depth + 1);
        }
    }

    private static String formatNanos(long nanos) {
        if (nanos < 1_000L) {
            return nanos + "ns";
        }
        if (nanos < 1_000_000L) {
            return (nanos / 1_000L) + "µs";
        }
        if (nanos < 1_000_000_000L) {
            return (nanos / 1_000_000L) + "ms";
        }
        return String.format("%.1fs", nanos / 1_000_000_000.0);
    }

    /** @return the forest as a JSON document (hand-rolled; no dependencies) */
    public String renderJson() {
        return renderJson("");
    }

    /**
     * The forest as JSON, with document-level fields in front of {@code roots}.
     *
     * <p>Exists because some of what a troubleshooter wants is a property of the whole recorder
     * rather than of any node — admission totals belong to the harness, which is shared by a
     * whole tree. Rather than pushing those into every node, or splicing strings at the call
     * site, the caller hands them in already rendered.</p>
     *
     * @param leadingFields JSON object fields, comma-terminated, or empty
     * @return the JSON document
     */
    String renderJson(String leadingFields) {
        final StringBuilder out = new StringBuilder("{").append(leadingFields).append("\"roots\":[");
        for (int i = 0; i < roots.size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            renderNodeJson(out, roots.get(i));
        }
        return out.append("]}").toString();
    }

    private static void renderNodeJson(StringBuilder out, ExecutorNodeSnapshot node) {
        out.append('{');
        jsonString(out, "executorId", node.executorId()).append(',');
        jsonString(out, "path", node.path()).append(',');
        jsonString(out, "liveness", node.liveness()).append(',');
        out.append("\"submitted\":").append(node.submitted()).append(',');
        out.append("\"recorded\":").append(node.totalRecorded()).append(',');

        out.append("\"statusCounts\":{");
        boolean first = true;
        for (Map.Entry<TaskStatus, Long> count : node.statusCounts().entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append('"').append(count.getKey().name()).append("\":").append(count.getValue());
        }
        out.append("},");

        out.append("\"duration\":{\"count\":").append(node.durationCount())
                .append(",\"minNanos\":").append(node.durationMinNanos())
                .append(",\"maxNanos\":").append(node.durationMaxNanos())
                .append(",\"meanNanos\":").append((long) node.meanDurationNanos()).append("},");

        out.append("\"histogram\":{");
        for (int i = 0; i < node.histogram().length; i++) {
            if (i > 0) {
                out.append(',');
            }
            out.append('"').append(escape(DurationHistogram.LABELS[i])).append("\":").append(node.histogram()[i]);
        }
        out.append("},");

        out.append("\"failures\":{");
        first = true;
        for (Map.Entry<String, Long> failure : node.failureCounts().entrySet()) {
            if (!first) {
                out.append(',');
            }
            first = false;
            out.append('"').append(escape(failure.getKey())).append("\":").append(failure.getValue());
        }
        out.append("},");

        out.append("\"evictedChildren\":").append(node.evictedChildren()).append(',');

        out.append("\"records\":[");
        for (int i = 0; i < node.records().size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            renderRecordJson(out, node.records().get(i));
        }
        out.append("],");

        out.append("\"children\":[");
        for (int i = 0; i < node.children().size(); i++) {
            if (i > 0) {
                out.append(',');
            }
            renderNodeJson(out, node.children().get(i));
        }
        out.append("]}");
    }

    private static void renderRecordJson(StringBuilder out, TaskRecord record) {
        out.append('{');
        out.append("\"sequence\":").append(record.sequence()).append(',');
        jsonString(out, "name", record.name()).append(',');
        out.append("\"sizeEstimate\":").append(record.sizeEstimate()).append(',');
        jsonString(out, "backend", record.backend()).append(',');
        out.append("\"submittedAtEpochMillis\":").append(record.submittedAtEpochMillis()).append(',');
        out.append("\"queuedNanos\":").append(record.queuedNanos()).append(',');
        out.append("\"durationNanos\":").append(record.durationNanos()).append(',');
        jsonString(out, "status", record.status().name()).append(',');
        jsonString(out, "completionThread", record.completionThread()).append(',');
        jsonString(out, "failure", record.failure());
        out.append('}');
    }

    private static StringBuilder jsonString(StringBuilder out, String key, String value) {
        out.append('"').append(key).append("\":");
        if (value == null) {
            return out.append("null");
        }
        return out.append('"').append(escape(value)).append('"');
    }

    /** Escapes quotes, backslashes and control characters; everything else is legal raw UTF-8. */
    private static String escape(String raw) {
        final StringBuilder out = new StringBuilder(raw.length() + 8);
        for (int i = 0; i < raw.length(); i++) {
            final char c = raw.charAt(i);
            switch (c) {
                case '"' -> out.append("\\\"");
                case '\\' -> out.append("\\\\");
                case '\n' -> out.append("\\n");
                case '\r' -> out.append("\\r");
                case '\t' -> out.append("\\t");
                default -> {
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
                }
            }
        }
        return out.toString();
    }
}
