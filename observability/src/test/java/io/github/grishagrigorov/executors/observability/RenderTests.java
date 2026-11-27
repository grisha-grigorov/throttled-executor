package io.github.grishagrigorov.executors.observability;

import static org.assertj.core.api.Assertions.assertThat;

import io.github.grishagrigorov.executors.ThrottledExecutor;
import io.github.grishagrigorov.executors.ThrottledTask;
import io.github.grishagrigorov.executors.throttling.MaxConcurrencyPolicy;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * Tests for the two renderings. The JSON one is validated by an actual (minimal) parser,
 * not by string-contains — well-formedness is the contract, and escaping bugs hide from
 * substring assertions.
 *
 * <p>Each test also prints what it rendered, so the Gradle test report (and a
 * {@code --info} console run) shows the real output a troubleshooter would see — the
 * quickest way to eyeball formatting changes.</p>
 */
class RenderTests {

    /**
     * Prints a rendering under a labelled rule so it is findable in the test output.
     * JSON is pretty-printed for reading — {@code renderJson()} itself stays compact
     * (that is the wire format); this reformatting is display-only.
     */
    private static void printRendering(String label, String rendering) {
        System.out.println("── " + label + " " + "─".repeat(Math.max(0, 60 - label.length())));
        final String trimmed = rendering.stripLeading();
        System.out.println(trimmed.startsWith("{") || trimmed.startsWith("[") ? prettyJson(rendering) : rendering);
        System.out.println();
    }

    /**
     * Minimal escape-aware JSON re-indenter: newline + indent after {@code '{'}, {@code '['}
     * and {@code ','}, before {@code '}'} and {@code ']'}, a space after {@code ':'} —
     * while structural characters inside quoted strings are left untouched.
     */
    private static String prettyJson(String json) {
        final StringBuilder out = new StringBuilder(json.length() * 2);
        int depth = 0;
        boolean inString = false;

        for (int i = 0; i < json.length(); i++) {
            final char c = json.charAt(i);

            if (inString) {
                out.append(c);
                if (c == '\\') {
                    out.append(json.charAt(++i));   // the escaped character, verbatim
                } else if (c == '"') {
                    inString = false;
                }
                continue;
            }

            switch (c) {
                case '"' -> {
                    inString = true;
                    out.append(c);
                }
                case '{', '[' -> {
                    out.append(c);
                    // Keep empty containers on one line: {} and []
                    if (i + 1 < json.length() && json.charAt(i + 1) == (c == '{' ? '}' : ']')) {
                        out.append(json.charAt(++i));
                    } else {
                        depth++;
                        newline(out, depth);
                    }
                }
                case '}', ']' -> {
                    depth--;
                    newline(out, depth);
                    out.append(c);
                }
                case ',' -> {
                    out.append(c);
                    newline(out, depth);
                }
                case ':' -> out.append(": ");
                default -> out.append(c);
            }
        }
        return out.toString();
    }

    private static void newline(StringBuilder out, int depth) {
        out.append('\n').append("  ".repeat(Math.max(0, depth)));
    }

    @Test
    @DisplayName("renderText indents children under parents, root first")
    void renderTextIndentsChildrenUnderParentsRootFirst() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("TextRoot");
        root.addListener(recorder);
        final ThrottledExecutor child = root.newChild("TextChild");

        root.submit((Runnable) () -> { }).join();
        child.submit((Runnable) () -> { }).join();
        root.waitForCompletion();

        final String text = recorder.snapshot().renderText();
        printRendering("renderText — root with one child", text);
        final int rootIndex = text.indexOf(root.executorId);
        final int childIndex = text.indexOf(child.executorId);

        assertThat(rootIndex).isNotNegative();
        assertThat(childIndex).as("child rendered after its parent").isGreaterThan(rootIndex);

        final String childLine = text.substring(text.lastIndexOf('\n', childIndex) + 1);
        assertThat(childLine).as("child indented one level").startsWith("  ");
        assertThat(text).contains("submitted=");
    }

    @Test
    @DisplayName("renderJson is well-formed")
    void renderJsonIsWellFormed() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("JsonRoot");
        root.addListener(recorder);
        root.newChild("JsonChild").submit((Runnable) () -> {
            throw new IllegalStateException("to exercise the failure fields");
        });
        ExecutionRecorderTests.swallowAggregate(root);

        final String json = recorder.snapshot().renderJson();
        printRendering("renderJson — tree with a failure", json);
        new MiniJson(json).parseDocument();   // throws on any malformation
    }

    @Test
    @DisplayName("renderJson escapes quotes, backslashes and control characters")
    void renderJsonEscapesQuotesBackslashesAndControlCharacters() {
        final ExecutionRecorder recorder = new ExecutionRecorder();
        final ThrottledExecutor root = new ThrottledExecutor("EscapeRoot");
        root.addListener(recorder);

        final String hostileName = "he said \"x\\y\"\nnext\tline";
        root.submitThrottled(
                List.of(new ThrottledTask<>((Runnable) () -> { }, 1L, hostileName)),
                new MaxConcurrencyPolicy(2));
        root.waitForCompletion();

        final String json = recorder.snapshot().renderJson();
        printRendering("renderJson — hostile task name, escaped", json);
        new MiniJson(json).parseDocument();
        assertThat(json).contains("\\\"x\\\\y\\\"").contains("\\n").contains("\\t");
    }

    /**
     * A deliberately tiny recursive-descent JSON validator — enough to reject any
     * malformed document this module could produce. Throws {@link AssertionError} with a
     * position on failure.
     */
    static final class MiniJson {

        private final String text;
        private int at;

        MiniJson(String text) {
            this.text = text;
        }

        void parseDocument() {
            skipWs();
            parseValue();
            skipWs();
            if (at != text.length()) {
                throw fail("trailing content");
            }
        }

        private void parseValue() {
            skipWs();
            if (at >= text.length()) {
                throw fail("unexpected end");
            }
            switch (text.charAt(at)) {
                case '{' -> parseObject();
                case '[' -> parseArray();
                case '"' -> parseString();
                case 't' -> expect("true");
                case 'f' -> expect("false");
                case 'n' -> expect("null");
                default -> parseNumber();
            }
        }

        private void parseObject() {
            expectChar('{');
            skipWs();
            if (peek() == '}') {
                at++;
                return;
            }
            while (true) {
                skipWs();
                parseString();
                skipWs();
                expectChar(':');
                parseValue();
                skipWs();
                if (peek() == ',') {
                    at++;
                    continue;
                }
                expectChar('}');
                return;
            }
        }

        private void parseArray() {
            expectChar('[');
            skipWs();
            if (peek() == ']') {
                at++;
                return;
            }
            while (true) {
                parseValue();
                skipWs();
                if (peek() == ',') {
                    at++;
                    continue;
                }
                expectChar(']');
                return;
            }
        }

        private void parseString() {
            expectChar('"');
            while (true) {
                if (at >= text.length()) {
                    throw fail("unterminated string");
                }
                final char c = text.charAt(at++);
                if (c == '"') {
                    return;
                }
                if (c < 0x20) {
                    throw fail("raw control character in string");
                }
                if (c == '\\') {
                    final char esc = text.charAt(at++);
                    if (esc == 'u') {
                        for (int i = 0; i < 4; i++) {
                            final char hex = text.charAt(at++);
                            if (Character.digit(hex, 16) < 0) {
                                throw fail("bad unicode escape");
                            }
                        }
                    } else if ("\"\\/bfnrt".indexOf(esc) < 0) {
                        throw fail("bad escape \\" + esc);
                    }
                }
            }
        }

        private void parseNumber() {
            final int start = at;
            if (peek() == '-') {
                at++;
            }
            while (at < text.length() && "0123456789.eE+-".indexOf(text.charAt(at)) >= 0) {
                at++;
            }
            if (at == start) {
                throw fail("expected a value");
            }
            Double.parseDouble(text.substring(start, at));
        }

        private char peek() {
            if (at >= text.length()) {
                throw fail("unexpected end");
            }
            return text.charAt(at);
        }

        private void expectChar(char expected) {
            if (at >= text.length() || text.charAt(at) != expected) {
                throw fail("expected '" + expected + "'");
            }
            at++;
        }

        private void expect(String literal) {
            if (!text.startsWith(literal, at)) {
                throw fail("expected " + literal);
            }
            at += literal.length();
        }

        private void skipWs() {
            while (at < text.length() && Character.isWhitespace(text.charAt(at))) {
                at++;
            }
        }

        private AssertionError fail(String reason) {
            final int from = Math.max(0, at - 30);
            final int to = Math.min(text.length(), at + 30);
            return new AssertionError("malformed JSON at " + at + " (" + reason + "): ..." + text.substring(from, to) + "...");
        }
    }
}
