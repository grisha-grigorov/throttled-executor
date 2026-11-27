#!/usr/bin/env bash
# Stop hook: refuse to end the turn while `./gradlew check` fails.
#
# Runs only when the working tree has uncommitted changes to sources or build
# files, so answering a question never triggers a build. Exit 2 with the failure
# on stderr makes Claude Code block the stop and hand the output back to Claude.
set -u
cd "${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel)}" || exit 0

input=$(cat)
# Claude Code sets stop_hook_active when this hook already blocked once this
# turn. Let the second stop through so a persistently red build cannot loop.
if printf '%s' "$input" | jq -e '.stop_hook_active == true' >/dev/null 2>&1; then
  exit 0
fi

changed=$(git status --porcelain --untracked-files=all 2>/dev/null \
  | grep -E '\.(java|gradle|kts|toml|properties|xml)$|module-info' || true)
[ -z "$changed" ] && exit 0

log=$(mktemp -t gradle-check.XXXXXX)
if ./gradlew check -q --console=plain >"$log" 2>&1; then
  rm -f "$log"
  exit 0
fi

{
  echo "./gradlew check FAILED. Fix the failures before ending the turn."
  echo "--- last 60 lines of output (full log: $log) ---"
  tail -60 "$log"
} >&2
exit 2
