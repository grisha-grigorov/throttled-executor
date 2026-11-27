#!/usr/bin/env bash
# PreToolUse hook (Edit/Write/MultiEdit/NotebookEdit): refuse file edits on the
# mainline branch. Feature and fix work happens on feat/<slug> or fix/<slug>.
#
# Exit 2 with the reason on stderr blocks the tool call and hands the reason back
# to Claude, which then creates the branch and retries the edit. Edits outside the
# repository (plan files, memory under ~/.claude) are always allowed.
set -u
cd "${CLAUDE_PROJECT_DIR:-$(git rev-parse --show-toplevel 2>/dev/null || pwd)}" || exit 0

input=$(cat)
path=$(printf '%s' "$input" | jq -r '.tool_input.file_path // .tool_input.notebook_path // empty' 2>/dev/null)
[ -z "$path" ] && exit 0

repo=$(git rev-parse --show-toplevel 2>/dev/null) || exit 0
case "$path" in
  "$repo"/*) ;;      # inside the repo: enforce
  *) exit 0 ;;       # plan files, ~/.claude, anything else: allow
esac

branch=$(git branch --show-current 2>/dev/null)
case "$branch" in
  master|main)
    slug=$(basename "$path" | sed -E 's/\.[^.]*$//' | tr '[:upper:]_' '[:lower:]-')
    {
      echo "Editing on '$branch' is blocked: feature and fix work lives on its own branch."
      echo "Create one first, then retry the edit, e.g.:"
      echo "  git switch -c feat/<slug>     (or fix/<slug>; e.g. feat/executors-${slug})"
    } >&2
    exit 2
    ;;
esac
exit 0
