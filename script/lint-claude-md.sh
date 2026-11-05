#!/usr/bin/env bash
#
# Fail if any CLAUDE.md or AGENTS.md file exceeds the word cap. These files
# are loaded into the context of every agent session, so every word is paid
# for on every request. The cap forces the files to stay curated: to add
# something, trim something else. Words rather than lines, because Markdown
# prose is often left unwrapped and a line cap would reward long lines.
#
# Source of truth: the claude-md skill in planitar-claude-tools
# (code/plugins/planitar-dev-tools/skills/claude-md/scripts/lint-claude-md.sh).
# Repos keep an unchanged copy; update it from there rather than editing here.

set -euo pipefail

MAX_WORDS=1500

cd "$(git rev-parse --show-toplevel)"

fail=0
while IFS= read -r file; do
  words=$(($(wc -w <"${file}")))
  if [[ "${words}" -gt "${MAX_WORDS}" ]]; then
    >&2 echo "${file}: ${words} words, exceeds the ${MAX_WORDS}-word cap"
    fail=1
  else
    echo "${file}: ${words} words (cap ${MAX_WORDS})"
  fi
done < <(find . \( -name CLAUDE.md -o -name AGENTS.md \) \
  -not -path './.git/*' \
  -not -path '*/node_modules/*' \
  -not -path '*/vendor/*' \
  -not -path './build/*')

exit "${fail}"
