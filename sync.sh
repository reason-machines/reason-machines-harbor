#!/usr/bin/env bash
# Usage: ./sync.sh /path/to/monorepo
# Replaces pyproject.toml and src/ with the adapter as the monorepo builds it,
# with the shared driver copied in. The README is maintained here, because the
# monorepo tracks no Markdown besides its own root files; PyPI shows it.
set -euo pipefail
monorepo=$(cd -- "$1" && pwd)
here=$(cd -- "$(dirname -- "$0")" && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
uv build --sdist --out-dir "$out" "$monorepo/packages/reason-machines-harbor"
rm -rf "$here/src" "$here/pyproject.toml"
tar -xzf "$out"/reason_machines_harbor-*.tar.gz --strip-components=1 -C "$here" \
  --exclude PKG-INFO --exclude .gitignore --exclude examples
awk '{ print } /^description = / { print "readme = \"README.md\"" }' "$here/pyproject.toml" > "$out/pyproject.toml"
mv "$out/pyproject.toml" "$here/pyproject.toml"

# The README's mode row must match the modes and default the adapter accepts.
modes=$(sed -n 's/^SESSION_MODES = {\(.*\)}$/\1/p' "$here/src/reasonmachines_harbor/_driver/driver.py" \
  | tr -d '" ' | tr ',' '\n' | sort)
default=$(sed -n 's/^ *mode: str = "\([a-z]*\)",$/\1/p' "$here/src/reasonmachines_harbor/agent.py")
row=$(grep '^| `mode` |' "$here/README.md" || true)
documented=$(printf '%s\n' "$row" | grep -o '`[a-z]*`:' | tr -d '`:' | sort)
if [ -z "$modes" ] || [ "$modes" != "$documented" ] || [[ "$row" != "| \`mode\` | \`$default\` |"* ]]; then
  printf 'README.md mode row is out of date: the adapter accepts %s (default %s)\n' \
    "$(echo $modes)" "$default" >&2
  exit 1
fi
printf 'Synced from %s at %s\n' "$monorepo" "$(git -C "$monorepo" rev-parse HEAD)"
