#!/usr/bin/env bash
# Usage: ./sync.sh /path/to/monorepo
# Replaces pyproject.toml and src/ with the adapter as the monorepo builds it,
# with the shared driver copied in.
set -euo pipefail
monorepo=$(cd -- "$1" && pwd)
here=$(cd -- "$(dirname -- "$0")" && pwd)
out=$(mktemp -d)
trap 'rm -rf "$out"' EXIT
uv build --sdist --out-dir "$out" "$monorepo/packages/reason-machines-harbor"
rm -rf "$here/src" "$here/pyproject.toml"
tar -xzf "$out"/reason_machines_harbor-*.tar.gz --strip-components=1 -C "$here" \
  --exclude PKG-INFO --exclude .gitignore --exclude examples
printf 'Synced from %s at %s\n' "$monorepo" "$(git -C "$monorepo" rev-parse HEAD)"
