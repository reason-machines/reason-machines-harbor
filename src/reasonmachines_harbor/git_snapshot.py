"""Read-only git snapshots of the task repository around the agent phase.

Artificial Analysis fails SWE-Atlas-QnA trials that change tracked repository
files. To score that rule against the starting state or a clean tree, the
adapter records the repository at the start and the end of the agent phase,
before the verifier, as ``agent/git-snapshot-{start,end}.txt`` (raw sections)
and ``.json`` (parsed).

The script, its marker, the file names and the parsed shape are the same as
pi-unreal's ``pi_unreal_harbor/git_snapshot.py``, so one scorer reads stock Pi
and Reason trials. Task images vary (Alpine, Debian, no Python
guaranteed), so the sandbox side is plain POSIX sh. It never writes to the
repository: ``--no-optional-locks`` stops ``git status`` refreshing the index,
fsmonitor and external diff/textconv drivers are off, and ``safe.directory=*``
reads a repo owned by another user without changing config. It is bounded by
``timeout 60`` when available and always exits 0.
"""

import json
import shlex

MARKER = "@@pi-unreal-git@@"

_SCRIPT = r"""
M='@@pi-unreal-git@@'
s() { printf '\n%s %s\n' "$M" "$1"; }
g() { git -c 'safe.directory=*' -c core.fsmonitor=false -c core.quotepath=false --no-optional-locks "$@"; }
s captured_at; date -u +%Y-%m-%dT%H:%M:%SZ
s cwd; pwd
if command -v git >/dev/null 2>&1; then
  s git_available; echo yes
  top=$(g rev-parse --show-toplevel 2>/dev/null)
  if [ -n "$top" ]; then
    s toplevel; printf '%s\n' "$top"
    head=$(g rev-parse --verify -q HEAD 2>/dev/null)
    s head; printf '%s\n' "$head"
    s status; g status --porcelain=v1 --untracked-files=all 2>/dev/null; rc=$?
    s status_rc; echo "$rc"
    if [ -n "$head" ]; then
      s tracked_changed; g diff HEAD --name-only --no-ext-diff --no-textconv 2>/dev/null; rc=$?
      s tracked_changed_rc; echo "$rc"
      s diff_stat; g diff HEAD --stat --no-color --no-ext-diff --no-textconv 2>/dev/null
      S=
      if command -v sha256sum >/dev/null 2>&1; then S=sha256sum
      elif command -v shasum >/dev/null 2>&1; then S='shasum -a 256'
      elif command -v openssl >/dev/null 2>&1; then S='openssl dgst -sha256 -r'
      fi
      if [ -n "$S" ]; then
        h=$(g diff HEAD --binary --no-color --no-ext-diff --no-textconv 2>/dev/null | $S)
        s tracked_diff_sha256; printf '%s\n' "${h%% *}"
      fi
    fi
  fi
fi
s end
"""


def snapshot_command(workdir: str) -> str:
    """Shell command that prints the raw snapshot of ``workdir``'s repo and exits 0."""
    return (
        f"cd {shlex.quote(workdir)}; "
        "if timeout 1 true >/dev/null 2>&1; then T='timeout 60'; else T=; fi; "
        f"$T sh -c {shlex.quote(_SCRIPT)} 2>/dev/null || true"
    )


def parse_snapshot(text: str) -> dict:
    """Parse the raw sections into the git-snapshot-<phase>.json shape.

    ``complete`` is false when the script was cut off (no end marker), and
    ``toplevel`` is null when the working directory is not in a git repo or git
    is missing.
    """
    sections: dict[str, list[str]] = {}
    current: list[str] | None = None
    for line in text.split("\n"):
        if line.startswith(MARKER + " "):
            current = sections.setdefault(line[len(MARKER) + 1:].strip(), [])
        elif current is not None and line:
            current.append(line)

    def one(name: str) -> str | None:
        values = sections.get(name) or []
        return values[0] if values and values[0] else None

    def code(name: str) -> int | None:
        value = one(name)
        return int(value) if value is not None and value.lstrip("-").isdigit() else None

    sha = one("tracked_diff_sha256")
    return {
        "toplevel": one("toplevel"),
        "head": one("head"),
        "status": sections.get("status", []),
        "tracked_diff_sha256": sha if sha and len(sha) == 64 else None,
        "tracked_changed": sections.get("tracked_changed", []),
        "diff_stat": sections.get("diff_stat", []),
        "captured_at": one("captured_at"),
        "cwd": one("cwd"),
        "git_available": one("git_available") == "yes",
        "status_rc": code("status_rc"),
        "tracked_changed_rc": code("tracked_changed_rc"),
        "complete": "end" in sections,
    }


def write_snapshot(logs_dir, phase: str, raw: str) -> dict:
    """Write ``git-snapshot-<phase>.txt`` and its parsed ``.json`` into ``logs_dir``."""
    (logs_dir / f"git-snapshot-{phase}.txt").write_text(raw)
    snapshot = parse_snapshot(raw)
    (logs_dir / f"git-snapshot-{phase}.json").write_text(json.dumps(snapshot, indent=2) + "\n")
    return snapshot
