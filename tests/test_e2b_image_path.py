"""E2B login shells keep the task image's PATH.

E2B runs every command in a login shell. A Debian-style /etc/profile resets PATH
before it sources /etc/profile.d, so ReasonE2BEnvironment writes a profile.d
script that puts the image's PATH entries back.
"""
import asyncio
import shutil
import subprocess
from pathlib import Path
from types import SimpleNamespace

import pytest
from harbor.environments.e2b import E2BEnvironment

from reasonmachines_harbor import environment
from reasonmachines_harbor.environment import PROFILE_SCRIPT, ReasonE2BEnvironment, image_path_profile

DEBIAN_ROOT_PATH = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# python:3.11-slim plus a task virtualenv, as in terminal-bench/roy-polymorph-cn.
IMAGE_PATH = "/opt/venv/bin:/root/.local/bin:/usr/local/bin:/usr/local/sbin:/usr/sbin:/usr/bin:/sbin:/bin"
SHELLS = [shell for shell in ("sh", "bash", "dash") if shutil.which(shell)]


def login_path(tmp_path: Path, shell: str, profile_d: dict[str, str], sources: int = 1) -> str:
    """PATH after a Debian-style /etc/profile: it resets PATH, then sources profile.d."""
    etc = tmp_path / "etc"
    (etc / "profile.d").mkdir(parents=True, exist_ok=True)
    for name, body in profile_d.items():
        (etc / "profile.d" / name).write_text(body)
    source_profile_d = f'for i in "{etc}"/profile.d/*.sh; do [ -r "$i" ] && . "$i"; done\n'
    (etc / "profile").write_text(f'PATH="{DEBIAN_ROOT_PATH}"\nexport PATH\n' + source_profile_d * sources)
    result = subprocess.run(
        [shell, "-c", f'. "{etc}/profile"; printf "%s" "$PATH"'],
        env={"PATH": IMAGE_PATH, "HOME": str(tmp_path)}, cwd=tmp_path,
        capture_output=True, text=True, check=True)
    assert result.stderr == ""
    return result.stdout


@pytest.mark.parametrize("shell", SHELLS)
def test_login_shell_profile_reset_keeps_the_image_path(tmp_path, shell):
    assert login_path(tmp_path, shell, {}) == DEBIAN_ROOT_PATH  # the reset this fixes

    path = login_path(tmp_path, shell, {"00-reason-image-path.sh": image_path_profile(IMAGE_PATH)})

    # Missing image entries come first, in image order; the shell's own entries keep theirs.
    assert path == "/opt/venv/bin:/root/.local/bin:" + DEBIAN_ROOT_PATH


@pytest.mark.parametrize("shell", SHELLS)
def test_profile_is_idempotent_and_leaves_a_complete_path_alone(tmp_path, shell):
    profile = {"00-reason-image-path.sh": image_path_profile(IMAGE_PATH)}

    assert login_path(tmp_path, shell, profile, sources=2) == "/opt/venv/bin:/root/.local/bin:" + DEBIAN_ROOT_PATH
    assert login_path(tmp_path, shell, {"00-reason-image-path.sh": image_path_profile(DEBIAN_ROOT_PATH)}) == DEBIAN_ROOT_PATH


@pytest.mark.parametrize("shell", SHELLS)
def test_profile_quotes_entries_literally(tmp_path, shell):
    marker = tmp_path / "expanded"
    image_path = f"/opt/it's here/bin::/opt/$(touch {marker})/bin:/opt/*/bin:" + DEBIAN_ROOT_PATH

    path = login_path(tmp_path, shell, {"00-reason-image-path.sh": image_path_profile(image_path)})

    assert path == f"/opt/it's here/bin:/opt/$(touch {marker})/bin:/opt/*/bin:" + DEBIAN_ROOT_PATH
    assert not marker.exists()


class FakeFiles:
    def __init__(self):
        self.writes = []

    async def write(self, path, data, user=None):
        self.writes.append((path, data, user))


def started(monkeypatch, image_env: dict[str, str], task_env: dict[str, str] | None = None) -> FakeFiles:
    """Run ReasonE2BEnvironment.start against a fake sandbox, without E2B."""
    files = FakeFiles()

    async def create_sandbox(self, force_build):
        self._sandbox = SimpleNamespace(files=files)

    monkeypatch.setattr(environment, "image_runtime", lambda _environment: (image_env, None))
    monkeypatch.setattr(E2BEnvironment, "start", create_sandbox)
    env = object.__new__(ReasonE2BEnvironment)
    env.task_env_config = SimpleNamespace(env=task_env)
    env._persistent_env = {}
    asyncio.run(env.start(force_build=False))
    return files


def test_start_writes_the_profile_when_the_image_sets_path(monkeypatch):
    files = started(monkeypatch, {"PATH": IMAGE_PATH, "VIRTUAL_ENV": "/opt/venv"})

    assert files.writes == [(PROFILE_SCRIPT, image_path_profile(IMAGE_PATH), "root")]
    assert PROFILE_SCRIPT.startswith("/etc/profile.d/") and PROFILE_SCRIPT.endswith(".sh")


def test_start_uses_the_task_path_over_the_image_path(monkeypatch):
    files = started(monkeypatch, {"PATH": IMAGE_PATH}, task_env={"PATH": "/task/bin:/usr/bin"})

    assert files.writes == [(PROFILE_SCRIPT, image_path_profile("/task/bin:/usr/bin"), "root")]


def test_start_writes_nothing_when_the_image_has_no_path(monkeypatch):
    assert started(monkeypatch, {"VIRTUAL_ENV": "/opt/venv"}).writes == []
    assert started(monkeypatch, {}, task_env={"PATH": "/task/bin"}).writes == []
