"""The task repo is snapshotted at the start and end of the agent phase, and a
sync-yield Session is told the trial's working directory."""
import asyncio
import hashlib
import io
import json
import subprocess
import tarfile
from types import SimpleNamespace

import httpx
import pytest

from reasonmachines_harbor import ReasonAgent
from reasonmachines_harbor._driver import driver as driver_module
from reasonmachines_harbor._driver.driver import (REASON_V7_SYNC_YIELD_MACHINE_SDK_RUNTIME_PROFILE,
                                                  REASON_V7_SYNC_YIELD_RUNTIME_PROFILE)
from reasonmachines_harbor.git_snapshot import MARKER
from test_rolling_deploy import CHILD, Guest, Server


def git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


def task_repo(tmp_path):
    repo = tmp_path / "app"
    repo.mkdir()
    git(repo, "init", "-q")
    (repo / "a.py").write_text("print(1)\n")
    git(repo, "add", "a.py")
    git(repo, "-c", "user.email=t@t", "-c", "user.name=t", "commit", "-qm", "init")
    return repo


class RepoGuest(Guest):
    """A guest whose WORKDIR is a real git repo, and which runs the snapshot script."""

    def __init__(self, repo):
        self.repo = repo

    async def exec(self, command, timeout_sec=None, **_):
        if MARKER in command:
            # A login shell's banner before the snapshot is ignored by the parse.
            result = subprocess.run(["sh", "-c", f"echo banner; {command}"], capture_output=True, text=True)
            return SimpleNamespace(return_code=result.returncode, stdout=result.stdout, stderr=result.stderr)
        if command.endswith("pwd -P"):
            return SimpleNamespace(return_code=0, stdout=f"__reason_guest_output__\n{self.repo}\n", stderr="")
        return await super().exec(command, timeout_sec)


class SyncYieldServer(Server):
    """Advertises a sync-yield profile; the 'agent' edits a tracked file when its Session starts."""

    def __init__(self, profile, repo):
        super().__init__(capacity_rejections=0)
        self.profile, self.repo = profile, repo

    def __call__(self, request):
        if request.url.path == "/v3/deployment":
            return httpx.Response(200, json={
                "session_modes": ["ephemeral"], "workspace_ownerships": ["externally-managed"],
                "experimental_runtime_profiles": [self.profile], "commit_sha": "a" * 40})
        if request.url.path == f"/v3/organizations/{CHILD}/sessions" and request.method == "POST":
            (self.repo / "a.py").write_text("print(2)\n")
            (self.repo / "notes.txt").write_text("scratch\n")
        return super().__call__(request)


def run_agent(tmp_path, monkeypatch, profile, fail=False):
    repo = task_repo(tmp_path)
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as bundle:
        bundle.addfile(tarfile.TarInfo("package/bin/ara"), io.BytesIO(b""))
    archive = tmp_path / "cli.tgz"
    archive.write_bytes(data.getvalue())
    monkeypatch.setenv("REASON_API_KEY", "parent-key")
    server = SyncYieldServer(profile, repo)
    client = httpx.AsyncClient
    monkeypatch.setattr(driver_module.httpx, "AsyncClient",
                        lambda *a, **k: client(*a, transport=httpx.MockTransport(server), **k))

    async def no_sleep(delay):
        pass
    monkeypatch.setattr(driver_module.asyncio, "sleep", no_sleep)
    agent = ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna", runtime_profile=profile,
                        cli_archive_path=str(archive), cli_archive_sha256=hashlib.sha256(archive.read_bytes()).hexdigest())
    context = SimpleNamespace(metadata=None, n_input_tokens=None, n_output_tokens=None, cost_usd=None)
    if fail:
        async def broken(*args, **kwargs):
            (repo / "a.py").write_text("print(3)\n")
            raise RuntimeError("session failed")
        monkeypatch.setattr(agent.driver, "run", broken)

    async def trial():
        await agent.driver.setup(RepoGuest(repo))
        await agent.run("Explain the repository.", RepoGuest(repo), context)

    asyncio.run(trial())
    return repo, server, tmp_path / "logs"


@pytest.mark.parametrize("profile", [REASON_V7_SYNC_YIELD_RUNTIME_PROFILE,
                                     REASON_V7_SYNC_YIELD_MACHINE_SDK_RUNTIME_PROFILE])
def test_sync_yield_trial_sends_workdir_and_snapshots_the_agent_phase(tmp_path, monkeypatch, profile):
    repo, server, logs = run_agent(tmp_path, monkeypatch, profile)

    assert server.create_body["target"]["workspace_path"] == str(repo)
    start = json.loads((logs / "git-snapshot-start.json").read_text())
    end = json.loads((logs / "git-snapshot-end.json").read_text())
    assert (logs / "git-snapshot-start.txt").read_text().startswith("banner\n")
    assert start["complete"] and end["complete"] and start["git_available"]
    assert start["toplevel"] == end["toplevel"] and start["head"] == end["head"]
    assert start["status"] == [] and start["tracked_changed"] == [] and start["diff_stat"] == []
    assert end["status"] == [" M a.py", "?? notes.txt"]
    assert end["tracked_changed"] == ["a.py"]
    assert end["diff_stat"][0].startswith(" a.py | 2")
    assert start["tracked_diff_sha256"] != end["tracked_diff_sha256"]
    # Read-only: the index was not refreshed or staged.
    assert subprocess.run(["git", "-C", str(repo), "diff", "--cached", "--name-only"],
                          capture_output=True, text=True).stdout == ""


def test_end_snapshot_is_taken_when_the_agent_phase_fails(tmp_path, monkeypatch):
    with pytest.raises(RuntimeError, match="session failed"):
        run_agent(tmp_path, monkeypatch, REASON_V7_SYNC_YIELD_RUNTIME_PROFILE, fail=True)
    end = json.loads((tmp_path / "logs" / "git-snapshot-end.json").read_text())
    assert end["tracked_changed"] == ["a.py"]
