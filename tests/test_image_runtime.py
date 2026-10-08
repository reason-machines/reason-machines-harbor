"""The worker starts with the task image's ENV and WORKDIR on E2B-like guests."""
import asyncio
import hashlib
import io
import logging
import shlex
import subprocess
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from reasonmachines_harbor import ReasonAgent, image_runtime
from reasonmachines_harbor._driver import driver as driver_module
from reasonmachines_harbor._driver.driver import (C2_TOOLS_V7_RUNTIME_PROFILE,
                                                  REASON_V7_SYNC_YIELD_MACHINE_SDK_RUNTIME_PROFILE,
                                                  REASON_V7_SYNC_YIELD_RUNTIME_PROFILE)

GO_PATH = "/usr/local/go/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
IMAGE_CONFIG = {"Env": [f"PATH={GO_PATH}", "GOPATH=/go", "GOMODCACHE=/go/pkg/mod", "TASK_VALUE=image"],
                "WorkingDir": "/app"}


class WorkerStarted(Exception):
    pass


class FakeE2BEnvironment:
    """Exec shells that, like E2B's, start in /root without the image config."""

    def __init__(self, tmp_path, docker_image="ghcr.io/scaleapi/swe-atlas:task"):
        self.task_env_config = SimpleNamespace(docker_image=docker_image, workdir=None)
        self.environment_dir = tmp_path
        self.commands = []

    def _startup_env(self):
        return {"TASK_VALUE": "task"}

    async def exec(self, command, timeout_sec=None, **_):
        command = command.split("__reason_guest_output__; ", 1)[-1]
        self.commands.append(command)
        if "worker status" in command:
            raise WorkerStarted
        stdout = {"pwd -P": "/root\n", "cd /app && pwd -P": "/app\n"}.get(command, "")
        if command.endswith("--version"):
            stdout = "1.0.0\n"
        return SimpleNamespace(return_code=0, stdout=stdout, stderr="")

    async def upload_file(self, source_path, target_path):
        pass


def worker_archive(tmp_path):
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as archive:
        archive.addfile(tarfile.TarInfo("package/bin/ara"), io.BytesIO(b""))
    path = tmp_path / "worker.tgz"
    path.write_bytes(data.getvalue())
    return str(path), hashlib.sha256(data.getvalue()).hexdigest()


def agent(tmp_path, monkeypatch):
    monkeypatch.setenv("REASON_API_KEY", "test-key")
    path, sha = worker_archive(tmp_path)
    return ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-5.6-luna",
                       cli_archive_path=path, cli_archive_sha256=sha,
                       runtime_profile=C2_TOOLS_V7_RUNTIME_PROFILE)


async def start_worker(driver, environment):
    async def api(method, suffix, body=None):
        return {"device_kind": "headless", "lifecycle": "ephemeral", "enrollment_ticket": "ticket"}
    driver._api = api
    driver.org = "org"
    with pytest.raises(WorkerStarted):
        await driver._enroll_and_wait_for_worker(environment)
    return next(command for command in environment.commands if "worker start" in command)


def test_worker_starts_in_image_workdir_with_image_env(tmp_path, monkeypatch):
    monkeypatch.setattr(image_runtime, "registry_config", lambda image: IMAGE_CONFIG)
    reason = agent(tmp_path, monkeypatch)
    environment = FakeE2BEnvironment(tmp_path)
    asyncio.run(reason.setup(environment))
    driver = reason.driver

    assert driver.workspace == "/app"
    assert driver._session_request("task", "root")["target"]["workspace_path"] == "/app"
    for profile in (REASON_V7_SYNC_YIELD_RUNTIME_PROFILE, REASON_V7_SYNC_YIELD_MACHINE_SDK_RUNTIME_PROFILE):
        driver.runtime_profile = profile
        assert driver._session_request("task", "root")["target"]["workspace_path"] == "/app"
    driver.runtime_profile = None
    assert "workspace_path" not in driver._session_request("task", "root")["target"]
    start = shlex.split(asyncio.run(start_worker(driver, environment)))
    assert start[:5] == ["cd", "/app", "&&", "{", "nohup"]
    assert start[5] == "env"
    # Only the redirected worker is backgrounded, so the exec returns at once;
    # its pid is kept so cleanup can wait for it to exit.
    assert start[-5:] == ["&", "echo", "$!", f">{driver.remote}/worker.pid;", "}"]
    assert f"PATH={GO_PATH}" in start
    assert "GOPATH=/go" in start and "GOMODCACHE=/go/pkg/mod" in start
    # The environment's own task/trial values stay above the image's.
    assert not any(word.startswith("TASK_VALUE=") for word in start)


def test_dockerfile_task_uses_base_image_config_and_own_env(tmp_path, monkeypatch):
    seen = []
    monkeypatch.setattr(image_runtime, "registry_config",
                        lambda image: seen.append(image) or {"Env": ["PATH=/usr/bin:/bin"], "WorkingDir": "/"})
    (tmp_path / "Dockerfile").write_text(
        "FROM golang:1.22\nENV GOPATH=/go \\\n    PATH=/usr/local/go/bin:$PATH\nWORKDIR /app\n")
    environment = FakeE2BEnvironment(tmp_path, docker_image=None)
    environment._workdir = "/app"  # Harbor's E2B environment parses WORKDIR itself.
    env, workdir = asyncio.run(image_runtime.resolve_image_runtime(environment, logging.getLogger()))
    assert seen == ["golang:1.22"]
    assert env == {"PATH": "/usr/local/go/bin:/usr/bin:/bin", "GOPATH": "/go"}
    assert workdir == "/app"


def test_unavailable_image_config_fails_the_trial_before_the_worker_starts(tmp_path, monkeypatch):
    # Running without the image's PATH and toolchain would grade the model on a broken
    # task; the error is an infrastructure failure that Harbor retries.
    def unavailable(image):
        raise OSError("registry unreachable")
    monkeypatch.setattr(image_runtime, "registry_config", unavailable)
    reason = agent(tmp_path, monkeypatch)
    environment = FakeE2BEnvironment(tmp_path)
    with pytest.raises(image_runtime.ImageConfigUnavailableError, match="registry unreachable"):
        asyncio.run(reason.setup(environment))
    assert not any("worker" in command for command in environment.commands)


def test_e2b_sandbox_gets_the_image_env_below_task_values(tmp_path, monkeypatch):
    from harbor.environments.e2b import E2BEnvironment
    from reasonmachines_harbor.environment import ReasonE2BEnvironment

    monkeypatch.setattr(image_runtime, "registry_config", lambda image: IMAGE_CONFIG)
    monkeypatch.setattr(E2BEnvironment, "_startup_env", lambda self: {"TASK_VALUE": "task"})
    started, written = [], []

    async def start(self, force_build):
        started.append(self._startup_env())

        async def write(path, data, user=None):
            written.append(path)
        self._sandbox = SimpleNamespace(files=SimpleNamespace(write=write))
    monkeypatch.setattr(E2BEnvironment, "start", start)
    environment = ReasonE2BEnvironment.__new__(ReasonE2BEnvironment)
    environment.task_env_config = SimpleNamespace(docker_image="ghcr.io/scaleapi/swe-atlas:task", workdir=None)
    environment.environment_dir = tmp_path
    asyncio.run(environment.start(force_build=False))
    assert started == [{"PATH": GO_PATH, "GOPATH": "/go", "GOMODCACHE": "/go/pkg/mod", "TASK_VALUE": "task"}]
    assert written == ["/etc/profile.d/00-reason-image-path.sh"]  # the image sets PATH (0.3.16)

    monkeypatch.setattr(image_runtime, "registry_config", lambda image: (_ for _ in ()).throw(OSError("429")))
    environment = ReasonE2BEnvironment.__new__(ReasonE2BEnvironment)
    environment.task_env_config = SimpleNamespace(docker_image="ghcr.io/scaleapi/swe-atlas:task", workdir=None)
    environment.environment_dir = tmp_path
    with pytest.raises(image_runtime.ImageConfigUnavailableError):
        asyncio.run(environment.start(force_build=False))
    assert len(started) == 1


def test_worker_wrapper_drops_environment_the_worker_refuses(tmp_path, monkeypatch):
    # oven/bun sets BUN_RUNTIME_TRANSPILER_CACHE_PATH; the worker exits 78 on it.
    monkeypatch.setattr(image_runtime, "registry_config", lambda image: IMAGE_CONFIG)
    reason = agent(tmp_path, monkeypatch)
    environment = FakeE2BEnvironment(tmp_path)
    asyncio.run(reason.setup(environment))
    remote = reason.driver.remote
    write = next(command for command in environment.commands if "ara-clean" in command and "printf" in command)
    guest = tmp_path / "guest"
    (guest / "package/bin").mkdir(parents=True)
    (guest / "package/bin/ara").write_text("#!/bin/sh\nenv\n")
    (guest / "package/bin/ara").chmod(0o755)
    subprocess.run(["/bin/sh", "-c", write.replace(remote, str(guest))], check=True)
    image = {"PATH": "/usr/local/bin:/usr/bin:/bin", "BUN_RUNTIME_TRANSPILER_CACHE_PATH": "0",
             "BUN_INSTALL_BIN": "/usr/local/bin", "BUN_INSTALL": "/root/.bun", "NODE_DEBUG": "net",
             "DYLD_INSERT_LIBRARIES": "/x", "HTTPS_PROXY": "http://proxy", "GOPATH": "/go",
             "MULTILINE": "first\nBUN_NOT_A_VARIABLE x=1\nlast"}
    seen = subprocess.run([str(guest / "ara-clean")], env=image, check=True,
                          capture_output=True, text=True).stdout
    names = {line.split("=", 1)[0] for line in seen.splitlines() if "=" in line}
    assert {"PATH", "GOPATH", "BUN_INSTALL", "MULTILINE"} <= names
    assert not names & {"BUN_RUNTIME_TRANSPILER_CACHE_PATH", "BUN_INSTALL_BIN", "NODE_DEBUG",
                        "DYLD_INSERT_LIBRARIES", "HTTPS_PROXY"}


PARENT_ORG = "11111111-1111-4111-8111-111111111111"
CHILD_ORG = "22222222-2222-4222-8222-222222222222"


def ephemeral_create_body(driver, monkeypatch):
    sent = {}
    async def organization(request_api, expected):
        return PARENT_ORG
    async def ephemeral_api(method, suffix, body, stage):
        sent.update(body)
        return {"organization_id": CHILD_ORG, "parent_organization_id": PARENT_ORG,
                "reference": body["reference"], "api_key": "child-key"}
    monkeypatch.setattr(driver_module, "resolve_organization", organization)
    driver._ephemeral_api = ephemeral_api
    asyncio.run(driver._create_ephemeral_workspace())
    return sent


@pytest.mark.parametrize("task_toml, deadline_min, ttl_min, rcus", [
    # Terminal-Bench: 8 h for the agent.
    ("[agent]\ntimeout_sec = 28800.0\n", 490, 510, "82"),
    # DeepSWE: 1.5 h; the workspace keeps its 240-minute floor and the default budget.
    ("[agent]\ntimeout_sec = 5400\n", 100, 240, "30"),
    # Beyond the server's 12 h credential ceiling, the deadline stays just under it.
    ("[agent]\ntimeout_sec = 86400\n", 719, 720, "120"),
    # No limit in task.toml: 170 minutes, as before, and the server's default budget.
    ("[verifier]\ntimeout_sec = 600\n", 170, 240, None),
    (None, 170, 240, None),
])
def test_session_deadline_follows_the_task_agent_time_limit(tmp_path, monkeypatch, task_toml, deadline_min, ttl_min, rcus):
    monkeypatch.setattr(image_runtime, "registry_config", lambda image: IMAGE_CONFIG)
    reason = agent(tmp_path, monkeypatch)
    environment = FakeE2BEnvironment(tmp_path)
    environment.environment_dir = tmp_path / "environment"
    if task_toml is not None:
        (tmp_path / "task.toml").write_text(task_toml)
    asyncio.run(reason.setup(environment))
    driver = reason.driver

    request = driver._session_request("task", "root")
    assert request["execution_budget"] == {"wall_clock_max_ms": deadline_min * 60_000}
    body = ephemeral_create_body(driver, monkeypatch)
    assert body["ttl_minutes"] == ttl_min
    assert body.get("rcus") == rcus
