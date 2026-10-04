"""The worker starts with the task image's ENV and WORKDIR on E2B-like guests."""
import asyncio
import hashlib
import io
import logging
import shlex
import tarfile
from pathlib import Path
from types import SimpleNamespace

import pytest

from reasonmachines_harbor import ReasonAgent, image_runtime
from reasonmachines_harbor._driver.driver import PI_C2_TOOLS_V7_RUNTIME_PROFILE

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
                       runtime_profile=PI_C2_TOOLS_V7_RUNTIME_PROFILE)


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
    start = shlex.split(asyncio.run(start_worker(driver, environment)))
    assert start[:5] == ["cd", "/app", "&&", "{", "nohup"]
    assert start[5] == "env"
    # Only the redirected worker is backgrounded, so the exec returns at once.
    assert start[-2:] == ["&", "}"]
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


def test_unavailable_image_config_keeps_exec_shell_and_logs(tmp_path, monkeypatch, caplog):
    def unavailable(image):
        raise OSError("registry unreachable")
    monkeypatch.setattr(image_runtime, "registry_config", unavailable)
    reason = agent(tmp_path, monkeypatch)
    environment = FakeE2BEnvironment(tmp_path)
    with caplog.at_level(logging.WARNING):
        asyncio.run(reason.setup(environment))
    assert "Task image config unavailable" in caplog.text
    assert reason.driver.workspace == "/root"
    start = asyncio.run(start_worker(reason.driver, environment))
    assert start.startswith("cd /root && { nohup env REASON_RELAY_CUSTOM_DOMAIN=1 /tmp/")
