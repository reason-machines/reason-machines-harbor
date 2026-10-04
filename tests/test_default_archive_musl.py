"""The automatic CLI archive serves glibc and musl (Alpine) guests from one stable release."""
import asyncio
import hashlib
import io
import tarfile
from types import SimpleNamespace

import httpx

from reasonmachines_harbor import ReasonAgent
from reasonmachines_harbor._driver import driver as driver_module
from reasonmachines_harbor._driver.cli_release import resolve_stable_archive

BASE = "https://reasonmachines.com/cli/releases/download/cli-v1.2.3"


def release(targets):
    def handler(request):
        if request.url.path == "/api/cli-manifest":
            return httpx.Response(200, json={"version": "1.2.3", "tag": "cli-v1.2.3"})
        return httpx.Response(200, json={"version": "1.2.3", "tag": "cli-v1.2.3",
                                         "downloadBaseUrl": BASE, "targets": targets})
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


async def resolve(targets):
    async with release(targets) as client:
        return await resolve_stable_archive(client)


GLIBC = {"id": "linux-x64", "archive": "reason-machines-cli-linux-x64-1.2.3.tgz", "sha256": "a" * 64}
MUSL = {"id": "linux-x64-musl", "archive": "reason-machines-cli-linux-x64-musl-1.2.3.tgz", "sha256": "b" * 64}


def test_stable_release_resolves_the_musl_build_beside_glibc():
    resolution = asyncio.run(resolve([GLIBC, MUSL]))
    assert resolution["source"] == "stable_manifest"
    assert resolution["archive_url"] == f"{BASE}/reason-machines-cli-linux-x64-1.2.3.tgz"
    assert resolution["musl_archive_url"] == f"{BASE}/reason-machines-cli-linux-x64-musl-1.2.3.tgz"
    assert resolution["musl_archive_sha256"] == "b" * 64


def test_missing_or_invalid_musl_build_leaves_glibc_resolution_intact():
    for targets in ([GLIBC], [GLIBC, {**MUSL, "archive": "other.tgz"}], [GLIBC, {**MUSL, "sha256": "x"}]):
        resolution = asyncio.run(resolve(targets))
        assert resolution["source"] == "stable_manifest"
        assert resolution["musl_archive_url"] is None and resolution["musl_archive_sha256"] is None


def archive_bytes():
    data = io.BytesIO()
    with tarfile.open(fileobj=data, mode="w:gz") as bundle:
        bundle.addfile(tarfile.TarInfo("package/bin/ara"), io.BytesIO(b""))
    return data.getvalue()


class Guest:
    def __init__(self, libc):
        self.libc = libc

    async def exec(self, command, timeout_sec=None, **_):
        if command.startswith("uname -s"):
            stdout = "Linux\nx86_64\n" + ("musl\n" if self.libc == "musl" else "glibc 2.36\n")
        elif "ld-musl-x86_64.so.1 ]; then printf musl" in command:
            stdout = "musl" if self.libc == "musl" else ""
        elif command == "pwd -P":
            stdout = "/root\n"
        elif command.endswith("--version"):
            stdout = "1.2.3\n"
        else:
            stdout = ""
        return SimpleNamespace(return_code=0, stdout=stdout, stderr="")

    async def upload_file(self, source_path, target_path):
        pass


def install(tmp_path, monkeypatch, libc):
    monkeypatch.setenv("REASON_API_KEY", "test-key")
    payload = archive_bytes()
    sha = hashlib.sha256(payload).hexdigest()
    fetched = []

    async def stable(_client):
        return {"schema_version": 1, "source": "stable_manifest", "requested_version": "1.2.3",
                "selected_version": "1.2.3", "fallback_reason": None,
                "archive_url": f"{BASE}/glibc.tgz", "archive_sha256": sha,
                "musl_archive_url": f"{BASE}/musl.tgz", "musl_archive_sha256": sha}

    async def fetch(_client, url):
        fetched.append(url)
        return payload

    monkeypatch.setattr(driver_module, "resolve_stable_archive", stable)
    monkeypatch.setattr(driver_module, "fetch_archive", fetch)
    reason = ReasonAgent(logs_dir=tmp_path / "logs", model_name="openai/gpt-6-luna")
    assert reason.driver.uses_default_archive
    asyncio.run(reason.driver.setup(Guest(libc)))
    return reason.driver, fetched


def test_alpine_guest_installs_the_stable_musl_build(tmp_path, monkeypatch):
    driver, fetched = install(tmp_path, monkeypatch, "musl")
    assert fetched == [f"{BASE}/musl.tgz"]
    assert driver.cli_version == "1.2.3"


def test_glibc_guest_still_installs_the_glibc_build(tmp_path, monkeypatch):
    driver, fetched = install(tmp_path, monkeypatch, "glibc")
    assert fetched == [f"{BASE}/glibc.tgz"]
