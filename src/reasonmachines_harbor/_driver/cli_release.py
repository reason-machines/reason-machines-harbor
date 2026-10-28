"""Official Reason CLI release selection, download, and verification.

The stable manifest picks the newest published Linux x64 CLI; the packaged
release below is the qualified fallback when the manifest is unavailable.
Release URLs can carry download signatures, so their HTTP logs stay private.
"""
from __future__ import annotations

import hashlib
import json
import logging
import re
import tarfile
from contextvars import ContextVar
from functools import wraps
from pathlib import Path, PurePosixPath
from urllib.parse import urljoin, urlparse

import httpx

from .config import DEFAULT_RUNTIME_CONFIG

# Verified official Linux x64 CLI release, installed inside the task container.
DEFAULT_CLI_MANIFEST_URL = "https://reasonmachines.com/api/cli-manifest"
DEFAULT_CLI_ARCHIVE_URL = (
    "https://reasonmachines.com/cli/releases/download/"
    "cli-v0.2.61/reason-machines-cli-linux-x64-0.2.61.tgz"
)
DEFAULT_CLI_ARCHIVE_SHA256 = "cdc0828abde3157e39afc6a64e033deee52ff7cfb09b55a42e60394df24980b9"
# A dated benchmark release runs the worker CLI it was frozen with, not the
# newest stable one: the Hands worker inside it carries the tool code
# (output truncation, edit application) the release's tests pin.
# Never change an entry; a new dated release adds its own.
RELEASE_CLI_ARCHIVES = {
    "runtime/reason-v7-2026-10": {
        "version": "0.2.151",
        "archive_sha256": "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955",
        "musl_archive_sha256": "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8",
    },
    "runtime/reason-v7-2026-11": {
        "version": "0.2.185",
        "archive_sha256": "52dcb28b61aa497ee0aab6a5558a8ab08233ca023316b2628f206257c97d8eb5",
        "musl_archive_sha256": "91698c6706a3f451b472f92f629adc107233495097401e2196694cceebc8b51e",
    },
    # The worker every runtime/reason-v7-bash-reason-v1 benchmark ran.
    "runtime/reason-v8-2026-10": {
        "version": "0.2.151",
        "archive_sha256": "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955",
        "musl_archive_sha256": "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8",
    },
    # v8's worker: runtime/reason-v8.1-2026-10 is v8 with Bash yielding at its timeout.
    "runtime/reason-v8.1-2026-10": {
        "version": "0.2.151",
        "archive_sha256": "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955",
        "musl_archive_sha256": "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8",
    },
    # v8.1's worker (v8's): runtime/reason-v8.2-2026-10 is v8.1 without the 25-turn guideline.
    "runtime/reason-v8.2-2026-10": {
        "version": "0.2.151",
        "archive_sha256": "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955",
        "musl_archive_sha256": "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8",
    },
    # v8.2's worker: runtime/reason-agent-1.2 is v8.2 under a standalone release name.
    "runtime/reason-agent-1.2": {
        "version": "0.2.151",
        "archive_sha256": "2c3ea45c1e28c9e65046d36249ed0f297c7fc1edff30c9fc3a6de0a9244c7955",
        "musl_archive_sha256": "a1de656866b444f4eca1a9d0e8c8a251c845e1f0dd7ef9b514c03d730e8f50c8",
    },
    # The worker that runs each Bash command off its shell's argv, so pkill -f with a
    # pattern from the command spares that shell.
    "runtime/reason-agent-1.3": {
        "version": "0.2.625",
        "archive_sha256": "6d9dcade664b25e87ab21a5d19cead022b5484772ca2bf6468a3640ad3325c75",
        "musl_archive_sha256": "509c399541555777a6465c899e3ecf1517831ee40fcc65ece654e3ab50082c6c",
    },
}
# A dated benchmark release that ships agent tools (rg, file, xxd, strings, unzip,
# apply_patch: packages/reason-machines-driver/agent-tools) installs this archive
# last on the worker's PATH when the trial names no agent_tools_archive_path. The
# SHA-256 pins it wherever the URL redirects. Never change an entry; a new dated
# release adds its own. A release without one (v7, v8) installs none.
RELEASE_AGENT_TOOLS = {
    "runtime/reason-v8.1-2026-10": {
        "url": "https://github.com/reason-machines/benchmark/releases/download/agent-tools-2026-10-22/agent-tools-linux-x64.tgz",
        "sha256": "31336a2c0c4adef5d2e8c1a0c12761742377732c6abfa47f978815789fbf8de6",
    },
    # v8.1's tools plus ps (BusyBox's applet).
    "runtime/reason-v8.2-2026-10": {
        "url": "https://github.com/reason-machines/benchmark/releases/download/agent-tools-2026-10-23/agent-tools-linux-x64.tgz",
        "sha256": "059652fc47297dfab47833917f11d02858c8e34fb9bfd45bcd66d381d695cab5",
    },
    # v8.2's archive: runtime/reason-agent-1.2 is v8.2 under a standalone release name.
    "runtime/reason-agent-1.2": {
        "url": "https://github.com/reason-machines/benchmark/releases/download/agent-tools-2026-10-23/agent-tools-linux-x64.tgz",
        "sha256": "059652fc47297dfab47833917f11d02858c8e34fb9bfd45bcd66d381d695cab5",
    },
    # 1.2's tools plus bc, pkill, pgrep, free and which (BusyBox applets) and column (util-linux).
    "runtime/reason-agent-1.3": {
        "url": "https://github.com/reason-machines/benchmark/releases/download/agent-tools-2026-10-27/agent-tools-linux-x64.tgz",
        "sha256": "00252f67c979c7ab175e77cde6ca8e974c23aebe4da62ac747bd73050b2ede72",
    },
}
CLI_MANIFEST_MAX_BYTES = 64 * 1024
CLI_MANIFEST_HOSTS = frozenset({"reasonmachines.com", "ara-desktop-releases.t3.storage.dev"})


class CliManifestError(ValueError):
    """Bounded release failure code, never a response body or signed URL."""


_release_request = ContextVar("reason_release_request", default=False)


class _ReleaseRequestFilter(logging.Filter):
    def filter(self, record):
        return not _release_request.get()


# HTTPX logs complete request URLs at INFO; HTTPCore debug traces can also log
# redirect headers. The context is task-local: unrelated concurrent API traffic
# keeps its diagnostics, and cancellation restores the caller's context.
_filter = _ReleaseRequestFilter()
for _name in ("httpx", "httpcore.connection", "httpcore.http11", "httpcore.http2",
              "httpcore.proxy", "httpcore.socks"):
    logging.getLogger(_name).addFilter(_filter)


def private_release_request_logs(function):
    @wraps(function)
    async def guarded(*args, **kwargs):
        token = _release_request.set(True)
        try:
            return await function(*args, **kwargs)
        finally:
            _release_request.reset(token)
    return guarded


@private_release_request_logs
async def fetch_archive(downloader: httpx.AsyncClient, url: str) -> bytes:
    # Release assets redirect to HTTPS object storage. This client carries
    # no application credentials; validate every hop rather than delegating
    # redirect handling (which could permit an HTTPS -> HTTP downgrade).
    for hop in range(DEFAULT_RUNTIME_CONFIG.archive_redirect_max_hops):
        parsed = urlparse(url)
        if parsed.scheme != "https" or not parsed.netloc or parsed.username or parsed.password:
            raise ValueError("CLI archive redirects must remain anonymous HTTPS")
        response = await downloader.get(url, follow_redirects=False)
        if response.status_code in {301, 302, 303, 307, 308}:
            location = response.headers.get("location")
            if not location or hop == DEFAULT_RUNTIME_CONFIG.archive_redirect_max_hops - 1:
                raise RuntimeError("CLI archive redirect limit exceeded or missing location")
            url = urljoin(url, location)
            continue
        if not response.is_success:
            raise RuntimeError(f"CLI archive download failed: HTTP {response.status_code}")
        return response.content
    raise RuntimeError("CLI archive redirect limit exceeded")


@private_release_request_logs
async def fetch_manifest(downloader: httpx.AsyncClient, url: str):
    current_url = url
    for _ in range(DEFAULT_RUNTIME_CONFIG.archive_redirect_max_hops):
        try:
            parsed = urlparse(current_url)
            safe = (parsed.scheme == "https" and parsed.hostname in CLI_MANIFEST_HOSTS
                    and parsed.port in {None, 443} and not parsed.username and not parsed.password)
        except ValueError:
            safe = False
        if not safe:
            raise CliManifestError("manifest_redirect_unsafe")
        async with downloader.stream("GET", current_url, headers={"Accept-Encoding": "identity"}) as response:
            if response.status_code in {301, 302, 303, 307, 308}:
                location = response.headers.get("location")
                if not location:
                    raise CliManifestError("manifest_redirect_missing_location")
                try:
                    current_url = urljoin(current_url, location)
                except ValueError:
                    raise CliManifestError("manifest_redirect_unsafe") from None
                continue
            if not response.is_success:
                raise CliManifestError(f"manifest_http_{response.status_code}")
            if response.headers.get("content-encoding", "identity").strip().lower() != "identity":
                raise CliManifestError("manifest_content_encoding")
            declared_length = response.headers.get("content-length")
            if declared_length is not None:
                try:
                    length = int(declared_length)
                except ValueError:
                    raise CliManifestError("manifest_invalid_length") from None
                if length < 0:
                    raise CliManifestError("manifest_invalid_length")
                if length > CLI_MANIFEST_MAX_BYTES:
                    raise CliManifestError("manifest_too_large")
            content = bytearray()
            async for chunk in response.aiter_bytes(chunk_size=CLI_MANIFEST_MAX_BYTES + 1):
                content.extend(chunk)
                if len(content) > CLI_MANIFEST_MAX_BYTES:
                    raise CliManifestError("manifest_too_large")
            try:
                return json.loads(content.decode("utf-8"))
            except (UnicodeDecodeError, ValueError):
                raise CliManifestError("manifest_invalid_json") from None
    raise CliManifestError("manifest_redirect_limit")


async def resolve_stable_archive(downloader: httpx.AsyncClient) -> dict:
    """Return the cli-resolution receipt for the stable release, or the packaged fallback."""
    requested_version = None
    fallback_reason = None
    # Each setup starts from the known qualified outage floor, not a mutable
    # process-wide cache. Once setup ends, the admitted binary stays fixed.
    archive_url = DEFAULT_CLI_ARCHIVE_URL
    archive_sha = DEFAULT_CLI_ARCHIVE_SHA256
    # Alpine task images need the musl build. The packaged fallback has none, and
    # a release without a valid one leaves musl guests unsupported.
    musl_url = musl_sha = None
    try:
        latest = await fetch_manifest(downloader, DEFAULT_CLI_MANIFEST_URL)
        if not isinstance(latest, dict):
            raise CliManifestError("manifest_invalid_identity")
        version = latest.get("version")
        tag = latest.get("tag")
        if not isinstance(version, str) or not re.fullmatch(r"[0-9]+\.[0-9]+\.[0-9]+", version):
            raise CliManifestError("manifest_invalid_identity")
        if tag != f"cli-v{version}":
            raise CliManifestError("manifest_invalid_identity")
        requested_version = version
        base_url = f"https://reasonmachines.com/cli/releases/download/{tag}"
        published = await fetch_manifest(downloader, f"{base_url}/manifest.json")
        if not isinstance(published, dict):
            raise CliManifestError("manifest_invalid_identity")
        if published.get("version") != version or published.get("tag") != tag:
            raise CliManifestError("manifest_identity_mismatch")
        if published.get("downloadBaseUrl") != base_url:
            raise CliManifestError("manifest_invalid_origin")
        targets = published.get("targets")
        if not isinstance(targets, list):
            raise CliManifestError("manifest_invalid_targets")
        target = next((candidate for candidate in targets
                       if isinstance(candidate, dict) and candidate.get("id") == "linux-x64"), None)
        if target is None:
            raise CliManifestError("manifest_missing_linux_x64")
        archive = target.get("archive")
        sha256 = target.get("sha256")
        if archive != f"reason-machines-cli-linux-x64-{version}.tgz":
            raise CliManifestError("manifest_invalid_archive")
        if not isinstance(sha256, str) or not re.fullmatch(r"[a-f0-9]{64}", sha256):
            raise CliManifestError("manifest_invalid_checksum")
        musl = next((candidate for candidate in targets
                     if isinstance(candidate, dict) and candidate.get("id") == "linux-x64-musl"), None)
        if (musl is not None and musl.get("archive") == f"reason-machines-cli-linux-x64-musl-{version}.tgz"
                and isinstance(musl.get("sha256"), str) and re.fullmatch(r"[a-f0-9]{64}", musl["sha256"])):
            musl_url, musl_sha = f"{base_url}/{musl['archive']}", musl["sha256"]
        archive_url = f"{base_url}/{archive}"
        archive_sha = sha256
    except CliManifestError as error:
        fallback_reason = str(error)
    except httpx.TimeoutException:
        fallback_reason = "manifest_timeout"
    except httpx.HTTPError:
        fallback_reason = "manifest_transport_error"
    return {
        "schema_version": 1,
        "source": "packaged_fallback" if fallback_reason else "stable_manifest",
        "requested_version": requested_version,
        "selected_version": archive_url.rsplit("cli-v", 1)[1].split("/", 1)[0],
        "archive_url": archive_url, "archive_sha256": archive_sha,
        "musl_archive_url": None if fallback_reason else musl_url,
        "musl_archive_sha256": None if fallback_reason else musl_sha,
        "fallback_reason": fallback_reason,
    }


def release_archive(runtime_profile: str | None) -> dict | None:
    """The cli-resolution receipt for a dated release's pinned CLI, or None for any other profile."""
    pin = RELEASE_CLI_ARCHIVES.get(runtime_profile or "")
    if pin is None:
        return None
    version = pin["version"]
    base_url = f"https://reasonmachines.com/cli/releases/download/cli-v{version}"
    return {
        "schema_version": 1,
        "source": "release_pin",
        "requested_version": version,
        "selected_version": version,
        "archive_url": f"{base_url}/reason-machines-cli-linux-x64-{version}.tgz",
        "archive_sha256": pin["archive_sha256"],
        "musl_archive_url": f"{base_url}/reason-machines-cli-linux-x64-musl-{version}.tgz",
        "musl_archive_sha256": pin["musl_archive_sha256"],
        "fallback_reason": None,
    }


def verify_archive(archive: Path, sha256: str) -> None:
    """Admit only the pinned archive, with every member inside `package/`."""
    if hashlib.sha256(archive.read_bytes()).hexdigest() != sha256:
        raise ValueError("CLI archive checksum mismatch")
    with tarfile.open(archive) as bundle:
        for member in bundle.getmembers():
            path = PurePosixPath(member.name)
            if path.is_absolute() or ".." in path.parts or path.parts[0] != "package":
                raise ValueError("unsafe CLI archive member")
            if member.isdev() or member.isfifo():
                raise ValueError("unsupported CLI archive member")
            if member.issym() or member.islnk():
                # Runtime links must remain inside the extracted package.
                target = PurePosixPath(member.linkname)
                if target.is_absolute() or ".." in target.parts:
                    raise ValueError("unsafe CLI archive link")
