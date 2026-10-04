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
        "fallback_reason": fallback_reason,
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
