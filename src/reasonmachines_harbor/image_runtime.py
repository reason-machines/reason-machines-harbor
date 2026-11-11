"""The task image's runtime config (ENV and WORKDIR) for guests that drop it.

E2B builds a template from the image filesystem but its exec shells get
neither the image config's ENV nor its WORKDIR: PATH loses entries such as
/usr/local/go/bin and the shell starts in /root. This reads the config from the
image registry (anonymously, linux/amd64). A Dockerfile task uses its FROM image's config plus its own
ENV and WORKDIR lines. Docker applies the config natively and is skipped.

Registries rate-limit anonymous reads (Docker Hub answers 429 under benchmark
concurrency), so each image's config is cached on disk for every process of a
run, a 429 or 5xx is retried with backoff, and DOCKERHUB_USERNAME with
DOCKERHUB_TOKEN authenticates Docker Hub reads. A config that still cannot be
read raises: running a task without its image ENV silently drops its toolchain.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
from functools import cache
from pathlib import Path

from harbor.environments.docker.docker import DockerEnvironment

ACCEPT = ", ".join([
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
])


def split_reference(image: str) -> tuple[str, str, str]:
    """Registry host, repository and tag-or-digest of an image reference."""
    name, reference, digest = image, "latest", None
    if "@" in name:
        name, digest = name.split("@", 1)
    if ":" in name.rsplit("/", 1)[-1]:
        name, reference = name.rsplit(":", 1)
    first = name.split("/", 1)[0]
    if "/" in name and ("." in first or ":" in first or first == "localhost"):
        host, repository = name.split("/", 1)
    else:
        host, repository = "registry-1.docker.io", name if "/" in name else "library/" + name
    return host, repository, digest or reference


CACHE_DIR = Path(os.environ.get("REASON_HARBOR_IMAGE_CONFIG_CACHE")
                 or Path.home() / ".cache" / "reason-machines-harbor" / "image-config")
CACHE_TTL_SEC = 24 * 3600
RETRY_DELAYS_SEC = (2, 5, 10, 20, 40, 60)


def _get(url: str, token: str | None, accept: str | None = None, basic: str | None = None) -> bytes:
    headers = {"Accept": accept} if accept else {}
    authorization = "Bearer " + token if token else "Basic " + basic if basic else None
    for delay in (*RETRY_DELAYS_SEC, None):
        request = urllib.request.Request(url, headers=headers)
        # Not carried to a redirect: from AWS, Docker Hub sends blob reads to a presigned S3
        # URL, which refuses a second credential (400 "Only one auth mechanism allowed").
        if authorization:
            request.add_unredirected_header("Authorization", authorization)
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                return response.read()
        except urllib.error.HTTPError as error:
            if delay is None or (error.code != 429 and error.code < 500):
                raise
            retry_after = error.headers.get("Retry-After", "")
            time.sleep(min(int(retry_after), 120) if retry_after.isdigit() else delay)
    raise AssertionError("unreachable")


def _dockerhub_basic(host: str) -> str | None:
    user, token = os.environ.get("DOCKERHUB_USERNAME"), os.environ.get("DOCKERHUB_TOKEN")
    if host != "registry-1.docker.io" or not user or not token:
        return None
    return base64.b64encode(f"{user}:{token}".encode()).decode()


def _token(host: str, repository: str) -> str | None:
    try:
        _get(f"https://{host}/v2/", None)
        return None
    except urllib.error.HTTPError as error:
        challenge = error.headers.get("WWW-Authenticate", "")
    fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    if "realm" not in fields:
        return None
    # The realm comes from the registry the task names. urllib also opens file: and ftp: URLs, and
    # a token read from one would be sent on to that registry.
    realm = urllib.parse.urlsplit(fields["realm"])
    if realm.scheme != "https" or not realm.hostname:
        raise ValueError(f"registry {host} names a token realm that is not an HTTPS URL")
    data = json.loads(_get(f"{fields['realm']}?service={fields.get('service', '')}"
                           f"&scope=repository:{repository}:pull", None, basic=_dockerhub_basic(host)))
    return data.get("token") or data.get("access_token")


@cache
def registry_config(image: str) -> dict:
    """The image config (Env, WorkingDir, ...) for linux/amd64, cached on disk."""
    path = CACHE_DIR / (hashlib.sha256(image.encode()).hexdigest() + ".json")
    try:
        if time.time() - path.stat().st_mtime < CACHE_TTL_SEC:
            return json.loads(path.read_text())
    except (OSError, ValueError):
        pass
    config = _fetch_config(image)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False) as file:
        json.dump(config, file)
    os.replace(file.name, path)
    return config


def _fetch_config(image: str) -> dict:
    host, repository, reference = split_reference(image)
    token = _token(host, repository)
    base = f"https://{host}/v2/{repository}"
    manifest = json.loads(_get(f"{base}/manifests/{reference}", token, ACCEPT))
    if "manifests" in manifest:
        chosen = next((m for m in manifest["manifests"]
                       if m.get("platform", {}).get("architecture") == "amd64"
                       and m.get("platform", {}).get("os") == "linux"), manifest["manifests"][0])
        manifest = json.loads(_get(f"{base}/manifests/{chosen['digest']}", token, ACCEPT))
    return json.loads(_get(f"{base}/blobs/{manifest['config']['digest']}", token)).get("config") or {}


def _env(entries: list[str] | None) -> dict[str, str]:
    return dict(entry.split("=", 1) for entry in entries or [] if "=" in entry)


def image_runtime(environment) -> tuple[dict[str, str], str | None]:
    """(ENV, WORKDIR) of the task image. Raises when the config is unavailable."""
    task = environment.task_env_config
    if task.docker_image:
        config = registry_config(task.docker_image)
        image_env, workdir = _env(config.get("Env")), config.get("WorkingDir") or None
    else:
        from dockerfile_parse import DockerfileParser

        dockerfile = DockerfileParser(path=str(environment.environment_dir / "Dockerfile"))
        config = registry_config(dockerfile.baseimage)
        base_env = _env(config.get("Env"))
        dockerfile.parent_env = base_env  # expands ${PATH}-style references
        image_env = {**base_env, **dockerfile.envs}
        workdir = getattr(environment, "_workdir", None) or config.get("WorkingDir") or None
    # Harbor's precedence: a task-configured workdir wins over the image's.
    return image_env, task.workdir or workdir


class ImageConfigUnavailableError(RuntimeError):
    """The task image's config could not be read; the trial is an infrastructure failure."""


async def resolve_image_runtime(environment, logger):
    """Image ENV below the environment's own task/trial values, and WORKDIR.

    Returns (None, None) for Docker, which applies the config natively. Raises
    ImageConfigUnavailableError when the config cannot be read, so the trial
    fails as infrastructure and is retried rather than run without its toolchain.
    """
    if isinstance(environment, DockerEnvironment):
        return None, None
    try:
        image_env, workdir = await asyncio.to_thread(image_runtime, environment)
    except Exception as error:
        raise ImageConfigUnavailableError(f"task image config unavailable ({type(error).__name__}: {error})") from error
    startup_env = getattr(environment, "_startup_env", dict)()
    return {key: value for key, value in image_env.items() if key not in startup_env}, workdir
