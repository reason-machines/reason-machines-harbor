"""The task image's runtime config (ENV and WORKDIR) for guests that drop it.

E2B builds a template from the image filesystem but its exec shells get
neither the image config's ENV nor its WORKDIR: PATH loses entries such as
/usr/local/go/bin and the shell starts in /root. This reads the config from the
image registry (anonymously, linux/amd64), as the stock Pi adapter's E2B
environment does. A Dockerfile task uses its FROM image's config plus its own
ENV and WORKDIR lines. Docker applies the config natively and is skipped.
"""
from __future__ import annotations

import asyncio
import json
import re
import urllib.error
import urllib.request
from functools import cache

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


def _get(url: str, token: str | None, accept: str | None = None) -> bytes:
    headers = {"Accept": accept} if accept else {}
    if token:
        headers["Authorization"] = "Bearer " + token
    with urllib.request.urlopen(urllib.request.Request(url, headers=headers), timeout=60) as response:
        return response.read()


def _token(host: str, repository: str) -> str | None:
    try:
        _get(f"https://{host}/v2/", None)
        return None
    except urllib.error.HTTPError as error:
        challenge = error.headers.get("WWW-Authenticate", "")
    fields = dict(re.findall(r'(\w+)="([^"]*)"', challenge))
    if "realm" not in fields:
        return None
    data = json.loads(_get(f"{fields['realm']}?service={fields.get('service', '')}"
                           f"&scope=repository:{repository}:pull", None))
    return data.get("token") or data.get("access_token")


@cache
def registry_config(image: str) -> dict:
    """The image config (Env, WorkingDir, ...) for linux/amd64."""
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


async def resolve_image_runtime(environment, logger):
    """Image ENV below the environment's own task/trial values, and WORKDIR.

    Returns (None, None) for Docker, which applies the config natively, and
    when the config is unavailable (logged); the driver then keeps the exec
    shell's environment and working directory.
    """
    if isinstance(environment, DockerEnvironment):
        return None, None
    try:
        image_env, workdir = await asyncio.to_thread(image_runtime, environment)
    except Exception as error:
        logger.warning("Task image config unavailable (%s: %s); the Reason worker keeps the "
                       "exec shell's environment and working directory", type(error).__name__, error)
        return None, None
    startup_env = getattr(environment, "_startup_env", dict)()
    return {key: value for key, value in image_env.items() if key not in startup_env}, workdir
