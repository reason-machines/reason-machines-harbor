"""Harbor environment on Munito sandboxes (Linux microVMs, docs.munito.ai/sandboxes).

Munito boots sandboxes only from its own templates, not from arbitrary OCI images,
so every task runs Docker-in-sandbox: one `ubuntu` sandbox (Docker + Compose) per
trial, with the task's image or Dockerfile brought up through Harbor's standard
compose files and every command going through `docker compose exec`. The compose
path gives single-image, Dockerfile and multi-service (sidecar) tasks one code path.

Network policy is enforced on the sandbox, not in compose: the sandbox keeps
internet so dockerd can pull and build, and a DOCKER-USER iptables rule drops
forwarded container traffic to the uplink while a phase is no-network. Sidecars on
the project's bridge still reach each other. That is what lets one sandbox switch
policy between the agent and verifier phases (dynamic_network_policy).

Only httpx is used; Munito's REST API is small enough not to need an SDK.
"""

from __future__ import annotations

import asyncio
import os
import re
import shlex
import tempfile
from pathlib import Path
from typing import Any

import httpx

from harbor.constants import MAIN_SERVICE_NAME
from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import (
    EnvironmentCapabilities,
    EnvironmentResourceCapabilities,
)
from harbor.environments.compose_service_ops import (
    ComposeServiceOpsMixin,
    ComposeServiceTransport,
)
from harbor.environments.definition import (
    COMPOSE_FILE_NAME,
    effective_exec_cwd,
    parse_dockerfile_workdir,
    should_use_prebuilt_docker_image,
)
from harbor.environments.dind_compose import DinDComposeOps
from harbor.environments.docker import (
    COMPOSE_BUILD_PATH,
    COMPOSE_PREBUILT_PATH,
    RESOURCES_COMPOSE_NAME,
    self_bind_mount,
    write_mounts_compose_file,
    write_resources_compose_file,
)
from harbor.environments.docker.compose_env import (
    ComposeInfraEnvVars,
    legacy_log_mount_env_vars,
    merge_compose_env,
)
from harbor.environments.docker.docker import _sanitize_docker_image_name
from harbor.environments.tar_transfer import (
    extract_dir_from_file,
    pack_dir_to_file,
    remote_pack_command,
    remote_unpack_command,
)
from harbor.models.task.config import NetworkMode, NetworkPolicy
from harbor.models.trial.config import ResourceMode, ServiceVolumeConfig
from harbor.utils.env import resolve_env_vars

API_BASE = os.environ.get("MUNITO_SANDBOX_API", "https://api.munito.ai/sandboxes/v1")

# The Docker host holds every task container plus dockerd and image layers, so it
# gets headroom over the task's own request. 4 vCPU matches what verifier-heavy
# tasks needed on Superserve (nextjs-performance timed out at 2).
_MIN_VCPUS = int(os.environ.get("MUNITO_MIN_VCPUS", "4"))
_MIN_MEMORY_MIB = 8192
_HOST_MEMORY_HEADROOM_MIB = 2048
_MAX_VCPUS = 16
_MAX_MEMORY_MIB = 32768
_MIN_DISK_GIB = 40
_MAX_DISK_GIB = 100
_SANDBOX_TIMEOUT_SEC = 86400
# /exec holds the HTTP request for the command's life and caps it at 1,800 s.
# Longer or unbounded commands run as background commands and are polled.
_SYNC_EXEC_MAX_SEC = 1700
_POLL_INTERVAL_SEC = 5.0
_BACKGROUND_MAX_SEC = 86400
_CREATE_RETRY_SEC = 15 * 60
# Munito's file API takes at most 100 MiB per file.
_FILE_CHUNK_BYTES = 90 * 1024 * 1024


def _quote(s: str) -> str:
    return shlex.quote(s)


def _slug(value: str, limit: int = 60) -> str:
    slug = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
    return (slug or "harbor")[:limit].strip("-")


class MunitoError(RuntimeError):
    pass


class _MunitoClient:
    """The few Munito sandbox calls Harbor needs, with retries on capacity limits."""

    def __init__(self, api_key: str):
        self._http = httpx.AsyncClient(
            base_url=API_BASE,
            headers={"Authorization": f"Bearer {api_key}"},
            timeout=httpx.Timeout(120.0, connect=30.0),
        )

    async def close(self):
        await self._http.aclose()

    async def _request(self, method: str, path: str, *, retries: int = 4, **kw) -> httpx.Response:
        delay = 2.0
        for attempt in range(retries + 1):
            try:
                resp = await self._http.request(method, path, **kw)
            except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError) as exc:
                if attempt == retries:
                    raise MunitoError(f"{method} {path}: {exc}") from exc
                await asyncio.sleep(delay)
                delay = min(delay * 2, 30)
                continue
            if resp.status_code in (502, 503, 504) and attempt < retries:
                await asyncio.sleep(float(resp.headers.get("retry-after") or delay))
                delay = min(delay * 2, 30)
                continue
            return resp
        raise AssertionError("unreachable")

    @staticmethod
    def _check(resp: httpx.Response, what: str) -> httpx.Response:
        if resp.status_code >= 400:
            raise MunitoError(f"{what}: HTTP {resp.status_code} {resp.text[:500]}")
        return resp

    async def create(self, body: dict[str, Any]) -> dict[str, Any]:
        """Create a sandbox. A full plan (429 sandbox_limit_reached) or a host with
        no room (503 no_capacity) waits and retries, so the plan cap acts as a queue."""
        deadline = asyncio.get_running_loop().time() + _CREATE_RETRY_SEC
        delay = 5.0
        while True:
            resp = await self._request("POST", "/sandboxes", json=body)
            if resp.status_code in (429, 503) and asyncio.get_running_loop().time() < deadline:
                await asyncio.sleep(float(resp.headers.get("retry-after") or delay))
                delay = min(delay * 1.5, 60)
                continue
            return self._check(resp, "create sandbox").json()

    async def delete(self, sandbox_id: str):
        resp = await self._request("DELETE", f"/sandboxes/{sandbox_id}")
        if resp.status_code not in (200, 202, 204, 404):
            self._check(resp, "delete sandbox")

    async def exec(self, sandbox_id: str, command: str, *, cwd: str | None,
                   env: dict[str, str] | None, timeout_sec: int | None) -> ExecResult:
        if timeout_sec is not None and timeout_sec <= _SYNC_EXEC_MAX_SEC:
            body: dict[str, Any] = {"command": command, "timeout": max(1, int(timeout_sec))}
            if cwd:
                body["cwd"] = cwd
            if env:
                body["env"] = env
            # A retried POST could run the command twice, so only connection-level
            # failures before the request is sent are retried (inside _request).
            resp = await self._request("POST", f"/sandboxes/{sandbox_id}/exec", json=body,
                                       timeout=httpx.Timeout(timeout_sec + 120.0, connect=30.0),
                                       retries=1)
            data = self._check(resp, "exec").json()
            stderr = data.get("stderr") or ""
            if data.get("timed_out"):
                stderr += f"\n[munito] command timed out after {timeout_sec}s"
            return ExecResult(stdout=data.get("stdout") or "", stderr=stderr,
                              return_code=data.get("exit_code") if data.get("exit_code") is not None else 124)
        return await self._background(sandbox_id, command, cwd=cwd, env=env, timeout_sec=timeout_sec)

    async def _background(self, sandbox_id: str, command: str, *, cwd: str | None,
                          env: dict[str, str] | None, timeout_sec: int | None) -> ExecResult:
        body: dict[str, Any] = {"command": command,
                                "timeout": int(min(timeout_sec or _BACKGROUND_MAX_SEC, _BACKGROUND_MAX_SEC))}
        if cwd:
            body["cwd"] = cwd
        if env:
            body["env"] = env
        started = self._check(await self._request("POST", f"/sandboxes/{sandbox_id}/commands",
                                                  json=body, retries=1), "start command").json()
        cid = started["id"]
        out: list[str] = []
        err: list[str] = []
        so = se = 0
        while True:
            await asyncio.sleep(_POLL_INTERVAL_SEC)
            resp = await self._request("GET", f"/sandboxes/{sandbox_id}/commands/{cid}",
                                       params={"stdout": so, "stderr": se})
            data = self._check(resp, "poll command").json()
            out.append(data.get("stdout") or "")
            err.append(data.get("stderr") or "")
            so = data.get("stdout_bytes", so)
            se = data.get("stderr_bytes", se)
            status = data.get("status")
            if status == "running":
                continue
            stderr = "".join(err)
            if status == "failed":
                stderr += "\n[munito] the sandbox lost the command"
            if data.get("timed_out"):
                stderr += f"\n[munito] command timed out after {timeout_sec}s"
            code = data.get("exit_code")
            return ExecResult(stdout="".join(out), stderr=stderr,
                              return_code=code if code is not None else 1)

    async def put_file(self, sandbox_id: str, path: str, data: bytes):
        resp = await self._request("PUT", f"/sandboxes/{sandbox_id}/files/content",
                                   params={"path": path}, content=data,
                                   headers={"Content-Type": "application/octet-stream"},
                                   timeout=httpx.Timeout(600.0, connect=30.0))
        self._check(resp, f"write {path}")

    async def get_file(self, sandbox_id: str, path: str) -> bytes:
        resp = await self._request("GET", f"/sandboxes/{sandbox_id}/files/content",
                                   params={"path": path}, timeout=httpx.Timeout(600.0, connect=30.0))
        return self._check(resp, f"read {path}").content


class ReasonMunitoEnvironment(ComposeServiceOpsMixin, BaseEnvironment):
    """One Munito `ubuntu` sandbox per trial, running the task through Docker Compose."""

    def __init__(self, environment_dir: Path, environment_name: str, session_id: str,
                 trial_paths, task_env_config, *, extra_docker_compose=None, **kwargs):
        self._sandbox_id: str | None = None
        self._client: _MunitoClient | None = None
        self._dind = _MunitoDinD(self)
        super().__init__(environment_dir=environment_dir, environment_name=environment_name,
                         session_id=session_id, trial_paths=trial_paths,
                         task_env_config=task_env_config,
                         extra_docker_compose=extra_docker_compose, **kwargs)
        self._dockerfile_workdir = parse_dockerfile_workdir(self.environment_dir / "Dockerfile")
        self._workdir = effective_exec_cwd(None, self.task_env_config.workdir,
                                           self._dockerfile_workdir) or "/"

    @classmethod
    def preflight(cls) -> None:
        if not os.environ.get("MUNITO_API_KEY"):
            raise SystemExit("ReasonMunitoEnvironment needs MUNITO_API_KEY "
                             "(1Password: op://Sven/ikuunvqp5mqzcjvgekdqf4su7m/credential).")

    @staticmethod
    def type() -> str:
        return "munito"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_request=True, memory_request=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        # No GPUs on Munito. Allowlists resolve each hostname on the sandbox and accept
        # forwarded traffic to those addresses only (no wildcards).
        return EnvironmentCapabilities(
            docker_compose=True,
            disable_internet=True,
            network_allowlist=True,
            network_allowlist_hostnames=True,
            network_allowlist_ipv4_addresses=True,
            network_allowlist_ipv6_addresses=True,
            dynamic_network_policy=True,
        )

    def _validate_definition(self) -> None:
        if not (self.environment_dir / "Dockerfile").exists() and not (
                self.environment_dir / COMPOSE_FILE_NAME).exists() and not self.task_env_config.docker_image:
            raise FileNotFoundError(
                f"{self.environment_dir} has no Dockerfile, {COMPOSE_FILE_NAME} or docker_image.")

    @property
    def _uses_compose(self) -> bool:
        return True

    def _compose_service_transport(self, service: str | None) -> ComposeServiceTransport:
        return self._dind

    # ── sandbox host primitives ─────────────────────────────────────────

    def _require(self) -> tuple[_MunitoClient, str]:
        if self._client is None or self._sandbox_id is None:
            raise RuntimeError("Munito sandbox is not running; call start() first.")
        return self._client, self._sandbox_id

    async def _host_run(self, command: str, *, env: dict[str, str] | None = None,
                        timeout_sec: int | None = None) -> ExecResult:
        client, sid = self._require()
        return await client.exec(sid, command, cwd="/", env=env, timeout_sec=timeout_sec)

    async def _host_put(self, source: Path | str, host_path: str):
        await self._host_put_bytes(Path(source).read_bytes(), host_path)

    async def _host_put_bytes(self, data: bytes, host_path: str):
        """Write a file on the host. Munito's file API takes at most 100 MiB per file,
        so larger files go up in chunks and are joined on the host."""
        client, sid = self._require()
        if len(data) <= _FILE_CHUNK_BYTES:
            await client.put_file(sid, host_path, data)
            return
        prefix = f"/tmp/harbor-chunk-{os.urandom(6).hex()}"
        parts = []
        for i in range(0, len(data), _FILE_CHUNK_BYTES):
            part = f"{prefix}.{i // _FILE_CHUNK_BYTES:05d}"
            await client.put_file(sid, part, data[i:i + _FILE_CHUNK_BYTES])
            parts.append(part)
        q = " ".join(_quote(p) for p in parts)
        r = await self._host_run(
            f"mkdir -p $(dirname {_quote(host_path)}) && cat {q} > {_quote(host_path)} && rm -f {q}",
            timeout_sec=600)
        if r.return_code != 0:
            raise RuntimeError(f"joining chunks into {host_path} failed: {r.stderr}")

    async def _host_get_bytes(self, host_path: str) -> bytes:
        """Read a file from the host, in chunks when it exceeds the 100 MiB API limit."""
        client, sid = self._require()
        try:
            return await client.get_file(sid, host_path)
        except MunitoError as exc:
            if "HTTP 413" not in str(exc):
                raise
        prefix = f"/tmp/harbor-chunk-{os.urandom(6).hex()}."
        r = await self._host_run(
            f"split -a 5 -d -b {_FILE_CHUNK_BYTES} {_quote(host_path)} {_quote(prefix)} && ls {_quote(prefix)}*",
            timeout_sec=600)
        if r.return_code != 0:
            raise RuntimeError(f"splitting {host_path} failed: {r.stderr}")
        chunks = [await client.get_file(sid, p) for p in sorted(r.stdout.split())]
        await self._host_run(f"rm -f {_quote(prefix)}*", timeout_sec=60)
        return b"".join(chunks)

    async def _host_put_dir(self, source_dir: Path | str, host_dir: str):
        client, sid = self._require()
        with tempfile.TemporaryDirectory(prefix="harbor-munito-") as tmp:
            archive = Path(tmp) / "dir.tar.gz"
            pack_dir_to_file(source_dir, archive)
            remote = f"/tmp/harbor-up-{os.urandom(6).hex()}.tar.gz"
            await self._host_put_bytes(archive.read_bytes(), remote)
        result = await self._host_run(
            f"{remote_unpack_command(remote, host_dir)} && rm -f {_quote(remote)}", timeout_sec=600)
        if result.return_code != 0:
            raise RuntimeError(f"unpack into {host_dir} failed: {result.stdout} {result.stderr}")

    async def _host_get(self, host_path: str, target: Path | str):
        data = await self._host_get_bytes(host_path)
        target = Path(target)
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)

    async def _host_get_dir(self, host_dir: str, target_dir: Path | str):
        client, sid = self._require()
        remote = f"/tmp/harbor-down-{os.urandom(6).hex()}.tar.gz"
        result = await self._host_run(remote_pack_command(host_dir, remote), timeout_sec=600)
        if result.return_code != 0:
            raise RuntimeError(f"pack {host_dir} failed: {result.stdout} {result.stderr}")
        data = await self._host_get_bytes(remote)
        await self._host_run(f"rm -f {_quote(remote)}", timeout_sec=30)
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryDirectory(prefix="harbor-munito-") as tmp:
            archive = Path(tmp) / "dir.tar.gz"
            archive.write_bytes(data)
            extract_dir_from_file(archive, target)

    # ── lifecycle ───────────────────────────────────────────────────────

    def _sandbox_body(self) -> dict[str, Any]:
        cpus = int(self._effective_cpus or 0)
        mem = int(self._effective_memory_mb or 0)
        vcpus = min(max(cpus, _MIN_VCPUS), _MAX_VCPUS)
        memory = max(mem + _HOST_MEMORY_HEADROOM_MIB, _MIN_MEMORY_MIB, vcpus * 512)
        memory = min(-(-memory // 256) * 256, _MAX_MEMORY_MIB)
        disk = int((self._effective_storage_mb or 0) / 1024) + 20
        disk = min(max(disk, _MIN_DISK_GIB), _MAX_DISK_GIB)
        meta = {"harbor": "1", "env": _slug(self.environment_name, 64),
                "session": _slug(self.session_id, 64)}
        if job := os.environ.get("MUNITO_JOB_TAG"):
            meta["job"] = _slug(job, 64)
        return {"template": "ubuntu", "vcpus": vcpus, "memory_mib": memory, "disk_gib": disk,
                "internet": True, "timeout": _SANDBOX_TIMEOUT_SEC, "metadata": meta}

    async def start(self, force_build: bool) -> None:
        if self._sandbox_id is not None:
            return
        self._client = _MunitoClient(os.environ["MUNITO_API_KEY"])
        sandbox = await self._client.create(self._sandbox_body())
        self._sandbox_id = sandbox["id"]
        self.logger.debug("Munito sandbox %s started", self._sandbox_id)
        await self._dind.start(force_build)
        await self._apply_network_policy(self._network_policy)

    async def stop(self, delete: bool) -> None:
        # Always delete: a kept Munito sandbox bills for its full size every second.
        client, sid = self._client, self._sandbox_id
        self._sandbox_id = None
        if client is None:
            return
        try:
            if sid is not None:
                await client.delete(sid)
        except Exception as exc:
            self.logger.warning("Failed to delete Munito sandbox %s: %s", sid, exc)
        finally:
            await client.close()
            self._client = None

    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None:
        mode = network_policy.network_mode
        hosts = list(network_policy.allowed_hosts) if mode == NetworkMode.ALLOWLIST else []
        wild = [h for h in hosts if "*" in h]
        if wild:
            raise ValueError(
                f"Munito environment resolves allowlisted hosts to addresses, so wildcard entries "
                f"{wild} cannot be enforced; list exact hostnames instead.")
        block = mode in (NetworkMode.NO_NETWORK, NetworkMode.ALLOWLIST)
        # Rules carry comments so they can be removed idempotently. Forwarded traffic
        # to the uplink is dropped; host processes (dockerd pulls, the embedded DNS that
        # containers resolve through) and container-to-container traffic are untouched.
        # An allowlist inserts ACCEPT rules for each host's resolved addresses above the
        # DROP. Hosts are resolved several times to catch rotating DNS answers.
        script = r"""
set -e
IF=$(ip route show default | awk '{print $5; exit}')
for T in iptables ip6tables; do
  command -v $T >/dev/null || continue
  $T -N DOCKER-USER 2>/dev/null || true
  $T -S DOCKER-USER | grep -E 'harbor-(no-network|allow)' | sed 's/^-A /-D /' | while read -r rule; do eval "$T $rule"; done
  if [ "$BLOCK" = 1 ]; then
    $T -I DOCKER-USER -o "$IF" -m comment --comment harbor-no-network -j DROP || [ $T = ip6tables ]
  fi
done
# Docker's embedded DNS forwards lookups from the container's namespace, so an
# allowlist must also reach the sandbox's upstream resolvers on port 53.
if [ -n "$HOSTS" ]; then
  RC=/etc/resolv.conf; [ -f /run/systemd/resolve/resolv.conf ] && RC=/run/systemd/resolve/resolv.conf
  for ns in $(awk '/^nameserver/{print $2}' $RC); do
    case $ns in 127.*|::1) continue ;; *:*) T=ip6tables ;; *) T=iptables ;; esac
    command -v $T >/dev/null || continue
    for proto in udp tcp; do
      $T -I DOCKER-USER -o "$IF" -d "$ns" -p $proto --dport 53 -m comment --comment harbor-allow -j ACCEPT || [ $T = ip6tables ]
    done
  done
fi
for h in $HOSTS; do
  ips=$(for i in 1 2 3; do getent ahosts "$h" | awk '{print $1}'; done | sort -u)
  [ -n "$ips" ] || { echo "cannot resolve allowlisted host $h" >&2; exit 3; }
  for ip in $ips; do
    case $ip in *:*) T=ip6tables ;; *) T=iptables ;; esac
    command -v $T >/dev/null || continue
    $T -I DOCKER-USER -o "$IF" -d "$ip" -m comment --comment harbor-allow -j ACCEPT || [ $T = ip6tables ]
  done
done
"""
        result = await self._host_run(
            script, env={"BLOCK": "1" if block else "0", "HOSTS": " ".join(hosts)}, timeout_sec=60)
        if result.return_code != 0:
            raise RuntimeError(f"Applying network policy failed: {result.stdout} {result.stderr}")

    # ── Harbor operations (all via compose on the host) ─────────────────

    async def exec(self, command: str, cwd: str | None = None, env: dict[str, str] | None = None,
                   timeout_sec: int | None = None, user: str | int | None = None) -> ExecResult:
        return await self._dind.exec(
            command,
            cwd=effective_exec_cwd(cwd, self.task_env_config.workdir, self._dockerfile_workdir) or "/",
            env=self._merge_env(env), timeout_sec=timeout_sec,
            user=self._resolve_user(user), service=MAIN_SERVICE_NAME)

    async def upload_file(self, source_path: Path | str, target_path: str) -> None:
        await self._dind.upload_file(source_path, target_path)

    async def upload_dir(self, source_dir: Path | str, target_dir: str) -> None:
        await self._dind.upload_dir(source_dir, target_dir)

    async def download_file(self, source_path: str, target_path: Path | str) -> None:
        await self._dind.download_file(source_path, target_path)

    async def download_dir(self, source_dir: str, target_dir: Path | str) -> None:
        await self._dind.download_dir(source_dir, target_dir)


class _MunitoDinD(DinDComposeOps):
    """Compose strategy on the Munito sandbox's own dockerd (after Harbor's Blaxel one)."""

    _SELF_BIND_LOG_DIRS = True
    _CP_FILE_TIMEOUT_SEC = 300
    _CP_DIR_TIMEOUT_SEC = 600
    _DOCKER_DAEMON_TIMEOUT_SEC = 90
    _MAIN_CONTAINER_TIMEOUT_SEC = 90
    _COMPOSE_DIR = "/harbor/compose"
    _ENVIRONMENT_DIR = "/harbor/environment"
    _MOUNTS_COMPOSE_NAME = "docker-compose-mounts.json"
    # The sandbox's dockerd hands containers a low open-files limit; Docker Desktop and
    # e2b VMs allow ~1M. eicrud-keyset-pagination-cursor's in-process Mongo hit
    # "Too many open files" until main got the usual limit.
    _ULIMITS_COMPOSE_NAME = "docker-compose-munito-ulimits.json"
    _ULIMITS_COMPOSE = '{"services": {"main": {"ulimits": {"nofile": {"soft": 1048576, "hard": 1048576}}}}}'

    def __init__(self, env: ReasonMunitoEnvironment) -> None:
        self._env = env
        self._use_prebuilt = False

    async def exec(self, command: str, cwd: str | None = None, env: dict[str, str] | None = None,
                   timeout_sec: int | None = None, user: str | int | None = None, *,
                   service: str | None = None) -> ExecResult:
        """`docker compose exec`, matching Harbor's local Docker environment: `bash -c` in
        main, not DinDComposeOps' `bash -lc`. A login shell runs /etc/profile, which resets
        PATH and drops image ENV entries such as /opt/venv/bin, so graders cannot find
        pytest (the igel-persist-feature-schema failure the benchmark notes on e2b)."""
        service = service or MAIN_SERVICE_NAME
        parts = ["exec", "-T"]
        if cwd:
            parts += ["-w", cwd]
        for k, v in (env or {}).items():
            parts += ["-e", f"{k}={v}"]
        if user is not None:
            parts += ["-u", str(user)]
        parts.append(service)
        parts += ["bash", "-c", command] if service == MAIN_SERVICE_NAME else ["sh", "-c", command]
        return await self._compose_exec(parts, timeout_sec=timeout_sec)

    async def _host_exec(self, command: str, timeout_sec: int | None = None) -> ExecResult:
        return await self._env._host_run(command, timeout_sec=timeout_sec)

    async def _stage_file_to_host(self, source_path: Path | str, host_path: str):
        await self._env._host_put(source_path, host_path)

    async def _stage_dir_to_host(self, source_dir: Path | str, host_dir: str):
        await self._env._host_put_dir(source_dir, host_dir)

    async def _fetch_file_from_host(self, host_path: str, target_path: Path | str):
        await self._env._host_get(host_path, target_path)

    async def _fetch_dir_from_host(self, host_dir: str, target_dir: Path | str):
        await self._env._host_get_dir(host_dir, target_dir)

    def _resolve_volumes(self) -> list[ServiceVolumeConfig]:
        return [self_bind_mount(m) if m.get("type") == "bind" else m for m in self._env._mounts]

    def _infra_env_vars(self) -> dict[str, str]:
        e = self._env
        env_vars = ComposeInfraEnvVars(
            main_image_name=_sanitize_docker_image_name(f"hb__{e.environment_name}"),
            context_dir=self._ENVIRONMENT_DIR,
            prebuilt_image_name=e.task_env_config.docker_image if self._use_prebuilt else None,
            cpus=e._effective_cpus,
            memory=f"{m}M" if (m := e._effective_memory_mb) else None,
        ).to_env_dict()
        env_vars.update(legacy_log_mount_env_vars(self._resolve_volumes(), host_value="target"))
        return env_vars

    def _compose_env_vars(self) -> dict[str, str]:
        e = self._env
        user_env: dict[str, str] = {}
        paths = [e.environment_dir / COMPOSE_FILE_NAME, *e.extra_docker_compose_paths]
        text = "\n".join(p.read_text() for p in paths if p.exists())
        for a, b in re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-[^}]*)?\}|\$([A-Za-z_][A-Za-z0-9_]*)\b", text):
            if (v := os.environ.get(a or b)) is not None:
                user_env[a or b] = v
        if e.task_env_config.env:
            user_env.update(resolve_env_vars(e.task_env_config.env))
        if e._persistent_env:
            user_env.update(e._persistent_env)
        return merge_compose_env(user_env=user_env, infra_env=self._infra_env_vars(),
                                 logger=e.logger, collision_label="Referenced/task/persistent env vars")

    def _extra_compose_target_paths(self) -> list[str]:
        return [f"{self._COMPOSE_DIR}/docker-compose-extra-{i}.yaml"
                for i, _ in enumerate(self._env.extra_docker_compose_paths)]

    def _compose_cmd(self, subcommand: list[str]) -> str:
        files = [f"{self._COMPOSE_DIR}/{RESOURCES_COMPOSE_NAME}",
                 f"{self._COMPOSE_DIR}/" + ("docker-compose-prebuilt.yaml" if self._use_prebuilt
                                            else "docker-compose-build.yaml"),
                 f"{self._COMPOSE_DIR}/{self._MOUNTS_COMPOSE_NAME}",
                 f"{self._COMPOSE_DIR}/{self._ULIMITS_COMPOSE_NAME}"]
        if (self._env.environment_dir / COMPOSE_FILE_NAME).exists():
            files.append(f"{self._ENVIRONMENT_DIR}/{COMPOSE_FILE_NAME}")
        files.extend(self._extra_compose_target_paths())
        flags = [x for f in files for x in ("-f", f)]
        return shlex.join(["docker", "compose", "-p", _slug(self._env.session_id),
                           "--project-directory", self._ENVIRONMENT_DIR, *flags, *subcommand])

    async def _compose_exec(self, subcommand: list[str], timeout_sec: int | None = None) -> ExecResult:
        return await self._env._host_run(self._compose_cmd(subcommand), env=self._compose_env_vars(),
                                         timeout_sec=timeout_sec)

    async def _wait(self, check, what: str, timeout: int):
        last = ""
        for _ in range(timeout // 2):
            r = await check()
            if r.return_code == 0:
                return
            last = (r.stdout or "") + (r.stderr or "")
            await asyncio.sleep(2)
        raise RuntimeError(f"{what} not ready after {timeout}s: {last[-500:]}")

    async def start(self, force_build: bool) -> None:
        e = self._env
        await self._wait(lambda: self._host_exec("docker info >/dev/null", timeout_sec=15),
                         "Docker daemon", self._DOCKER_DAEMON_TIMEOUT_SEC)
        if (user := os.environ.get("DOCKERHUB_USERNAME")) and (tok := os.environ.get("DOCKERHUB_TOKEN")):
            client, sid = e._require()
            r = await client.exec(sid, 'printf %s "$T" | docker login -u "$U" --password-stdin >/dev/null',
                                  cwd="/", env={"U": user, "T": tok}, timeout_sec=60)
            if r.return_code != 0:
                e.logger.warning("docker login failed: %s", r.stderr[-300:])
        await e._host_put_bytes(self._ULIMITS_COMPOSE.encode(), f"{self._COMPOSE_DIR}/{self._ULIMITS_COMPOSE_NAME}")
        for path in (COMPOSE_BUILD_PATH, COMPOSE_PREBUILT_PATH):
            await e._host_put(path, f"{self._COMPOSE_DIR}/{path.name}")
        with tempfile.TemporaryDirectory(prefix="harbor-munito-") as tmp:
            res = Path(tmp) / RESOURCES_COMPOSE_NAME
            write_resources_compose_file(
                res,
                cpu_request=e._resource_request_value("cpu", auto_mode=ResourceMode.REQUEST),
                cpu_limit=e._resource_limit_value("cpu", auto_mode=ResourceMode.REQUEST),
                memory_request_mb=e._resource_request_value("memory", auto_mode=ResourceMode.REQUEST),
                memory_limit_mb=e._resource_limit_value("memory", auto_mode=ResourceMode.REQUEST))
            await e._host_put(res, f"{self._COMPOSE_DIR}/{RESOURCES_COMPOSE_NAME}")
            volumes = self._resolve_volumes()
            mounts = Path(tmp) / self._MOUNTS_COMPOSE_NAME
            write_mounts_compose_file(mounts, volumes)
            await e._host_put(mounts, f"{self._COMPOSE_DIR}/{self._MOUNTS_COMPOSE_NAME}")
        await e._host_put_dir(e.environment_dir, self._ENVIRONMENT_DIR)
        for src, dst in zip(e.extra_docker_compose_paths, self._extra_compose_target_paths(), strict=True):
            await e._host_put(src, dst)
        binds = [v["source"] for v in volumes if v.get("type") == "bind"]
        if binds:
            q = " ".join(_quote(b) for b in binds)
            r = await self._host_exec(f"mkdir -p {q} && chmod 777 {q}", timeout_sec=30)
            if r.return_code != 0:
                raise RuntimeError(f"Preparing bind mounts failed: {r.stdout} {r.stderr}")

        self._use_prebuilt = should_use_prebuilt_docker_image(
            e.environment_dir, docker_image=e.task_env_config.docker_image, force_build=force_build)
        build_timeout = round(e.task_env_config.build_timeout_sec)
        for attempt in range(3):  # registry pulls fail transiently at high concurrency
            step = ["pull", "--quiet", MAIN_SERVICE_NAME] if self._use_prebuilt else ["build"]
            r = await self._compose_exec(step, timeout_sec=build_timeout)
            if r.return_code == 0:
                break
            if attempt == 2:
                raise RuntimeError(f"docker compose {step[0]} failed: {r.stdout[-2000:]} {r.stderr[-2000:]}")
            await asyncio.sleep(10 * (attempt + 1))
        r = await self._compose_exec(["up", "-d"], timeout_sec=max(600, build_timeout))
        if r.return_code != 0:
            raise RuntimeError(f"docker compose up failed: {r.stdout[-2000:]} {r.stderr[-2000:]}")
        await self._wait(lambda: self._compose_exec(["exec", "-T", MAIN_SERVICE_NAME, "true"], timeout_sec=20),
                         "Main container", self._MAIN_CONTAINER_TIMEOUT_SEC)
        await e._upload_environment_dir_after_start()
