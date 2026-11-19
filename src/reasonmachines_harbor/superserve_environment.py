"""Harbor environment on Superserve sandboxes (Firecracker microVMs).

A task's image becomes a Superserve template (named from the task image and its
resources, so reruns reuse the build). The sandbox starts from that template with
the image's ENV and a profile.d PATH fix, the same as ReasonE2BEnvironment.

Superserve's template input is an OCI image plus build steps, not a Dockerfile,
so only tasks that name a docker_image run here.
"""
import asyncio
import io
import os
import shlex
import zipfile
from pathlib import Path, PurePosixPath

from harbor.environments.base import BaseEnvironment, ExecResult
from harbor.environments.capabilities import EnvironmentCapabilities, EnvironmentResourceCapabilities
from harbor.environments.definition import SNAPSHOT_HASH_LEN, effective_exec_cwd, parse_dockerfile_workdir
from harbor.models.task.config import NetworkMode, NetworkPolicy

from .environment import PROFILE_SCRIPT, image_path_profile
from .image_runtime import ImageConfigUnavailableError, image_runtime

ALL_TRAFFIC = "0.0.0.0/0"
# Superserve allows 5 concurrent template builds per team (more needs a support ticket), and
# each task builds its own template, so builds queue here; the template is reused afterwards.
BUILD_SLOTS = asyncio.Semaphore(5)
# Superserve also caps a team at 100 templates. Templates this process is using are never evicted;
# when the cap is hit, the oldest other reason-* template (a rebuildable cache) is deleted.
TEMPLATES_IN_USE: set[str] = set()
SANDBOX_TIMEOUT_S = 86_400
DEFAULT_EXEC_TIMEOUT_S = 86_400


class SuperserveUnsupportedError(RuntimeError):
    """A task this environment cannot run on Superserve, with the reason."""


class ReasonSuperserveEnvironment(BaseEnvironment):
    """Superserve sandboxes with the task image's ENV, PATH and resources."""

    @classmethod
    def preflight(cls) -> None:
        if not os.environ.get("SUPERSERVE_API_KEY"):
            raise SystemExit("Superserve requires SUPERSERVE_API_KEY to be set.")
        try:
            import superserve  # noqa: F401
        except ImportError as error:
            raise SystemExit("Superserve needs the superserve package: uv add superserve") from error

    def __init__(self, environment_dir: Path, environment_name: str, session_id: str,
                 trial_paths, task_env_config, *args, **kwargs):
        super().__init__(environment_dir=environment_dir, environment_name=environment_name,
                         session_id=session_id, trial_paths=trial_paths,
                         task_env_config=task_env_config, **kwargs)
        self._workdir = parse_dockerfile_workdir(self._environment_definition_path)
        self._sandbox = None
        env_hash = self.environment_id[:SNAPSHOT_HASH_LEN]
        # Template names: lowercase letters, digits and dashes only.
        base = "".join(c if c.isalnum() else "-" for c in environment_name.lower()).strip("-")
        self._template_name = f"reason-{base[:40]}-{env_hash}".lower()

    @staticmethod
    def type() -> str:
        return "superserve"

    @classmethod
    def resource_capabilities(cls) -> EnvironmentResourceCapabilities:
        return EnvironmentResourceCapabilities(cpu_request=True, memory_request=True)

    @property
    def capabilities(self) -> EnvironmentCapabilities:
        return EnvironmentCapabilities(
            disable_internet=True,
            network_allowlist=True,
            network_allowlist_hostnames=True,
            network_allowlist_wildcard_hostnames=False,
            network_allowlist_ipv4_addresses=True,
            network_allowlist_ipv6_addresses=False,
            network_allowlist_ipv4_cidrs=True,
            network_allowlist_ipv6_cidrs=False,
            dynamic_network_policy=True,
        )

    @property
    def _environment_definition_path(self) -> Path:
        return self.environment_dir / "Dockerfile"

    def _validate_definition(self):
        from harbor.environments.definition import require_agent_environment_definition

        require_agent_environment_definition(self.environment_dir,
                                             docker_image=self.task_env_config.docker_image)

    def _network_config(self, policy: NetworkPolicy | None = None):
        from superserve import NetworkConfig

        policy = policy or self.network_policy
        if policy.network_mode == NetworkMode.PUBLIC:
            return NetworkConfig()
        if policy.network_mode == NetworkMode.NO_NETWORK:
            return NetworkConfig(deny_out=[ALL_TRAFFIC])
        return NetworkConfig(allow_out=list(policy.allowed_hosts), deny_out=[ALL_TRAFFIC])

    async def _apply_network_policy(self, network_policy: NetworkPolicy) -> None:
        # Harbor switches the agent phase to the task's policy after start (verifier phases too).
        await self._require_sandbox().update(network=self._network_config(network_policy))

    async def _ensure_template(self, force_build: bool):
        from superserve import AsyncTemplate

        TEMPLATES_IN_USE.add(self._template_name)
        image = self._base_image()
        if not force_build and any(
                t.name == self._template_name for t in await AsyncTemplate.list()):
            return
        # Floor of 4 vCPU / 8 GiB: a 2-vCPU sandbox timed out the nextjs-performance verifier
        # (300 s limit; it passes in about 105 s at 4 vCPU).
        vcpu = max(self._effective_cpus or 2, 4)
        memory_mib = max(self._effective_memory_mb or 4096, 8192)
        for attempt in range(3):
            async with BUILD_SLOTS:
                try:
                    template = await _create_template_evicting(
                        name=self._template_name, from_=image, vcpu=vcpu, memory_mib=memory_mib,
                        disk_mib=max(self._effective_storage_mb or 0, 8192))
                    await template.wait_until_ready(poll_interval_s=5.0)
                    return
                except Exception as error:
                    # Registry pulls fail transiently (the same image built on retry); keep the
                    # last error if it persists.
                    if "pull base image" not in str(error) or attempt == 2:
                        raise
                    self.logger.warning(f"Template build hit a registry pull failure; retrying ({attempt + 1}/2)")
            await asyncio.sleep(10)

    async def start(self, force_build: bool):
        from superserve import AsyncSandbox, NetworkConfig

        try:
            self._image_env, _ = await asyncio.to_thread(image_runtime, self)
        except Exception as error:
            raise ImageConfigUnavailableError(
                f"task image config unavailable ({type(error).__name__}: {error})") from error
        await self._ensure_template(force_build)
        self._sandbox = await AsyncSandbox.create(
            name=f"reason-{self.session_id[:12]}".lower(),
            from_template=self._template_name,
            timeout_seconds=SANDBOX_TIMEOUT_S,
            metadata={"environment_name": self.environment_name, "session_id": self.session_id},
            env_vars=self._startup_env(),
            # Dockerfile steps are build-time work: they may download (Go modules, toolchains), as
            # a Docker build does, so the sandbox starts open and takes the task's policy after.
            network=NetworkConfig() if not self.task_env_config.docker_image else self._network_config(),
        )
        await self.ensure_dirs(self._mount_targets(writable_only=True))
        path = self._startup_env().get("PATH") if "PATH" in self._image_env else None
        if path:
            await self._sandbox.files.write(PROFILE_SCRIPT, image_path_profile(path))
        if not self.task_env_config.docker_image:
            await self._apply_dockerfile_steps()
            await self._sandbox.update(network=self._network_config())
        await self._upload_environment_dir_after_start()

    def _base_image(self) -> str:
        """The task's image. A separate verifier environment has none in task.toml, so its
        Dockerfile's FROM is the base and the rest of the Dockerfile is applied after start."""
        image = self.task_env_config.docker_image
        if image:
            return image
        if not self._environment_definition_path.exists():
            raise SuperserveUnsupportedError(
                f"{self.environment_name} has no docker_image or Dockerfile to start from.")
        for line in _dockerfile_lines(self._environment_definition_path):
            if line[0].upper() == "FROM":
                return line[1].split()[0]
        raise SuperserveUnsupportedError(f"{self.environment_name}: Dockerfile has no FROM.")

    async def _apply_dockerfile_steps(self) -> None:
        """Apply a Dockerfile's ENV, WORKDIR, USER, COPY and RUN in order inside the sandbox.
        Superserve templates take OCI images and run/env/workdir/user steps only, and COPY
        needs the build context, so these run here, as a Dockerfile build would."""
        sandbox = self._require_sandbox()
        context = self.environment_dir
        env: dict[str, str] = {}
        workdir, user = "/", "root"
        for instruction, args in _dockerfile_lines(self._environment_definition_path):
            op = instruction.upper()
            if op == "ENV":
                env.update(_parse_env(args))
            elif op == "WORKDIR":
                workdir = args if args.startswith("/") else f"{workdir.rstrip('/')}/{args}"
                await sandbox.commands.run(f"mkdir -p {_quote(workdir)}", user="root", timeout_seconds=60)
            elif op == "USER":
                user = args.split(":")[0]
            elif op == "COPY":
                parts = [p for p in args.split() if not p.startswith("--")]
                *sources, dest = parts
                dest = dest if dest.startswith("/") else f"{workdir.rstrip('/')}/{dest}"
                for src in sources:
                    source = context / src
                    if source.is_dir():
                        for file_path in _copy_sources(source):
                            target = f"{dest.rstrip('/')}/{file_path.relative_to(source).as_posix()}"
                            await sandbox.files.write(target, file_path.read_bytes())
                    else:
                        target = dest + source.name if dest.endswith("/") else dest
                        await sandbox.files.write(target, source.read_bytes())
            elif op == "RUN":
                result = await sandbox.commands.run(
                    f"/bin/bash -lc {_quote(args)}", cwd=workdir, env=env or None,
                    user=user, timeout_seconds=DEFAULT_EXEC_TIMEOUT_S)
                if result.exit_code != 0:
                    raise RuntimeError(f"Dockerfile RUN failed ({result.exit_code}): {args[:200]}\n"
                                       f"{result.stderr[-800:]}")

    def _startup_env(self) -> dict[str, str]:
        # Superserve sets HOME=/home/user even for root, but task images populate root's caches
        # (Go modules, pip, nvm) under /root, as E2B's root sandboxes see them.
        # Superserve sets TERM=linux, which makes terminal UIs fall back to ASCII (clack's snapshots
        # expect Unicode); E2B and Docker sandboxes use a 256-colour terminal.
        return {"HOME": "/root", "TERM": "xterm-256color",
                **getattr(self, "_image_env", {}), **super()._startup_env()}

    async def stop(self, delete: bool):
        if not delete:
            self.logger.debug("Superserve sandboxes are ephemeral; deleting after use.")
        if self._sandbox is not None:
            try:
                await self._sandbox.kill()
            except Exception as error:
                self.logger.error(f"Error stopping Superserve sandbox: {error}")
            finally:
                self._sandbox = None
        TEMPLATES_IN_USE.discard(self._template_name)

    def _require_sandbox(self):
        if self._sandbox is None:
            raise RuntimeError("Sandbox not found. Please start the environment first.")
        return self._sandbox

    async def upload_file(self, source_path: Path | str, target_path: str):
        await self._require_sandbox().files.write(target_path, Path(source_path).read_bytes())

    async def upload_dir(self, source_dir: Path | str, target_dir: str):
        sandbox = self._require_sandbox()
        root = Path(source_dir)
        for file_path in sorted(root.rglob("*")):
            if file_path.is_file():
                target = str(PurePosixPath(target_dir) / file_path.relative_to(root).as_posix())
                await sandbox.files.write(target, file_path.read_bytes())

    async def download_file(self, source_path: str, target_path: Path | str):
        data = await self._require_sandbox().files.read(source_path)
        Path(target_path).parent.mkdir(parents=True, exist_ok=True)
        Path(target_path).write_bytes(data)

    async def download_dir(self, source_dir: str, target_dir: Path | str):
        """Superserve returns a directory as a zip whose entries are prefixed with the
        directory's own name (/tmp/d -> d/x/y.txt); extract them relative to target_dir."""
        data = await self._require_sandbox().files.download_dir(source_dir)
        target = Path(target_dir)
        target.mkdir(parents=True, exist_ok=True)
        prefix = PurePosixPath(source_dir).name + "/"
        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            for member in archive.infolist():
                name = member.filename
                if name.startswith(prefix):
                    name = name[len(prefix):]
                if not name:
                    continue
                if name.endswith("/"):
                    (target / name).mkdir(parents=True, exist_ok=True)
                    continue
                out = target / name
                out.parent.mkdir(parents=True, exist_ok=True)
                out.write_bytes(archive.read(member))

    async def is_dir(self, path: str, user: str | int | None = None) -> bool:
        return (await self.exec(f"test -d {_quote(path)}", user=user)).return_code == 0

    async def is_file(self, path: str, user: str | int | None = None) -> bool:
        return (await self.exec(f"test -f {_quote(path)}", user=user)).return_code == 0

    async def exec(self, command: str, cwd: str | None = None, env: dict[str, str] | None = None,
                   timeout_sec: int | None = None, user: str | int | None = None) -> ExecResult:
        sandbox = self._require_sandbox()
        user = self._resolve_user(user)
        # Superserve runs commands under /bin/sh (dash on Debian images), but Harbor's
        # installers are bash scripts. E2B runs every command in a login bash, so match it.
        result = await sandbox.commands.run(
            f"/bin/bash -lc {_quote(command)}",
            cwd=effective_exec_cwd(cwd, self.task_env_config.workdir, self._workdir),
            env=self._merge_env(env),
            user=str(user) if user is not None else "root",
            timeout_seconds=timeout_sec or DEFAULT_EXEC_TIMEOUT_S,
        )
        return ExecResult(stdout=result.stdout, stderr=result.stderr, return_code=result.exit_code)


async def _create_template_evicting(**kwargs):
    """Create a template, evicting the oldest idle reason-* template when the team cap is hit."""
    from superserve import AsyncTemplate
    from superserve.errors import RateLimitError

    from superserve.errors import ConflictError

    while True:
        try:
            return await AsyncTemplate.create(**kwargs)
        except ConflictError as error:
            # Another trial of this task created the template first; use it. Its build is waited on
            # by the caller, so return the existing handle.
            if "already exists" not in str(error):
                raise
            existing = [t for t in await AsyncTemplate.list() if t.name == kwargs["name"]]
            if existing:
                return await AsyncTemplate.connect(existing[0].id)
            await asyncio.sleep(5)
        except RateLimitError as error:
            if "templates" not in str(error):
                raise  # concurrent-build limit: the caller's slots already queue these
            if not await _evict_oldest_idle_template():
                await asyncio.sleep(30)  # every cached template is in use; wait for a trial to finish


async def _evict_oldest_idle_template() -> bool:
    from superserve import AsyncTemplate

    idle = sorted(
        (t for t in await AsyncTemplate.list()
         if t.name.startswith("reason-") and t.name not in TEMPLATES_IN_USE),
        key=lambda t: t.created_at or "")
    if not idle:
        return False
    await AsyncTemplate.delete_by_id(idle[0].id)
    return True


def _dockerfile_lines(path: Path) -> list[tuple[str, str]]:
    """(INSTRUCTION, args) for each Dockerfile instruction, with continuation lines joined
    and comments and blank lines dropped."""
    out: list[tuple[str, str]] = []
    pending = ""
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not pending and (not line or line.startswith("#")):
            continue
        if line.endswith("\\"):
            pending += line[:-1] + " "
            continue
        full = (pending + line).strip()
        pending = ""
        if not full:
            continue
        instruction, _, args = full.partition(" ")
        out.append((instruction, args.strip()))
    return out


def _parse_env(args: str) -> dict[str, str]:
    """ENV K=V [K2=V2 ...] or the legacy ENV K V form."""
    if "=" not in args.split(" ", 1)[0]:
        key, _, value = args.partition(" ")
        return {key: value.strip()}
    result = {}
    for token in shlex.split(args):
        key, _, value = token.partition("=")
        result[key] = value
    return result


def _copy_sources(path: Path):
    if path.is_dir():
        return sorted(p for p in path.rglob("*") if p.is_file())
    return [path]


def _quote(path: str) -> str:
    return "'" + path.replace("'", "'\\''") + "'"
