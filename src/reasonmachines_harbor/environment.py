"""Harbor environments for Reason benchmark runs.

ReasonDockerEnvironment builds platform-aligned egress sidecars.
ReasonE2BEnvironment applies the task image's ENV when the sandbox is created,
which E2B otherwise drops, so the agent, its shells and the verifier all see the
image's PATH, virtualenv and toolchain, as they would on Docker.
"""
import asyncio
import os

from harbor.environments.docker.docker import DockerEnvironment
from harbor.environments.docker.utils import default_docker_platform, ensure_docker_image_built
from harbor.environments.e2b import E2BEnvironment

from .image_runtime import ImageConfigUnavailableError, image_runtime


class ReasonDockerEnvironment(DockerEnvironment):
    """Build Harbor's egress sidecar for the task container platform."""

    async def _ensure_egress_control_sidecar_image_built(self) -> None:
        platform = os.environ.get("DOCKER_DEFAULT_PLATFORM") or await default_docker_platform()
        self._env_vars.egress_control_sidecar_image_name = (
            await ensure_docker_image_built(
                docker_name=self._EGRESS_CONTROL_SIDECAR_DOCKER_NAME,
                docker_build_context=self._EGRESS_CONTROL_SIDECAR_CONTEXT_PATH,
                dockerfile_path=self._egress_control_sidecar_dockerfile_path(),
                build_args={},
                platform=platform,
                logger=self.logger,
            )
        )


# E2B runs every command in a login shell, and a Debian-style /etc/profile resets PATH
# before it sources /etc/profile.d, so the image's PATH entries are put back there.
PROFILE_SCRIPT = "/etc/profile.d/00-reason-image-path.sh"


def image_path_profile(path: str) -> str:
    """A profile.d script that prepends the PATH entries a login shell's /etc/profile dropped."""
    entries = [entry for entry in path.split(":") if entry]
    quoted = " ".join("'" + entry.replace("'", "'\\''") + "'" for entry in reversed(entries))
    return ("# Written by reason-machines-harbor: the task image's PATH, which /etc/profile resets.\n"
            f'for _reason_entry in {quoted}; do\n'
            '  case ":$PATH:" in *":$_reason_entry:"*) ;; *) PATH="$_reason_entry:$PATH" ;; esac\n'
            "done\nunset _reason_entry\nexport PATH\n")


class ReasonE2BEnvironment(E2BEnvironment):
    """E2B with the task image's ENV below task and trial values; WORKDIR stays Harbor's."""

    async def start(self, force_build: bool):
        # Read the config off the event loop: a rate-limited registry retries for minutes.
        try:
            self._image_env, _ = await asyncio.to_thread(image_runtime, self)
        except Exception as error:
            raise ImageConfigUnavailableError(
                f"task image config unavailable ({type(error).__name__}: {error})") from error
        await super().start(force_build)
        path = self._startup_env().get("PATH") if "PATH" in self._image_env else None
        if path:
            await self._sandbox.files.write(PROFILE_SCRIPT, image_path_profile(path), user="root")

    def _startup_env(self) -> dict[str, str]:
        return {**getattr(self, "_image_env", {}), **super()._startup_env()}
